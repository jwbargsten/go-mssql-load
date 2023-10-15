package cmd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jwbargsten/go-mssql-load/db"
	"github.com/jwbargsten/go-mssql-load/util"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

func init() {
	rootCmd.AddCommand(loadcsvCmd)
	loadcsvCmd.Flags().String("nullstr", "", "if a column is nullable and its value is equal to this string, null is inferred")
	loadcsvCmd.Flags().String("sep", ",", "separator")
	loadcsvCmd.Flags().String("types", "", "file with types, takes precedence over CSV header types")
}

type ColTypes struct {
	byName map[string]string
	byPos  []string
}

func LoadColTypes(f string) (ColTypes, error) {
	typesB, err := os.ReadFile(f)
	if err != nil {
		return ColTypes{}, err
	}
	// https://stackoverflow.com/questions/55014001/check-if-json-is-object-or-array
	typesB = bytes.TrimLeft(typesB, " \t\r\n")

	var colTypes ColTypes
	isArray := len(typesB) > 0 && typesB[0] == '['
	isObject := len(typesB) > 0 && typesB[0] == '{'

	if isArray {
		if err := json.Unmarshal(typesB, &colTypes.byPos); err != nil {
			return ColTypes{}, err
		}
	} else if isObject {
		if err := json.Unmarshal(typesB, &colTypes.byName); err != nil {
			return ColTypes{}, err
		}
	} else {
		return ColTypes{}, errors.New("JSON type file doesn't seem to have an array or dict structure")
	}
	return colTypes, err
}

func (ct ColTypes) FindByName(name string) (string, bool) {
	if ct.byName == nil {
		return "", false
	}
	v, ok := ct.byName[name]
	return v, ok
}
func (ct ColTypes) FindByIdx(idx int) (string, bool) {
	if ct.byPos == nil {
		return "", false
	}
	if idx >= len(ct.byPos) || idx < 0 {
		return "", false
	}

	v := ct.byPos[idx]
	return v, true
}

func (ct ColTypes) Find(idx int, name string) (string, bool) {
	if t, found := ct.FindByIdx(idx); found {
		return t, true
	}
	if t, found := ct.FindByName(name); found {
		return t, true
	}
	return "", false
}

var loadcsvCmd = &cobra.Command{
	Use:   "loadcsv <table> <path>",
	Short: "Load a csv file into the db",
	Long:  `Load a csv file into the db`,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		dsn, err := buildDSN(flags)
		if err != nil {
			log.Errorw("could not build DSN", zap.Error(err))
		}
		nullstr, err := flags.GetString("nullstr")
		if err != nil {
			log.Errorw("could not parse nullstr flag", zap.Error(err))
		}
		log.Infof("null string is »%s«", nullstr)
		sep, err := flags.GetString("sep")
		if err != nil {
			log.Errorw("could not parse sep flag", zap.Error(err))
		}
		sepRune, _ := utf8.DecodeRuneInString(sep)
		log.Infof("sep is »%s«", sep)

		var colTypes ColTypes
		if flags.Changed("types") {
			v, err := flags.GetString("types")
			if err != nil {
				log.Errorw("could not parse types flag: %w", err)
			}
			colTypes, err = LoadColTypes(v)
			if err != nil {
				log.Errorw("could not parse types file: %w", err)
			}
		}

		tblname := args[0]
		f := args[1]
		log.Infof("loading csv file %s", f)
		nrows, err := loadcsv(tblname, f, dsn, nullstr, sepRune, colTypes)
		if err != nil {
			return err
		}
		log.Infof("inserted %d rows", nrows)
		log.Infof("loaded file successfully!")
		return nil
	},
}

type Header struct {
	Colnames []string
	Colopt   []bool
	Ncols    int
	Types    []string
	Parsers  []func(string) (any, error)
}

func parseInt(v string) (any, error)   { return strconv.ParseInt(v, 10, 64) }
func parseFloat(v string) (any, error) { return strconv.ParseFloat(v, 64) }

func parseBool(v string) (any, error) {
	// this should account for TRUE, true, T, t
	if strings.HasPrefix(strings.ToLower(v), "t") {
		return true, nil
	}
	// this should account for YES, yes, Y, y
	if strings.HasPrefix(strings.ToLower(v), "y") {
		return true, nil
	}
	// so, perhaps we have a number (0/1)
	vParsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return false, err
	}
	return vParsed > 0, nil
}
func parseString(v string) (any, error) { return v, nil }
func parseHeader(header []string, colTypes ColTypes) Header {
	ncols := len(header)

	types := make([]string, ncols)
	parsers := make([]func(string) (any, error), ncols)
	colnames := make([]string, ncols)
	colopt := make([]bool, ncols)
	for colidx, col := range header {
		res := strings.SplitN(col, "::", 2)
		var colname, coltype string
		if len(res) == 2 {
			colname, coltype = res[0], res[1]
		} else {
			// no type info in column header, just assume string
			colname = col
			coltype = ""
		}

		if ct, found := colTypes.Find(colidx, colname); found {
			coltype = ct
		}

		colopt[colidx] = false
		if len(coltype) > 0 && coltype[len(coltype)-1:] == "!" {
			colopt[colidx] = true
			coltype = coltype[:len(coltype)-1]
		}

		colnames[colidx] = colname
		switch coltype {
		case "int":
			parsers[colidx] = parseInt
			types[colidx] = "int"
		case "float":
			parsers[colidx] = parseFloat
			types[colidx] = "float"
		case "bool":
			parsers[colidx] = parseBool
			types[colidx] = "bool"
		case "string":
			parsers[colidx] = parseString
			types[colidx] = "string"
		default:
			log.Infow("No column type specified, using string.",
				"name", colname,
				"idx", colidx,
				"type", coltype,
			)
			parsers[colidx] = parseString
			types[colidx] = "string"
		}
	}

	return Header{
		Colopt:   colopt,
		Colnames: colnames,
		Parsers:  parsers,
		Ncols:    ncols,
		Types:    types,
	}
}

func parseRow(header Header, row []string, nullstr string) []any {
	if len(row) != header.Ncols {
		log.Fatalw("could not read csv")
	}

	parsedRow := make([]any, header.Ncols)

	for idx, v := range row {
		if header.Colopt[idx] && v == nullstr {
			parsedRow[idx] = nil
			continue
		}
		res, err := header.Parsers[idx](v)
		if err != nil {
			log.Fatalw("could not parse", zap.Error(err))
		}
		parsedRow[idx] = res

	}
	return parsedRow
}
func loadcsv(tblname string, f string, dsn *url.URL, nullstr string, sep rune, colTypes ColTypes) (int64, error) {
	fp, err := util.OpenFileorStdin(f, log)
	if err != nil {
		return 0, fmt.Errorf("unable to read input file: %w", err)
	}
	defer fp.Close()

	csvReader := csv.NewReader(fp)
	csvReader.Comma = sep

	rawHeader, err := csvReader.Read()
	header := parseHeader(rawHeader, colTypes)
	if err != nil {
		return 0, fmt.Errorf("could not read csv: %w", err)
	}
	log.Info("columns")
	log.Infof("%s   %-40s%-15s%s", "IDX", "NAME", "TYPE", "NULLABLE")
	for idx, name := range header.Colnames {
		n := ""
		if header.Colopt[idx] {
			n = "NULL"
		}
		log.Infof("[%3d] %-40s%-15s%s", idx, name, header.Types[idx], n)
	}

	con, err := db.Open(dsn)
	if err != nil {
		return 0, fmt.Errorf("could not connect to db: %w", err)
	}

	txn := con.MustBegin()
	stmt, err := txn.Prepare(mssql.CopyIn(tblname, mssql.BulkOptions{}, header.Colnames...))
	if err != nil {
		return 0, err
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("error reading csv: %w", err)
		}
		row := parseRow(header, record, nullstr)

		_, err = stmt.Exec(row...)

		if err != nil {
			return 0, fmt.Errorf("could not exec sql: %w", err)
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	rowCount, _ := result.RowsAffected()

	return rowCount, nil
}
