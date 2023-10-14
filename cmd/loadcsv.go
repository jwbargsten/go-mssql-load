package cmd

import (
	"encoding/csv"
	"fmt"
	"github.com/jwbargsten/go-mssql-load/db"
	"github.com/jwbargsten/go-mssql-load/util"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"io"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

func init() {
	rootCmd.AddCommand(loadcsvCmd)
	loadcsvCmd.Flags().String("nullstr", "", "if a column is nullable and its value is equal to this string, null is inferred")
	loadcsvCmd.Flags().String("sep", ",", "separator")
}

var loadcsvCmd = &cobra.Command{
	Use:   "loadcsv <table> <path>",
	Short: "Load a csv file into the db",
	Long:  `Load a csv file into the db`,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn,err := buildDSN(cmd.Flags())
		if err != nil {
			log.Errorw("could not build DSN", zap.Error(err))
		}
		nullstr, err := cmd.Flags().GetString("nullstr")
		if err != nil {
			log.Errorw("could not parse nullstr flag", zap.Error(err))
		}
		log.Infof("null string is »%s«", nullstr)
		sep, err := cmd.Flags().GetString("sep")
		sepRune, _ := utf8.DecodeRuneInString(sep)
		if err != nil {
			log.Errorw("could not parse sep flag", zap.Error(err))
		}
		log.Infof("sep is »%s«", sep)

		tblname := args[0]
		f := args[1]
		log.Infof("loading csv file %s", f)
		nrows, err := loadcsv(tblname, f, dsn, nullstr, sepRune)
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

func parseHeader(header []string) Header {
	ncols := len(header)

	types := make([]string, ncols)
	parsers := make([]func(string) (any, error), ncols)
	colnames := make([]string, ncols)
	colopt := make([]bool, ncols)
	for colidx, col := range header {
		res := strings.SplitN(col, "::", 2)
		if len(res) != 2 {
			// no type info in column header, just assume string
			colnames[colidx] = col
			parsers[colidx] = func(v string) (any, error) { return v, nil }
			types[colidx] = "string"
			continue
		}
		colname, coltype := res[0], res[1]
		colopt[colidx] = false
		if len(coltype) > 0 && coltype[len(coltype)-1:] == "!" {
			colopt[colidx] = true
			coltype = coltype[:len(coltype)-1]
		}

		colnames[colidx] = colname
		switch coltype {
		case "int":
			parsers[colidx] = func(v string) (any, error) { return strconv.ParseInt(v, 10, 64) }
			types[colidx] = "int"
		case "float":
			parsers[colidx] = func(v string) (any, error) { return strconv.ParseFloat(v, 64) }
			types[colidx] = "float"
		case "bool":
			parsers[colidx] = func(v string) (any, error) {
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
			types[colidx] = "bool"
		default:
			parsers[colidx] = func(v string) (any, error) { return v, nil }
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
func loadcsv(tblname string, f string, dsn *url.URL, nullstr string, sep rune) (int64, error) {
	fp, err := util.OpenFileorStdin(f, log)
	if err != nil {
		return 0, fmt.Errorf("Unable to read input file: %w", err)
	}
	defer fp.Close()

	csvReader := csv.NewReader(fp)
	csvReader.Comma = sep

	rawHeader, err := csvReader.Read()
	header := parseHeader(rawHeader)
	if err != nil {
		return 0,fmt.Errorf("could not read csv: %w", err)
	}
	log.Info("columns")
	for idx, name := range header.Colnames {
		n := ""
		if header.Colopt[idx] {
			n = "NULL"
		}
		log.Infof("[%3d] %-40s%-15s%s", idx+1, name, header.Types[idx], n)

	}

	con, err := db.Open(dsn)
	if err != nil {
		return 0,fmt.Errorf("could not connect to db: %w", err)
	}

	txn := con.MustBegin()
	stmt, err := txn.Prepare(mssql.CopyIn(tblname, mssql.BulkOptions{}, header.Colnames...))
	if err != nil {
		return 0,err
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0,fmt.Errorf("error reading csv: %w", err)
		}
		row := parseRow(header, record, nullstr)

		_, err = stmt.Exec(row...)

		if err != nil {
			return 0,fmt.Errorf("could not exec sql: %w", err)
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

	return rowCount,nil
}
