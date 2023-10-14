package db

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/jwbargsten/go-mssql-load/util"
	"io"

	//"fmt"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/batch"
	"go.uber.org/zap"
	"net/url"
	"strings"
)

func strip(v string) string {
	scanner := bufio.NewScanner(strings.NewReader(v))
	scanner.Split(bufio.ScanLines)

	var stmt string
	for scanner.Scan() {
		v := scanner.Text()
		vp := strings.TrimSpace(v)
		if len(vp) == 0 {
			continue
		}

		stmt += v + "\n"
	}

	return stmt
}

func QuerySql(log *zap.SugaredLogger, f string, dsn *url.URL) error {
	fp, err := util.OpenFileorStdin(f, log)
	if err != nil {
		return fmt.Errorf("Unable to read input file: %w", err)
	}
	defer fp.Close()
	raw, err := io.ReadAll(fp)
	if err != nil {
		//log.Fatalw("could not open file", "file", f, zap.Error(err))
		return err
	}

	db, err := Open(dsn)
	if err != nil {
		//log.Fatalw("could not connect to db", "host", dsn.Host, zap.Error(err))
		return err
	}
	defer db.Close()

	res := batch.Split(string(raw), "GO")
	for i, v1 := range res {
		if i > 0 {
			fmt.Println("---")
		}
		v2 := strip(v1)
		if len(v2) == 0 {
			continue
		}
		rows, err := db.Queryx(v2)
		if err != nil {
			return err
		}
		for rows.Next() {
			results := make(map[string]interface{})
			err = rows.MapScan(results)
			if err != nil {
				rows.Close()
				return err
			}
			jsonContent, err := json.Marshal(results)
			if err != nil {
				rows.Close()
				return err
			}

			fmt.Println(string(jsonContent))
		}
	}
	return nil
}

func LoadSql(log *zap.SugaredLogger, f string, dsn *url.URL) error {
	fp, err := util.OpenFileorStdin(f, log)
	if err != nil {
		return fmt.Errorf("Unable to read input file: %w", err)
	}
	defer fp.Close()
	raw, err := io.ReadAll(fp)
	if err != nil {
		//log.Fatalw("could not open file", "file", f, zap.Error(err))
		return err
	}

	db, err := Open(dsn)
	if err != nil {
		//log.Fatalw("could not connect to db", "host", dsn.Host, zap.Error(err))
		return err
	}
	defer db.Close()

	tx, err := db.Beginx()
	if err != nil {
		//panic(err)
		return err
	}
	res := batch.Split(string(raw), "GO")
	for _, v1 := range res {
		v2 := strip(v1)
		if len(v2) == 0 {
			continue
		}
		_, err := db.Exec(v2)
		if err != nil {
			tx.Rollback()
			//log.Fatalw("db query error", zap.Error(err))
			return err
		}
	}
	tx.Commit()
	return nil
}
