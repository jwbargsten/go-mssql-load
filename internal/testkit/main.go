package testkit

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"net/url"
	"testing"
)

func OpenTestDB(t testing.TB, name string) (*sqlx.DB) {
	q := make(url.Values)
	q.Add("database", name)
	dsn := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword("sa", "Passw0rd"),
		Host:     fmt.Sprintf("%s:%s", "localhost", "1433"),
		RawQuery: q.Encode(),
	}

	db, err := sqlx.Open("sqlserver", dsn.String())
	if err != nil {
		t.Error("could not open db connection", "dsn", dsn.String(), err)
	}

	return db
}
