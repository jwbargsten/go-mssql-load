package db

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"go.uber.org/zap"
	"net/url"
	"time"
)

func Open(dsn *url.URL) (*sqlx.DB, error) {

	db, err := sqlx.Open("sqlserver", dsn.String())
	return db, err
}

// StatusCheck returns nil if it can successfully talk to the database. It
// returns a non-nil error otherwise.
func StatusCheck(ctx context.Context, db *sqlx.DB, log *zap.SugaredLogger) error {

	// First check we can ping the database.
	var pingError error
	for attempts := 1; ; attempts++ {
		pingError = db.Ping()
		if pingError == nil {
			break
		}
		log.Warnw("could not ping db", zap.Error(pingError))
		time.Sleep(time.Duration(attempts) * 100 * time.Millisecond)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempts >= 6 {
			return fmt.Errorf("could not make connection (tried %d times)", attempts)
		}
	}

	// Make sure we didn't timeout or be cancelled.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Run a simple query to determine connectivity. Running this query forces a
	// round trip through the database.
	const q = `SELECT COUNT(*) FROM sys.databases`
	var tmp string
	err := db.QueryRowContext(ctx, q).Scan(&tmp)
	return err
}
