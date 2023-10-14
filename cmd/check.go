package cmd

import (
	"context"
	"github.com/jwbargsten/go-mssql-load/db"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	rootCmd.AddCommand(checkCmd)
}

var checkCmd = &cobra.Command{
	Use:   "check [flags] <path>",
	Short: "Test db connection",
	Long:  `Test db connection.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn, err := buildDSN(cmd.Flags())
		if err != nil {
			log.Errorw("could not build DSN", zap.Error(err))
			return err
		}


		log.Infof("checking connection to %s/%s", dsn.Host, dsn.Query().Get("database"))
		ctx := context.Background()
		con, err := db.Open(dsn)
		if err != nil {
			log.Errorw("could not connect to db", zap.Error(err))
			return err
		}
		err = db.StatusCheck(ctx, con, log)
		if err != nil {
			log.Errorw("could not connect to db", zap.Error(err))
			return err
		}
		log.Infof("connection successful!")
		return nil
	},
}
