package cmd

import (
	"github.com/jwbargsten/go-mssql-load/db"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	rootCmd.AddCommand(loadsqlCmd)
}

var loadsqlCmd = &cobra.Command{
	Use:   "loadsql <path>",
	Short: "Load a sql file into the db",
	Long: `Load a sql file into the db

You can supply a sql file as arg. All statements in this file will be parsed
and executed separately. You can separate statements with a line containing
only the keyword "GO".`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn,err := buildDSN(cmd.Flags())
		if err != nil {
			log.Errorw("could not build DSN", zap.Error(err))
			return err
		}

		f := args[0]
		log.Infof("loading sql file %s", f)
		err = db.LoadSql(log, f, dsn)
		if err != nil {
			return err
		}
		log.Infof("loaded file successfully!")
		return nil
	},
}
