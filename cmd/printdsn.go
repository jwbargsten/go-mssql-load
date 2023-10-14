package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	printDsnCmd.PersistentFlags().Bool("redacted", false, "Hide the password")
	rootCmd.AddCommand(printDsnCmd)
}

var printDsnCmd = &cobra.Command{
	Use:   "printdsn",
	Short: "Print the DSN",
	Long:  `Print the DSN.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn, err := buildDSN(cmd.Flags())
		if err != nil {
			log.Errorw("could not build DSN", zap.Error(err))
			return err
		}
		isRedacted, err := cmd.Flags().GetBool("redacted")
		if err != nil {
			return fmt.Errorf("could not parse redacted flag: %w", err)
		}
		if isRedacted {
			fmt.Printf("%+v\n", dsn.Redacted())
		} else {
			fmt.Printf("%+v\n", dsn.String())
		}

		return nil
	},
}
