package cmd

import (
	"fmt"
	"github.com/jwbargsten/go-mssql-load/config"
	"github.com/jwbargsten/go-mssql-load/util"
	"github.com/spf13/pflag"
	"net/url"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "go-mssql-load",
	Short: "Utility functions for loading data into mssql server",
	Long:  `Utility functions for loading data into mssql server

The connection parameters can be supplied via CLI flags
and via environment variables. If a flag has a corresponding
env var, it is mentioned in curly brackets, e.g. "{MSSQL_NAME}"
in the Flags section. CLI flags take precedence over the env vars.

It is also possible to specify the complete DSN via {MSSQL_DSN}:

MSSQL_DSN=sqlserver://sa:Passw0rd@localhost:1433?database=master \
	go-mssql-load check

This allows you to supply parameters that are not supported directly
by go-mssql-load.

NOTE:
  If the DSN is specified (as param or env var), it takes precedence
	over all other flags/vars.

All supported parameters can be found on the official go-mssqldb lib:
https://github.com/microsoft/go-mssqldb#connection-parameters-and-dsn

File arguments can also take "-" as file name for reading the file
contents from STDIN.
`,
}

func init() {
	rootCmd.PersistentFlags().StringP("name", "n", "master", "Specifies the db name. {MSSQL_NAME}")
	rootCmd.PersistentFlags().StringP("host", "H", "localhost", `Specifies the host name of the machine on
which the server is running. {MSSQL_HOST}`)
	rootCmd.PersistentFlags().IntP("port", "p", 1433, `Specifies the TCP port on which the server is
listening for connections. {MSSQL_PORT}`)
	rootCmd.PersistentFlags().StringP("user", "U", "", "Specifies the user name. {MSSQL_USER}")
	rootCmd.PersistentFlags().StringP("pass", "P", "", "Specifies the password. {MSSQL_PASS}")
	rootCmd.PersistentFlags().Bool("encrypt", false, "Corresponds to the encrypt parameter of go-mssqldb. {MSSQL_ENCRYPT}")
	rootCmd.PersistentFlags().Bool("trust-server-cert", true, `Corresponds to (and behaves like!) the
TrustServerCertificate parameter of go-mssqldb.
The default is determined by encrypt: if encrypt=true, trust=false;
if encrypt=false, trust=true. {MSSQL_TRUST_SERVER_CERT}`)
	rootCmd.PersistentFlags().String("dsn",  "", `Specifies the full dsn. If specified, takes
precedence over all other parameters. {MSSQL_DSN}`)
	rootCmd.SilenceUsage = true
}

func buildConnString(cfg config.Config) *url.URL {
	q := make(url.Values)
	q.Add("encrypt", cfg.WithEncryption)
	q.Add("database", cfg.Name)
	q.Add("TrustServerCertificate", cfg.WithTrustServerCert)
	//query.Add("log", "63") --> to enable verbose logging
	dbUrl := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cfg.User, cfg.Pass),
		Host:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		RawQuery: q.Encode(),
	}
	return &dbUrl
}

var log = util.NewLogger()

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func buildDSN(flags *pflag.FlagSet) (*url.URL, error) {
	cfg := config.New()
	if flags.Changed("name") {
		v, err := flags.GetString("name")
		if err != nil {
			return nil, fmt.Errorf("could not parse name flag: %w", err)
		}
		cfg.Name = v
	}
	if flags.Changed("host") {
		v, err := flags.GetString("host")
		if err != nil {
			return nil, fmt.Errorf("could not parse host flag: %w", err)
		}
		cfg.Host = v
	}
	if flags.Changed("port") {
		v, _ := flags.GetInt("port")
		cfg.Port = strconv.Itoa(v)
	}
	if flags.Changed("user") {
		v, _ := flags.GetString("user")
		cfg.User = v
	}
	if flags.Changed("pass") {
		v, _ := flags.GetString("pass")
		cfg.Pass = v
	}
	if flags.Changed("encrypt") {
		v, _ := flags.GetBool("encrypt")
		if v {
			cfg.WithEncryption = "true"
			cfg.WithTrustServerCert = "false"
		} else {
			cfg.WithEncryption = "false"
			cfg.WithTrustServerCert = "true"
		}
	}
	if flags.Changed("trust-server-cert") {
		v, _ := flags.GetBool("trust-server-cert")
		if v {
			cfg.WithTrustServerCert = "true"
		} else {
			cfg.WithTrustServerCert = "false"
		}
	}

	var dsn *url.URL
	if flags.Changed("dsn") {
		var err error
		v, _ := flags.GetString("dsn")
		dsn, err = url.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("could not parse dsn flag: %w", err)
		}
	} else {
		v, ok := os.LookupEnv("MSSQL_DSN")
		if ok {
			var err error
			dsn, err = url.Parse(v)
			if err != nil {
				return nil, fmt.Errorf("could not parse dsn env var: %w", err)
			}
		}
	}
	if dsn == nil {
		dsn = buildConnString(cfg)
	}

	return dsn, nil
}
