package config

import (
	"os"
)

type Config struct {
	Name                       string
	User string
	Pass string
	Host string
	Port                       string
	WithEncryption      string
	WithTrustServerCert string
}

func New() Config {
	return Config{
		Name:                getEnv("MSSQL_NAME", "master"),
		User:                getEnv("MSSQL_USER", "sa"),
		Pass:                getEnv("MSSQL_PASS", ""),
		Host:                getEnv("MSSQL_HOST", "localhost"),
		Port:                getEnv("MSSQL_PORT", "1433"),
		WithEncryption:      getEnv("MSSQL_ENCRYPT", "false"),
		WithTrustServerCert: getEnv("MSSQL_TRUST_SERVER_CERT", "true"),
	}
}

func getEnv(key string, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		value = fallback
		//log.Warnw("value not found in env, using fallback", "key", key, "fallback", fallback)
	}
	return value
}
