package util

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

func OpenFileorStdin(f string, log *zap.SugaredLogger) (*os.File, error) {
	if f == "-" {
		return os.Stdin, nil
	} else {
		return os.Open(f)
	}

}

func NewLogger() *zap.SugaredLogger {
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"stderr"}
	config.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	config.DisableStacktrace = true
	log, err := config.Build()
	if err != nil {
		panic(err)
	}
	return log.Sugar()
}

