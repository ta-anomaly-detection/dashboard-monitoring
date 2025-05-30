package config

import (
	"os"

	"github.com/natefinch/lumberjack"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type AppLoggers struct {
    Middleware *zap.Logger
    App        *zap.Logger
}

func NewLogger(viper *viper.Viper) *AppLoggers {
	logFilePath := viper.GetString("log.filePath")
	if logFilePath == "" {
		logFilePath = "/var/app/logs/web-server.log"
	}

	lumberjackLogger := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    viper.GetInt("log.maxSize"), // megabytes
		MaxBackups: viper.GetInt("log.maxBackups"),
		MaxAge:     viper.GetInt("log.maxAge"), // days
		Compress:   viper.GetBool("log.compress"),
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	if viper.GetString("env") == "development" {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		encoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	}

	fileWriter := zapcore.AddSync(lumberjackLogger)
	consoleWriter := zapcore.AddSync(os.Stdout)

	var logLevel zapcore.Level
	switch viper.GetString("log.level") {
	case "debug":
		logLevel = zapcore.DebugLevel
	case "info":
		logLevel = zapcore.InfoLevel
	case "warn":
		logLevel = zapcore.WarnLevel
	case "error":
		logLevel = zapcore.ErrorLevel
	default:
		logLevel = zapcore.InfoLevel
	}

	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		consoleWriter,
		logLevel,
	)
	
	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		fileWriter,
		logLevel,
	)

	core := zapcore.NewTee(consoleCore, fileCore)

	middlewareLogger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	appLogger := zap.New(consoleCore, 
		zap.AddCaller(), 
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return &AppLoggers{
		Middleware: middlewareLogger,
		App:        appLogger,
	}
}
