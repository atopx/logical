package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func Setup(level string) (err error) {
	var loggerLevel = new(zapcore.Level)
	if err = loggerLevel.UnmarshalText([]byte(level)); err != nil {
		return err
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
	}), zapcore.AddSync(os.Stdout), loggerLevel)
	logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	return err
}

func Debug(message string, fields ...zapcore.Field) {
	logger.Debug(message, fields...)
}

func Info(message string, fields ...zapcore.Field) {
	logger.Info(message, fields...)
}

func Warn(message string, fields ...zapcore.Field) {
	logger.Warn(message, fields...)
}

func Error(message string, fields ...zapcore.Field) {
	logger.Error(message, fields...)
}

func Fatal(message string, fields ...zapcore.Field) {
	logger.Fatal(message, fields...)
}

func Panic(message string, fields ...zapcore.Field) {
	logger.Panic(message, fields...)
}
