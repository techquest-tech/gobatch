package gobatch

type Logger interface {
	Debug(msg string, keyAndValue ...interface{})
	// Info logs routine messages about cron's operation.
	Info(msg string, keysAndValues ...interface{})
	// Error logs an error condition.
	Error(err error, msg string, keysAndValues ...interface{})
}

type LoggerImpl struct {
}

func (l *LoggerImpl) Debug(msg string, keyAndValue ...interface{}) {

}
