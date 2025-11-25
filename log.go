package godatabend

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
)

type contextKey string

// DBSessionIDKey is context key of session id
const DBSessionIDKey contextKey = "LOG_SESSION_ID"

// SFSessionUserKey is context key of  user id of a session
const SFSessionUserKey contextKey = "LOG_USER"

// LogKeys these keys in context should be included in logging messages when using logger.WithContext
var LogKeys = [...]contextKey{DBSessionIDKey, SFSessionUserKey}

// ContextLogger represents a logger that already captured desired context.
type ContextLogger interface {
	Infoln(args ...interface{})
}

// DBLogger Databend logger interface backed by slog.
type DBLogger interface {
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Info(args ...interface{})
	SetLogLevel(level string) error
	SetOutput(output io.Writer)
	WithContext(ctx context.Context) ContextLogger
}

// DBCallerPrettyfier to provide base file name and function name from calling frame used in SFLogger
func DBCallerPrettyfier(frame *runtime.Frame) (string, string) {
	return path.Base(frame.Function), fmt.Sprintf("%s:%d", path.Base(frame.File), frame.Line)
}

type defaultLogger struct {
	mu        sync.RWMutex
	levelVar  *slog.LevelVar
	handlerFn func(io.Writer) slog.Handler
	inner     *slog.Logger
	output    io.Writer
}

// CreateDefaultLogger return a new instance of logger with default config
func CreateDefaultLogger() DBLogger {
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)

	replaceAttr := func(groups []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.SourceKey {
			if src, ok := attr.Value.Any().(*slog.Source); ok && src != nil {
				frame := &runtime.Frame{
					Function: src.Function,
					File:     src.File,
					Line:     src.Line,
				}
				function, location := DBCallerPrettyfier(frame)
				attr.Value = slog.StringValue(strings.TrimSpace(function + " " + location))
			}
		}
		return attr
	}

	handlerFn := func(w io.Writer) slog.Handler {
		if w == nil {
			w = os.Stdout
		}
		return slog.NewTextHandler(w, &slog.HandlerOptions{
			AddSource:   true,
			Level:       levelVar,
			ReplaceAttr: replaceAttr,
		})
	}

	dLogger := &defaultLogger{
		levelVar:  levelVar,
		handlerFn: handlerFn,
		output:    os.Stdout,
	}
	dLogger.inner = slog.New(handlerFn(dLogger.output))
	return dLogger
}

func (log *defaultLogger) getLogger() *slog.Logger {
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.inner
}

// SetLogLevel set logging level for calling defaultLogger
func (log *defaultLogger) SetLogLevel(level string) error {
	lvl, err := parseLevel(level)
	if err != nil {
		return err
	}
	log.levelVar.Set(lvl)
	return nil
}

func parseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(level) {
	case "trace":
		fallthrough
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "dpanic", "panic", "fatal":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown log level: %s", level)
	}
}

// WithContext return a ContextLogger to include fields in context
func (log *defaultLogger) WithContext(ctx context.Context) ContextLogger {
	base := log.getLogger()
	attrs := context2Attrs(ctx)
	if len(attrs) > 0 {
		args := make([]interface{}, len(attrs))
		for i := range attrs {
			args[i] = attrs[i]
		}
		base = base.With(args...)
	}
	return &contextLogger{inner: base}
}

func (log *defaultLogger) Info(args ...interface{}) {
	log.getLogger().Info(fmt.Sprint(args...))
}

func (log *defaultLogger) Error(args ...interface{}) {
	log.getLogger().Error(fmt.Sprint(args...))
}

func (log *defaultLogger) Debugf(format string, args ...interface{}) {
	log.getLogger().Debug(fmt.Sprintf(format, args...))
}

func (log *defaultLogger) SetOutput(output io.Writer) {
	if output == nil {
		return
	}
	log.mu.Lock()
	log.output = output
	log.inner = slog.New(log.handlerFn(output))
	log.mu.Unlock()
}

// SetLogger set a new logger of defaultLogger interface for godatabend
func SetLogger(inLogger DBLogger) {
	if inLogger == nil {
		return
	}
	logger = inLogger
}

// GetLogger return logger that is not public
func GetLogger() DBLogger {
	return logger
}

func context2Attrs(ctx context.Context) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(LogKeys))
	if ctx == nil {
		return attrs
	}

	for i := 0; i < len(LogKeys); i++ {
		if ctx.Value(LogKeys[i]) != nil {
			attrs = append(attrs, slog.Any(string(LogKeys[i]), ctx.Value(LogKeys[i])))
		}
	}
	return attrs
}

type contextLogger struct {
	inner *slog.Logger
}

func (c *contextLogger) Infoln(args ...interface{}) {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.Info(formatLine(args...))
}

func formatLine(args ...interface{}) string {
	if len(args) == 0 {
		return ""
	}
	return strings.TrimSuffix(fmt.Sprintln(args...), "\n")
}
