// Package cliobs wires command-line logging, tracing, and metrics helpers.
package cliobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/stokaro/ptah/migration/migrator"
)

const (
	LogFormatFlagName   = "log-format"
	LogLevelFlagName    = "log-level"
	MetricsAddrFlagName = "metrics-addr"
)

const (
	logFormatText = "text"
	logFormatJSON = "json"
)

var globalStateMu sync.Mutex

// Options configures command observability.
type Options struct {
	Command     string
	LogFormat   string
	LogLevel    string
	MetricsAddr string
	LogWriter   io.Writer
}

// Runtime owns command-scoped observability resources.
type Runtime struct {
	logger   *slog.Logger
	observer migrator.Observer
	human    bool
	shutdown func(context.Context) error
	once     sync.Once
}

// Start initializes command logging, tracing, and metrics.
func Start(ctx context.Context, opts Options) (*Runtime, error) {
	logLevel, err := parseLogLevel(opts.LogLevel)
	if err != nil {
		return nil, err
	}
	logWriter := opts.LogWriter
	if logWriter == nil {
		logWriter = os.Stderr
	}
	logFormat := strings.TrimSpace(opts.LogFormat)
	if logFormat == "" {
		logFormat = logFormatText
	}

	var logger *slog.Logger
	human := true
	switch logFormat {
	case logFormatText:
		logger = slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: logLevel}))
	case logFormatJSON:
		logger = slog.New(slog.NewJSONHandler(logWriter, &slog.HandlerOptions{Level: logLevel}))
		human = false
	default:
		return nil, fmt.Errorf("invalid --%s value %q: expected text or json", LogFormatFlagName, opts.LogFormat)
	}
	logger = logger.With("correlation_id", newCorrelationID(), "command", opts.Command)

	globalStateMu.Lock()
	previousLogger := slog.Default()
	slog.SetDefault(logger)

	observer, shutdown, err := startOTel(ctx, opts)
	if err != nil {
		slog.SetDefault(previousLogger)
		globalStateMu.Unlock()
		return nil, err
	}
	shutdown = joinShutdown(shutdown, func(context.Context) error {
		slog.SetDefault(previousLogger)
		globalStateMu.Unlock()
		return nil
	})

	metrics, err := startMetricsServer(opts.MetricsAddr)
	if err != nil {
		_ = shutdown(ctx)
		return nil, err
	}
	if metrics != nil {
		observer = combineObservers(observer, metrics)
		shutdown = joinShutdown(shutdown, metrics.Shutdown)
		logger.Info("metrics endpoint started", "addr", opts.MetricsAddr, "path", "/metrics")
	}

	if observer == nil {
		observer = migrator.NoopObserver{}
	}

	return &Runtime{
		logger:   logger,
		observer: observer,
		human:    human,
		shutdown: shutdown,
	}, nil
}

// Logger returns the command logger.
func (r *Runtime) Logger() *slog.Logger {
	if r == nil || r.logger == nil {
		return slog.Default()
	}
	return r.logger
}

// Observer returns the migration observer.
func (r *Runtime) Observer() migrator.Observer {
	if r == nil || r.observer == nil {
		return migrator.NoopObserver{}
	}
	return r.observer
}

// HumanOutput reports whether the command should keep its traditional stdout
// report. JSON logging mode disables human output so every stdout line remains
// parseable JSON.
func (r *Runtime) HumanOutput() bool {
	return r == nil || r.human
}

// Shutdown flushes and closes observability resources.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil || r.shutdown == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		err = r.shutdown(ctx)
	})
	return err
}

// Emitter writes human output in text mode and structured log messages in JSON
// mode.
type Emitter struct {
	out    io.Writer
	logger *slog.Logger
	human  bool
}

// NewEmitter returns an output adapter for a command.
func NewEmitter(out io.Writer, runtime *Runtime) Emitter {
	if out == nil {
		out = os.Stdout
	}
	logger := slog.Default()
	human := true
	if runtime != nil {
		logger = runtime.Logger()
		human = runtime.HumanOutput()
	}
	return Emitter{
		out:    out,
		logger: logger,
		human:  human,
	}
}

// NewOutputWriter returns a writer for subprocess output. Text mode preserves
// the original stream; JSON mode logs complete lines as structured records.
func NewOutputWriter(out io.Writer, runtime *Runtime, msg string) io.Writer {
	if out == nil {
		out = os.Stdout
	}
	if runtime == nil || runtime.HumanOutput() {
		return out
	}
	if msg == "" {
		msg = "command output"
	}
	return &lineLogWriter{
		logger: runtime.Logger(),
		msg:    msg,
	}
}

// ObserveNoopMigration records a migration command span that intentionally does
// not enter the migrator execution path.
func ObserveNoopMigration(ctx context.Context, observer migrator.Observer, name string, attrs ...migrator.ObservationAttribute) {
	if observer == nil {
		observer = migrator.NoopObserver{}
	}
	_, span := observer.StartSpan(ctx, name, attrs...)
	span.End(nil)
}

// Println emits a line.
func (e Emitter) Println(args ...any) {
	if e.human {
		fmt.Fprintln(e.out, args...)
		return
	}
	msg := strings.TrimSpace(fmt.Sprintln(args...))
	if msg != "" {
		e.logger.Info(msg)
	}
}

// Printf emits a formatted line.
func (e Emitter) Printf(format string, args ...any) {
	if e.human {
		fmt.Fprintf(e.out, format, args...)
		return
	}
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	if msg != "" {
		e.logger.Info(msg)
	}
}

// Info emits a structured lifecycle event.
func (e Emitter) Info(msg string, attrs ...any) {
	if e.human {
		fmt.Fprintln(e.out, msg)
		return
	}
	e.logger.Info(msg, attrs...)
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid --%s value %q: expected debug, info, warn, or error", LogLevelFlagName, value)
	}
}

func newCorrelationID() string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes[:])
}

func joinShutdown(first, second func(context.Context) error) func(context.Context) error {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return func(ctx context.Context) error {
		err := first(ctx)
		if secondErr := second(ctx); secondErr != nil && err == nil {
			err = secondErr
		}
		return err
	}
}

type compositeObserver []migrator.Observer

func combineObservers(observers ...migrator.Observer) migrator.Observer {
	combined := make(compositeObserver, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			combined = append(combined, observer)
		}
	}
	if len(combined) == 0 {
		return nil
	}
	if len(combined) == 1 {
		return combined[0]
	}
	return combined
}

func (o compositeObserver) StartSpan(ctx context.Context, name string, attrs ...migrator.ObservationAttribute) (context.Context, migrator.ObservationSpan) {
	spans := make([]migrator.ObservationSpan, 0, len(o))
	for _, observer := range o {
		nextCtx, span := observer.StartSpan(ctx, name, attrs...)
		ctx = nextCtx
		if span != nil {
			spans = append(spans, span)
		}
	}
	return ctx, compositeSpan(spans)
}

func (o compositeObserver) AddCounter(ctx context.Context, name string, value int64, attrs ...migrator.ObservationAttribute) {
	for _, observer := range o {
		observer.AddCounter(ctx, name, value, attrs...)
	}
}

func (o compositeObserver) RecordDuration(ctx context.Context, name string, duration time.Duration, attrs ...migrator.ObservationAttribute) {
	for _, observer := range o {
		observer.RecordDuration(ctx, name, duration, attrs...)
	}
}

type compositeSpan []migrator.ObservationSpan

func (s compositeSpan) SetAttributes(attrs ...migrator.ObservationAttribute) {
	for _, span := range s {
		span.SetAttributes(attrs...)
	}
}

func (s compositeSpan) End(err error) {
	for _, span := range s {
		span.End(err)
	}
}

type lineLogWriter struct {
	mu     sync.Mutex
	logger *slog.Logger
	msg    string
	buf    []byte
}

func (w *lineLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf = append(w.buf, p...)
	for {
		idx := slices.Index(w.buf, '\n')
		if idx < 0 {
			break
		}
		w.logLine(string(w.buf[:idx]))
		w.buf = append(w.buf[:0], w.buf[idx+1:]...)
	}
	return len(p), nil
}

// Flush logs any buffered partial line.
func (w *lineLogWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buf) == 0 {
		return
	}
	w.logLine(string(w.buf))
	w.buf = w.buf[:0]
}

func (w *lineLogWriter) logLine(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	w.logger.Info(w.msg, "output", line)
}
