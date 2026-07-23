package cliobs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/cliobs"
)

func TestEmitterJSONModeWritesParseableJSONLines(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	runtime, err := cliobs.Start(context.Background(), cliobs.Options{
		Command:   "test",
		LogFormat: "json",
		LogLevel:  "info",
		LogWriter: &out,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Assert(runtime.Shutdown(context.Background()), qt.IsNil)
	})

	emit := cliobs.NewEmitter(&out, runtime)
	emit.Println("plain status")
	emit.Printf("version: %d\n", 42)

	for _, payload := range parseJSONLogLines(c, out.String()) {
		c.Assert(payload["msg"], qt.Not(qt.Equals), "")
		c.Assert(payload["correlation_id"], qt.Not(qt.Equals), "")
	}
}

func TestJSONOutputWriterLogsSubprocessLines(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	runtime, err := cliobs.Start(context.Background(), cliobs.Options{
		Command:   "test",
		LogFormat: "json",
		LogLevel:  "info",
		LogWriter: &out,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Assert(runtime.Shutdown(context.Background()), qt.IsNil)
	})

	writer := cliobs.NewOutputWriter(&out, runtime, "hook output")
	_, err = writer.Write([]byte("line one\nline two\n"))
	c.Assert(err, qt.IsNil)

	var records int
	for _, payload := range parseJSONLogLines(c, out.String()) {
		c.Assert(payload["msg"], qt.Equals, "hook output")
		c.Assert(payload["output"], qt.Not(qt.Equals), "")
		records++
	}
	c.Assert(records, qt.Equals, 2)
}

func TestJSONOutputWriterFlushesPartialLine(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	runtime, err := cliobs.Start(context.Background(), cliobs.Options{
		Command:   "test",
		LogFormat: "json",
		LogLevel:  "info",
		LogWriter: &out,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Assert(runtime.Shutdown(context.Background()), qt.IsNil)
	})

	writer := cliobs.NewOutputWriter(&out, runtime, "hook output")
	_, err = writer.Write([]byte("partial line"))
	c.Assert(err, qt.IsNil)
	flusher, ok := writer.(interface{ Flush() })
	c.Assert(ok, qt.IsTrue)
	flusher.Flush()

	var payload map[string]any
	c.Assert(json.Unmarshal(bytes.TrimSpace(out.Bytes()), &payload), qt.IsNil)
	c.Assert(payload["output"], qt.Equals, "partial line")
}

func TestStartRejectsInvalidLogOptions(t *testing.T) {
	c := qt.New(t)

	c.Run("invalid log format", func(c *qt.C) {
		_, err := cliobs.Start(context.Background(), cliobs.Options{LogFormat: "xml"})
		c.Assert(err, qt.IsNotNil)
	})

	c.Run("invalid log level", func(c *qt.C) {
		_, err := cliobs.Start(context.Background(), cliobs.Options{LogLevel: "trace"})
		c.Assert(err, qt.IsNotNil)
	})
}

func parseJSONLogLines(c *qt.C, raw string) []map[string]any {
	c.Helper()

	lines := strings.Split(strings.TrimSpace(raw), "\n")
	payloads := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var payload map[string]any
		c.Assert(json.Unmarshal([]byte(line), &payload), qt.IsNil)
		payloads = append(payloads, payload)
	}

	return payloads
}
