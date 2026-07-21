package ptahls

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRunPublishesUTF16DiagnosticsAndHandlesExit(t *testing.T) {
	c := qt.New(t)

	const uri = "file:///workspace/model.go"
	text := `//migrator:schema:field comment="привет" defaul="x"`
	input := framedMessage(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params":  map[string]any{},
	}) + framedMessage(map[string]any{
		"jsonrpc": "2.0",
		"method":  "textDocument/didOpen",
		"params": map[string]any{
			"textDocument": map[string]any{
				"uri":  uri,
				"text": text,
			},
		},
	}) + framedMessage(map[string]any{
		"jsonrpc": "2.0",
		"method":  "textDocument/didClose",
		"params": map[string]any{
			"textDocument": map[string]any{"uri": uri},
		},
	}) + framedMessage(map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "shutdown",
		"params":  map[string]any{},
	}) + framedMessage(map[string]any{
		"jsonrpc": "2.0",
		"method":  "exit",
	})

	var out bytes.Buffer
	err := RunWithOptions(context.Background(), strings.NewReader(input), &out, ServerOptions{Version: "v-test"})
	c.Assert(err, qt.IsNil)

	messages := readFramedMessages(c, out.String())
	c.Assert(messages, qt.HasLen, 4)

	initialize := decodeResult(c, messages[0])
	c.Assert(initialize["serverInfo"], qt.DeepEquals, map[string]any{
		"name":    "ptah-ls",
		"version": "v-test",
	})
	c.Assert(initialize["capabilities"].(map[string]any)["completionProvider"], qt.DeepEquals, map[string]any{
		"triggerCharacters": []any{" "},
	})

	params := decodeDiagnostics(c, messages[1])
	c.Assert(params.URI, qt.Equals, uri)
	c.Assert(params.Diagnostics, qt.HasLen, 1)
	c.Assert(params.Diagnostics[0].Code, qt.Equals, "PTAH002")
	c.Assert(params.Diagnostics[0].Range.Start.Character, qt.Equals, byteOffsetToUTF16(text, strings.Index(text, "defaul")))

	clearParams := decodeDiagnostics(c, messages[2])
	c.Assert(clearParams.Diagnostics, qt.HasLen, 0)
	c.Assert(string(messages[2].Params), qt.Contains, `"diagnostics":[]`)
}

func TestRunReturnsErrorWhenExitArrivesBeforeShutdown(t *testing.T) {
	c := qt.New(t)

	err := Run(context.Background(), strings.NewReader(framedMessage(map[string]any{
		"jsonrpc": "2.0",
		"method":  "exit",
	})), &bytes.Buffer{})

	c.Assert(err, qt.ErrorIs, errExitBeforeShutdown)
}

func TestReadMessageRejectsOversizedContentLength(t *testing.T) {
	c := qt.New(t)

	s := &server{reader: bufio.NewReader(strings.NewReader("Content-Length: 16777217\r\n\r\n"))}
	_, err := s.readMessage()

	c.Assert(err, qt.ErrorMatches, "Content-Length 16777217 exceeds limit")
}

func framedMessage(payload any) string {
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(body), body)
}

func readFramedMessages(c *qt.C, text string) []rpcMessage {
	c.Helper()

	s := &server{reader: bufio.NewReader(strings.NewReader(text))}
	var messages []rpcMessage
	for {
		msg, err := s.readMessage()
		if err != nil {
			c.Assert(err, qt.ErrorIs, io.EOF)
			return messages
		}
		messages = append(messages, msg)
	}
}

func decodeResult(c *qt.C, msg rpcMessage) map[string]any {
	c.Helper()

	raw, err := json.Marshal(msg.Result)
	c.Assert(err, qt.IsNil)
	var result map[string]any
	c.Assert(json.Unmarshal(raw, &result), qt.IsNil)
	return result
}

func decodeDiagnostics(c *qt.C, msg rpcMessage) publishDiagnosticsParams {
	c.Helper()

	var params publishDiagnosticsParams
	c.Assert(json.Unmarshal(msg.Params, &params), qt.IsNil)
	return params
}
