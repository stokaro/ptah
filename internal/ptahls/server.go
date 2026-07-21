package ptahls

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/stokaro/ptah/internal/annotationparse"
)

const maxContentLength = 16 << 20

var (
	errExit               = errors.New("lsp exit")
	errExitBeforeShutdown = errors.New("lsp exit before shutdown")
)

// ServerOptions configures the Ptah LSP server.
type ServerOptions struct {
	Version string
}

// Run serves the Ptah LSP protocol over reader/writer.
func Run(ctx context.Context, reader io.Reader, writer io.Writer) error {
	return RunWithOptions(ctx, reader, writer, ServerOptions{})
}

// RunWithOptions serves the Ptah LSP protocol with explicit server metadata.
func RunWithOptions(ctx context.Context, reader io.Reader, writer io.Writer, opts ServerOptions) error {
	server := &server{
		reader:  bufio.NewReader(reader),
		writer:  writer,
		docs:    make(map[string]string),
		version: opts.Version,
	}
	return server.run(ctx)
}

type server struct {
	reader   *bufio.Reader
	writer   io.Writer
	mu       sync.Mutex
	docs     map[string]string
	version  string
	shutdown bool
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type textDocumentIdentifier struct {
	URI string `json:"uri"`
}

type position struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

type rangeValue struct {
	Start position `json:"start"`
	End   position `json:"end"`
}

type textDocumentItem struct {
	URI  string `json:"uri"`
	Text string `json:"text"`
}

type didOpenParams struct {
	TextDocument textDocumentItem `json:"textDocument"`
}

type contentChange struct {
	Text string `json:"text"`
}

type didChangeParams struct {
	TextDocument   textDocumentIdentifier `json:"textDocument"`
	ContentChanges []contentChange        `json:"contentChanges"`
}

type didCloseParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
}

type hoverParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
	Position     position               `json:"position"`
}

type completionParams hoverParams

type publishDiagnosticsParams struct {
	URI         string          `json:"uri"`
	Diagnostics []lspDiagnostic `json:"diagnostics"`
}

type lspDiagnostic struct {
	Range    rangeValue `json:"range"`
	Severity int        `json:"severity"`
	Code     string     `json:"code"`
	Source   string     `json:"source"`
	Message  string     `json:"message"`
}

type completionItem struct {
	Label         string        `json:"label"`
	Kind          int           `json:"kind,omitempty"`
	Detail        string        `json:"detail,omitempty"`
	Documentation markupContent `json:"documentation"`
}

type markupContent struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

func (s *server) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		msg, err := s.readMessage()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := s.handle(msg); err != nil {
			if errors.Is(err, errExit) {
				return nil
			}
			return err
		}
	}
}

func (s *server) handle(msg rpcMessage) error {
	switch msg.Method {
	case "initialize":
		serverInfo := map[string]string{
			"name": "ptah-ls",
		}
		if s.version != "" {
			serverInfo["version"] = s.version
		}
		return s.respond(msg.ID, map[string]any{
			"capabilities": map[string]any{
				"textDocumentSync": 1,
				"hoverProvider":    true,
				"completionProvider": map[string]any{
					"triggerCharacters": []string{" "},
				},
			},
			"serverInfo": serverInfo,
		})
	case "shutdown":
		s.shutdown = true
		return s.respond(msg.ID, nil)
	case "exit":
		if s.shutdown {
			return errExit
		}
		return errExitBeforeShutdown
	case "textDocument/didOpen":
		var params didOpenParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return s.respondError(msg.ID, -32602, err.Error())
		}
		s.docs[params.TextDocument.URI] = params.TextDocument.Text
		return s.publishDiagnostics(params.TextDocument.URI)
	case "textDocument/didChange":
		var params didChangeParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return s.respondError(msg.ID, -32602, err.Error())
		}
		if len(params.ContentChanges) > 0 {
			s.docs[params.TextDocument.URI] = params.ContentChanges[len(params.ContentChanges)-1].Text
		}
		return s.publishDiagnostics(params.TextDocument.URI)
	case "textDocument/didClose":
		var params didCloseParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return s.respondError(msg.ID, -32602, err.Error())
		}
		delete(s.docs, params.TextDocument.URI)
		return s.sendNotification("textDocument/publishDiagnostics", publishDiagnosticsParams{
			URI:         params.TextDocument.URI,
			Diagnostics: []lspDiagnostic{},
		})
	case "textDocument/hover":
		var params hoverParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return s.respondError(msg.ID, -32602, err.Error())
		}
		text := s.docs[params.TextDocument.URI]
		value, ok := Hover(text, fromLSPPosition(text, params.Position))
		if !ok {
			return s.respond(msg.ID, nil)
		}
		return s.respond(msg.ID, map[string]any{
			"contents": markupContent{Kind: "markdown", Value: value},
		})
	case "textDocument/completion":
		var params completionParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return s.respondError(msg.ID, -32602, err.Error())
		}
		text := s.docs[params.TextDocument.URI]
		return s.respond(msg.ID, toCompletionItems(Complete(text, fromLSPPosition(text, params.Position))))
	default:
		if msg.ID == nil {
			return nil
		}
		return s.respondError(msg.ID, -32601, "method not found")
	}
}

func (s *server) publishDiagnostics(uri string) error {
	text := s.docs[uri]
	diagnostics := Analyze(text)
	return s.sendNotification("textDocument/publishDiagnostics", publishDiagnosticsParams{
		URI:         uri,
		Diagnostics: toLSPDiagnostics(text, diagnostics),
	})
}

func (s *server) readMessage() (rpcMessage, error) {
	contentLength := -1
	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			return rpcMessage{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			n, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return rpcMessage{}, fmt.Errorf("invalid Content-Length %q: %w", value, err)
			}
			contentLength = n
		}
	}
	if contentLength < 0 {
		return rpcMessage{}, fmt.Errorf("missing Content-Length header")
	}
	if contentLength > maxContentLength {
		return rpcMessage{}, fmt.Errorf("Content-Length %d exceeds limit", contentLength)
	}
	body := make([]byte, contentLength)
	if _, err := io.ReadFull(s.reader, body); err != nil {
		return rpcMessage{}, err
	}
	var msg rpcMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return rpcMessage{}, fmt.Errorf("decode LSP message: %w", err)
	}
	return msg, nil
}

func (s *server) respond(id any, result any) error {
	return s.writePayload(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func (s *server) respondError(id any, code int, message string) error {
	return s.writeMessage(rpcMessage{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &rpcError{Code: code, Message: message},
	})
}

func (s *server) sendNotification(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	return s.writeMessage(rpcMessage{
		JSONRPC: "2.0",
		Method:  method,
		Params:  raw,
	})
}

func (s *server) writeMessage(msg rpcMessage) error {
	return s.writePayload(msg)
}

func (s *server) writePayload(payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Content-Length: %d\r\n\r\n", len(body))
	buf.Write(body)
	_, err = s.writer.Write(buf.Bytes())
	return err
}

func toLSPDiagnostics(text string, diagnostics []Diagnostic) []lspDiagnostic {
	out := make([]lspDiagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		out = append(out, lspDiagnostic{
			Range:    toLSPRange(text, diagnostic.Range),
			Severity: int(diagnostic.Severity),
			Code:     diagnostic.Code,
			Source:   diagnostic.Source,
			Message:  diagnostic.Message,
		})
	}
	return out
}

func toLSPRange(text string, r annotationparse.Range) rangeValue {
	return rangeValue{
		Start: toLSPPosition(text, r.Start),
		End:   toLSPPosition(text, r.End),
	}
}

func toLSPPosition(text string, pos annotationparse.Position) position {
	line, ok := lineAt(text, pos.Line)
	if !ok {
		return position{Line: pos.Line, Character: pos.Character}
	}
	return position{Line: pos.Line, Character: byteOffsetToUTF16(line, pos.Character)}
}

func fromLSPPosition(text string, pos position) annotationparse.Position {
	line, ok := lineAt(text, pos.Line)
	if !ok {
		return annotationparse.Position{Line: pos.Line, Character: pos.Character}
	}
	return annotationparse.Position{Line: pos.Line, Character: utf16ToByteOffset(line, pos.Character)}
}

func lineAt(text string, lineNo int) (string, bool) {
	if lineNo < 0 {
		return "", false
	}
	for i, line := range strings.Split(text, "\n") {
		if i == lineNo {
			return line, true
		}
	}
	return "", false
}

func byteOffsetToUTF16(line string, offset int) int {
	if offset <= 0 {
		return 0
	}
	units := 0
	for byteIndex, r := range line {
		if byteIndex >= offset {
			return units
		}
		units += utf16RuneLen(r)
	}
	return units
}

func utf16ToByteOffset(line string, character int) int {
	if character <= 0 {
		return 0
	}
	units := 0
	for byteIndex, r := range line {
		next := units + utf16RuneLen(r)
		if next > character {
			return byteIndex
		}
		units = next
	}
	return len(line)
}

func utf16RuneLen(r rune) int {
	if r >= 0x10000 {
		return 2
	}
	return 1
}

func toCompletionItems(completions []Completion) []completionItem {
	items := make([]completionItem, 0, len(completions))
	for _, completion := range completions {
		items = append(items, completionItem{
			Label:         completion.Label,
			Kind:          10,
			Detail:        completion.Detail,
			Documentation: markupContent{Kind: "markdown", Value: completion.Documentation},
		})
	}
	return items
}
