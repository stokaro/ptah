package cliobs_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"

	qt "github.com/frankban/quicktest"
)

// otlpRequest is one request the receiver took, kept as it arrived.
type otlpRequest struct {
	method      string
	path        string
	contentType string
	body        []byte
}

// otlpReceiver stands in for an OpenTelemetry collector listening for OTLP over
// HTTP. It answers every request with 200 and keeps it, so a test can read what
// an exporter sent.
type otlpReceiver struct {
	server *httptest.Server
	mu     sync.Mutex
	taken  []otlpRequest
}

// newOTLPReceiver starts a receiver on a loopback port and closes it when the
// test ends.
func newOTLPReceiver(c *qt.C) *otlpReceiver {
	c.Helper()
	receiver := &otlpReceiver{}
	receiver.server = httptest.NewServer(http.HandlerFunc(receiver.serve))
	c.Cleanup(receiver.server.Close)
	return receiver
}

func (r *otlpReceiver) serve(w http.ResponseWriter, req *http.Request) {
	// A body cut short fails to decode in the test that reads it.
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.taken = append(r.taken, otlpRequest{
		method:      req.Method,
		path:        req.URL.Path,
		contentType: req.Header.Get("Content-Type"),
		body:        body,
	})
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

// requests closes the receiver and returns what it took. Closing first waits
// for every request in flight, so nothing sent before the call is missed.
func (r *otlpReceiver) requests() []otlpRequest {
	r.server.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.taken)
}
