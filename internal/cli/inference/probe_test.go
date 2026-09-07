package inference_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestProbe_MeasuresTheEndpointWithNoDatabase is why the verb exists.
//
// Every other verb that touches a provider needs PostgreSQL and a prepared run,
// so the first thing that measured an endpoint used to be a backfill -- which
// had already sent source rows to it. This reaches the same endpoint before any
// of that, and reaches it from a test with no database at all, which is also
// what a CI job has.
func TestProbe_MeasuresTheEndpointWithNoDatabase(t *testing.T) {
	c := qt.New(t)
	endpoint := httptest.NewServer(http.HandlerFunc(probeEndpoint(4)))
	defer endpoint.Close()

	output, _, err := runInferenceCommand(c, "probe",
		"--spec", writeProbeSpec(c, endpoint.URL, 4))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "ok   reachable")
	c.Assert(output, qt.Contains, "ok   authorized")
	c.Assert(output, qt.Contains, "ok   embeds")
	c.Assert(output, qt.Contains, "ok   dimension: 4 dimensions, as declared")
	c.Assert(output, qt.Contains, "ok   batch")
	c.Assert(output, qt.Contains, "ok   cancellation")
}

// TestProbe_RefusesADeclaredWidthTheProviderDoesNotAnswerWith is the failure
// this verb exists to move earlier.
//
// Before it, the specification said 4, the endpoint answered 1024, and the two
// met for the first time inside a backfill.
func TestProbe_RefusesADeclaredWidthTheProviderDoesNotAnswerWith(t *testing.T) {
	c := qt.New(t)
	endpoint := httptest.NewServer(http.HandlerFunc(probeEndpoint(1024)))
	defer endpoint.Close()

	output, _, err := runInferenceCommand(c, "probe",
		"--spec", writeProbeSpec(c, endpoint.URL, 4))

	c.Assert(err, qt.ErrorMatches, `\d+ of \d+ provider checks did not pass`)
	c.Assert(output, qt.Contains, "answered with 1024 dimensions and the specification declares 4")
}

// TestProbe_ReportsNothingItSent is the property that lets this be run before a
// decision about data has been taken.
//
// The two inputs are fixed strings and no vector is in the report, so what the
// verb prints can be pasted into an issue by somebody who has not yet agreed to
// send their corpus anywhere.
func TestProbe_ReportsNothingItSent(t *testing.T) {
	c := qt.New(t)
	var received []string
	endpoint := httptest.NewServer(http.HandlerFunc(recordingProbeEndpoint(4, &received)))
	defer endpoint.Close()

	body, _, err := runInferenceCommand(c, "probe",
		"--spec", writeProbeSpec(c, endpoint.URL, 4), "--format", "json")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", body))

	var document struct {
		Endpoint   string `json:"endpoint"`
		Credential string `json:"credential_source"`
		Dimension  int    `json:"dimension"`
		Passed     bool   `json:"passed"`
		Checks     []struct {
			Name   string `json:"name"`
			Passed bool   `json:"passed"`
		} `json:"checks"`
	}
	c.Assert(json.Unmarshal([]byte(body), &document), qt.IsNil)

	c.Assert(document.Passed, qt.IsTrue)
	c.Assert(document.Dimension, qt.Equals, 4)
	// The credential is named by where it lives, never by what it is.
	c.Assert(document.Credential, qt.Equals, "env:PTAH_PROBE_TOKEN")
	c.Assert(body, qt.Not(qt.Contains), "probe-token-value")
	// Everything that went out is one of the two fixed strings. A probe that
	// had reached for a row would show here.
	c.Assert(received, qt.Not(qt.HasLen), 0)
	for _, input := range received {
		c.Assert(strings.HasPrefix(input, "ptah provider probe"), qt.IsTrue,
			qt.Commentf("the probe sent %q", input))
	}
	// And no vector came back into the report.
	c.Assert(body, qt.Not(qt.Contains), "0.5")
}

// TestProbe_AnUnreachableEndpointSaysSoAndNamesWhatItCouldNotMeasure keeps the
// refusal one refusal.
func TestProbe_AnUnreachableEndpointSaysSoAndNamesWhatItCouldNotMeasure(t *testing.T) {
	c := qt.New(t)

	output, _, err := runInferenceCommand(c, "probe",
		"--spec", writeProbeSpec(c, "http://127.0.0.1:9", 4))

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "fail reachable")
	c.Assert(output, qt.Contains, "not measured: everything after reachability")
}

// probeEndpoint answers an embeddings request with vectors of one width.
func probeEndpoint(width int) http.HandlerFunc {
	return recordingProbeEndpoint(width, nil)
}

// recordingProbeEndpoint is the same, keeping what it was sent.
func recordingProbeEndpoint(width int, received *[]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		if received != nil {
			*received = append(*received, request.Input...)
		}
		if strings.Contains(request.Model, "no-such-model") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"message":"no such model"}}`))
			return
		}
		response := struct {
			Data []struct {
				Index     int       `json:"index"`
				Embedding []float32 `json:"embedding"`
			} `json:"data"`
		}{}
		for index := range request.Input {
			vector := make([]float32, width)
			for position := range vector {
				vector[position] = 0.5
			}
			response.Data = append(response.Data, struct {
				Index     int       `json:"index"`
				Embedding []float32 `json:"embedding"`
			}{Index: index, Embedding: vector})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}
}

// writeProbeSpec writes a specification naming an endpoint and a width.
func writeProbeSpec(c *qt.C, endpoint string, dimension int) string {
	c.Helper()
	c.Setenv("PTAH_PROBE_TOKEN", "probe-token-value")
	document := `
version: 1
name: probe
source:
  schema: public
  table: docs
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
preprocessing:
  separator: "\n"
  null_policy: empty
  empty_policy: skip
  unicode_normalization: none
  truncate: refuse
model:
  provider: openai-compatible
  endpoint_class: local
  endpoint: ` + endpoint + `/v1
  identifier: test-embed
  revision: "1"
  credential: env:PTAH_PROBE_TOKEN
  reported_dimension: ` + strconv.Itoa(dimension) + `
  normalization: none
target:
  schema: public
  table: docs
  column: embedding
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`
	path := filepath.Join(c.TempDir(), "probe-spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
