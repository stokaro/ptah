package embedprovider_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedprovider"
)

// endpoint stands an embedding server up and returns a provider pointed at it.
//
// The handler is the test's whole fixture: what a provider ANSWERS is the thing
// under test, and a fake that only ever answered correctly could not measure
// any of the refusals.
func endpoint(c *qt.C, dimension int, handler http.HandlerFunc) embedprovider.Provider {
	c.Helper()

	server := httptest.NewServer(handler)
	c.Cleanup(server.Close)

	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name:          "test",
		BaseURL:       server.URL + "/v1",
		Model:         "test-model",
		EndpointClass: "local",
		Dimension:     dimension,
	})
	c.Assert(err, qt.IsNil)
	return provider
}

// answer writes a well-formed response carrying the given vectors, in the given
// index order.
func answer(w http.ResponseWriter, indices []int, vectors [][]float32) {
	type entry struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	}
	payload := map[string]any{"model": "test-model"}
	data := make([]entry, 0, len(vectors))
	for position, vector := range vectors {
		data = append(data, entry{Index: indices[position], Embedding: vector})
	}
	payload["data"] = data
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// TestOpenAICompatible_ReturnsVectorsInTheInputOrder is the defect a trusting
// decoder ships.
//
// A provider may answer out of order. A caller that took the array's order
// would attribute vectors to the wrong rows, which produces a corpus that
// retrieves the wrong documents and looks entirely healthy from the outside
// (stokaro/ptah#2068).
func TestOpenAICompatible_ReturnsVectorsInTheInputOrder(t *testing.T) {
	c := qt.New(t)
	provider := endpoint(c, 2, func(w http.ResponseWriter, _ *http.Request) {
		// The second input's answer arrives first.
		answer(w, []int{1, 0}, [][]float32{{3, 4}, {1, 2}})
	})

	result, err := provider.Embed(context.Background(), []string{"first", "second"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Vectors, qt.HasLen, 2)
	c.Assert([]float32(result.Vectors[0]), qt.DeepEquals, []float32{1, 2})
	c.Assert([]float32(result.Vectors[1]), qt.DeepEquals, []float32{3, 4})
}

// TestOpenAICompatible_RefusesAnAnswerThatDoesNotSurviveValidation walks the
// answers a provider can give that would otherwise become rows.
func TestOpenAICompatible_RefusesAnAnswerThatDoesNotSurviveValidation(t *testing.T) {
	tests := []struct {
		name     string
		handler  http.HandlerFunc
		wantErr  error
		wantText string
	}{
		{
			name: "a short batch",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				answer(w, []int{0}, [][]float32{{1, 2}})
			},
			wantErr: embedprovider.ErrInvalidResponse, wantText: `.*partial batch.*`,
		},
		{
			name: "an index that names no input position",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				answer(w, []int{0, 7}, [][]float32{{1, 2}, {3, 4}})
			},
			wantErr: embedprovider.ErrInvalidResponse, wantText: `.*association is not stable.*`,
		},
		{
			name: "an error inside a nominal success",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"error":{"message":"model is loading","type":"server_error"}}`))
			},
			wantErr: embedprovider.ErrProvider, wantText: `.*model is loading.*`,
		},
		{
			name: "a body that is not the shape",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`not json at all`))
			},
			wantErr: embedprovider.ErrInvalidResponse, wantText: `.*decode response.*`,
		},
		{
			name: "the wrong dimension",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				answer(w, []int{0, 1}, [][]float32{{1, 2}, {3, 4, 5}})
			},
			wantErr: embedprovider.ErrInvalidResponse, wantText: `.*vector 1 has 3 dimensions.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := endpoint(c, 2, test.handler)

			_, err := provider.Embed(context.Background(), []string{"first", "second"})

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(err, qt.ErrorMatches, test.wantText)
		})
	}
}

// TestOpenAICompatible_SeparatesTheFailuresAnOperatorActsOn pins the error
// classes.
//
// "It did not work" is not an answer an operator can act on: an unreachable
// endpoint, a refused credential and a provider that answered with an error are
// three different next steps.
func TestOpenAICompatible_SeparatesTheFailuresAnOperatorActsOn(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr error
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, wantErr: embedprovider.ErrUnauthorized},
		{name: "forbidden", status: http.StatusForbidden, wantErr: embedprovider.ErrUnauthorized},
		{name: "a provider error", status: http.StatusInternalServerError, wantErr: embedprovider.ErrProvider},
		{name: "a bad request", status: http.StatusBadRequest, wantErr: embedprovider.ErrProvider},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := endpoint(c, 2, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(`{"message":"refused"}`))
			})

			_, err := provider.Embed(context.Background(), []string{"first"})

			c.Assert(err, qt.ErrorIs, test.wantErr)
		})
	}
}

// TestOpenAICompatible_AnUnreachableEndpointSaysSo is the class a caller reads
// as "nothing was sent".
func TestOpenAICompatible_AnUnreachableEndpointSaysSo(t *testing.T) {
	c := qt.New(t)
	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name: "test", BaseURL: "http://127.0.0.1:1/v1", Model: "test-model",
	})
	c.Assert(err, qt.IsNil)

	_, err = provider.Embed(context.Background(), []string{"first"})

	c.Assert(err, qt.ErrorIs, embedprovider.ErrUnreachable)
}

// TestOpenAICompatible_CancellationIsHonoured is the epic's explicit provider
// requirement, and it is what stops a stuck endpoint holding a migration open.
func TestOpenAICompatible_CancellationIsHonoured(t *testing.T) {
	c := qt.New(t)
	release := make(chan struct{})
	provider := endpoint(c, 2, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(release)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := provider.Embed(ctx, []string{"first"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, context.Canceled)
}

// TestOpenAICompatible_ProfileCarriesNoCredential is the privacy rule in the
// one place a report reads from.
//
// The profile is what an operator is shown and what a run records. It names
// WHERE the credential lives so somebody can find it, and never what it is.
func TestOpenAICompatible_ProfileCarriesNoCredential(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_TEST_EMBED_TOKEN", "super-secret-value")
	reference, err := embedprovider.ParseCredentialRef("env:PTAH_TEST_EMBED_TOKEN")
	c.Assert(err, qt.IsNil)
	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name: "test", BaseURL: "https://api.example.test/v1", Model: "test-model",
		Credential: reference, EndpointClass: "hosted",
	})
	c.Assert(err, qt.IsNil)

	profile := provider.Profile()

	c.Assert(profile.CredentialSource, qt.Equals, "env:PTAH_TEST_EMBED_TOKEN")
	c.Assert(profile.EndpointHost, qt.Equals, "api.example.test")
	rendered, err := json.Marshal(profile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered), qt.Not(qt.Contains), "super-secret-value")
	// Ptah speaks only for Ptah: whether the PROVIDER retains anything is
	// outside its knowledge and the profile must not claim otherwise.
	c.Assert(profile.Retains, qt.IsFalse)
}

// TestOpenAICompatible_SendsTheCredentialItWasReferred is the other half: a
// reference nobody resolves is a provider that never authenticates.
func TestOpenAICompatible_SendsTheCredentialItWasReferred(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_TEST_EMBED_TOKEN", "super-secret-value")
	reference, err := embedprovider.ParseCredentialRef("env:PTAH_TEST_EMBED_TOKEN")
	c.Assert(err, qt.IsNil)

	var seen string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("Authorization")
		answer(w, []int{0}, [][]float32{{1, 2}})
	}))
	c.Cleanup(server.Close)
	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name: "test", BaseURL: server.URL + "/v1", Model: "test-model",
		Credential: reference, Dimension: 2,
	})
	c.Assert(err, qt.IsNil)

	_, err = provider.Embed(context.Background(), []string{"first"})

	c.Assert(err, qt.IsNil)
	c.Assert(seen, qt.Equals, "Bearer super-secret-value")
}

// TestOpenAICompatible_RefusesAConfigurationItCannotHonour is the failure that
// belongs at construction rather than mid-run.
func TestOpenAICompatible_RefusesAConfigurationItCannotHonour(t *testing.T) {
	tests := []struct {
		name    string
		options embedprovider.OpenAICompatibleOptions
		want    string
	}{
		{
			name:    "no base URL",
			options: embedprovider.OpenAICompatibleOptions{Name: "x", Model: "m"},
			want:    `.*a base URL is required.*`,
		},
		{
			name:    "no model",
			options: embedprovider.OpenAICompatibleOptions{Name: "x", BaseURL: "http://h/v1"},
			want:    `.*a model identifier is required.*`,
		},
		{
			name:    "a base URL naming no host",
			options: embedprovider.OpenAICompatibleOptions{Name: "x", BaseURL: "/v1", Model: "m"},
			want:    `.*names no host.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := embedprovider.NewOpenAICompatible(test.options)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestOpenAICompatible_ARefusedCredentialCarriesTheProvidersReason covers
// stokaro/ptah#2641 finding 5.
//
// 401 and 403 were the one status class whose body was dropped, so an operator
// was told a number and nothing else -- and a provider answers those for a
// wrong key, an expired one, a key without the model, the wrong organization
// and an exhausted quota alike.
func TestOpenAICompatible_ARefusedCredentialCarriesTheProvidersReason(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "forbidden", status: http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := endpoint(c, 2, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(`{"error":{"message":"the key has no access to test-model"}}`))
			})

			_, err := provider.Embed(context.Background(), []string{"first"})

			c.Assert(err, qt.ErrorIs, embedprovider.ErrUnauthorized)
			c.Assert(err.Error(), qt.Contains, "the key has no access to test-model")
		})
	}
}

// TestOpenAICompatible_AQuotedBodyNeverCarriesTheCredential is the control, and
// it is the reason the body is not quoted raw.
//
// A provider answering 401 commonly echoes the key it rejected. Quoting the
// body is therefore exactly where a credential reaches a log, an exit message
// and a CI transcript -- the disclosure stokaro/ptah#2644 closed at the
// specification end, read back from the response end.
//
// Both status classes are asserted, because a rule that holds for one of them
// is one somebody has to remember.
func TestOpenAICompatible_AQuotedBodyNeverCarriesTheCredential(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "a bad request", status: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// #nosec G101 -- a fixture, and its shape is the point: the test
			// asserts a key is redacted from a body that echoes it, so a value
			// that did not look like one would not exercise the redaction.
			const secret = "sk-a-key-nobody-should-read"
			c.Setenv("PTAH_2641_TOKEN", secret)
			provider := credentialledEndpoint(c, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(`{"error":{"message":"Incorrect API key provided: ` + secret + `"}}`))
			})

			_, err := provider.Embed(context.Background(), []string{"first"})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), secret)
			// And the rest of the sentence survives, so the redaction is not
			// achieved by dropping the body again.
			c.Assert(err.Error(), qt.Contains, "Incorrect API key provided: [redacted]")
		})
	}
}

// credentialledEndpoint is [endpoint] with a resolvable credential, which is
// what makes a redaction assertion possible at all.
func credentialledEndpoint(c *qt.C, handler http.HandlerFunc) embedprovider.Provider {
	c.Helper()

	server := httptest.NewServer(handler)
	c.Cleanup(server.Close)

	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name:          "test",
		BaseURL:       server.URL + "/v1",
		Model:         "test-model",
		EndpointClass: "hosted",
		Dimension:     2,
		Credential:    embedprovider.CredentialRef{Scheme: "env", Locator: "PTAH_2641_TOKEN"},
	})
	c.Assert(err, qt.IsNil)
	return provider
}

// TestOpenAICompatible_RawAnswersHandsBackWhatArrived is the option the probe
// needs, and the pair with the control below is what makes it a decision rather
// than a hole.
//
// The probe's job is to say WHICH way an endpoint is wrong. While the adapter
// refused every malformed answer, one empty vector became
// `the model did not answer an embedding request` -- for an endpoint that
// answered over HTTP 200 -- and the shape, dimension, batch and cancellation
// checks were all abandoned as unmeasurable (stokaro/ptah#2641).
func TestOpenAICompatible_RawAnswersHandsBackWhatArrived(t *testing.T) {
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		answer(w, []int{0}, [][]float32{{}})
	}))
	c.Cleanup(server.Close)
	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name: "test", BaseURL: server.URL + "/v1", Model: "test-model",
		EndpointClass: "local", RawAnswers: true,
	})
	c.Assert(err, qt.IsNil)

	result, err := provider.Embed(context.Background(), []string{"one"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Vectors, qt.HasLen, 1)
	c.Assert(result.Vectors[0], qt.HasLen, 0)
}

// TestOpenAICompatible_WithoutRawAnswersTheSameAnswerIsRefused is the control,
// and it is the half that matters most.
//
// Every other caller depends on the refusal: a vector that fails validation
// must not reach a target, and `embedverify` cites that guarantee in its own
// report. An option that quietly turned validation off for everybody would
// satisfy the test above and break that.
func TestOpenAICompatible_WithoutRawAnswersTheSameAnswerIsRefused(t *testing.T) {
	c := qt.New(t)
	provider := endpoint(c, 0, func(w http.ResponseWriter, _ *http.Request) {
		answer(w, []int{0}, [][]float32{{}})
	})

	_, err := provider.Embed(context.Background(), []string{"one"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "vector 0 is empty")
}

// TestOpenAICompatible_AnAnswerWithNoUsageSaysItReportedNone covers the review
// finding on stokaro/ptah#2707.
//
// `json.Unmarshal` leaves a value struct at zero whether the field was absent
// or carried zeros, so a provider that reports no usage at all was
// indistinguishable from one reporting nothing spent -- and the counts are what
// an operator compares against an invoice. Decoding into a pointer is what
// separates them.
func TestOpenAICompatible_AnAnswerWithNoUsageSaysItReportedNone(t *testing.T) {
	c := qt.New(t)
	provider := endpoint(c, 2, func(w http.ResponseWriter, _ *http.Request) {
		answer(w, []int{0}, [][]float32{{0.1, 0.2}})
	})

	result, err := provider.Embed(context.Background(), []string{"one"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Usage.Reported, qt.IsFalse)
	c.Assert(result.Usage.PromptTokens, qt.Equals, 0)
	c.Assert(result.Usage.TotalTokens, qt.Equals, 0)
}

// TestOpenAICompatible_AnAnswerCarryingUsageKeepsIt is the control.
//
// Every assertion above is satisfied by a decoder that stopped reading usage
// altogether, which would leave the Cost section with nothing to report.
func TestOpenAICompatible_AnAnswerCarryingUsageKeepsIt(t *testing.T) {
	c := qt.New(t)
	provider := endpoint(c, 2, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "test-model",
			"data": []map[string]any{
				{"index": 0, "embedding": []float32{0.1, 0.2}},
			},
			"usage": map[string]any{"prompt_tokens": 7, "total_tokens": 9},
		})
	})

	result, err := provider.Embed(context.Background(), []string{"one"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Usage.Reported, qt.IsTrue)
	c.Assert(result.Usage.PromptTokens, qt.Equals, 7)
	c.Assert(result.Usage.TotalTokens, qt.Equals, 9)
}

// TestOpenAICompatible_AnAnswerReportingZeroIsNotAnAnswerReportingNothing is
// the pair that makes the distinction worth carrying.
//
// Both leave the counts at zero. Only the flag separates a provider that
// charged nothing from one that said nothing, and `status` renders a different
// sentence for each.
func TestOpenAICompatible_AnAnswerReportingZeroIsNotAnAnswerReportingNothing(t *testing.T) {
	c := qt.New(t)
	provider := endpoint(c, 2, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "test-model",
			"data": []map[string]any{
				{"index": 0, "embedding": []float32{0.1, 0.2}},
			},
			"usage": map[string]any{"prompt_tokens": 0, "total_tokens": 0},
		})
	})

	result, err := provider.Embed(context.Background(), []string{"one"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Usage.Reported, qt.IsTrue)
	c.Assert(result.Usage.PromptTokens, qt.Equals, 0)
}

// TestEmbed_ACanceledRequestIsNotAnUnreachableEndpoint is stokaro/ptah#2649
// finding 10, at the provider.
//
// An operator stopping the run is not an endpoint that could not be reached,
// and classifying it as one sent them looking at a provider that was answering
// perfectly well.
func TestEmbed_ACanceledRequestIsNotAnUnreachableEndpoint(t *testing.T) {
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(time.Minute)
	}))
	defer server.Close()

	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name: "local", BaseURL: server.URL + "/v1", Model: "test-embed",
	})
	c.Assert(err, qt.IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = provider.Embed(ctx, []string{"a document"})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(err, qt.Not(qt.ErrorIs), embedprovider.ErrUnreachable)
}

// TestEmbed_ADeadlineIsStillAnUnreachableEndpoint is the control.
//
// `--provider-timeout` expiring IS a fact about the endpoint, and a fix that
// reclassified every context error would take away the one word saying which.
func TestEmbed_ADeadlineIsStillAnUnreachableEndpoint(t *testing.T) {
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(time.Minute)
	}))
	defer server.Close()

	provider, err := embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name: "local", BaseURL: server.URL + "/v1", Model: "test-embed",
		Timeout: 50 * time.Millisecond,
	})
	c.Assert(err, qt.IsNil)

	_, err = provider.Embed(context.Background(), []string{"a document"})

	c.Assert(err, qt.ErrorIs, embedprovider.ErrUnreachable)
}
