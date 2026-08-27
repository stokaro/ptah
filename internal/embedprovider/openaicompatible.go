package embedprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// OpenAICompatibleOptions configures the one adapter this build ships.
//
// "Compatible" means measured against this adapter's contract, and nothing
// more: a server that accepts a similar URL is not thereby supported, and the
// provider test is what turns "it looked right" into "it answered correctly"
// (stokaro/ptah#2068).
type OpenAICompatibleOptions struct {
	// Name is the operator's name for this configuration.
	Name string
	// BaseURL is the endpoint root, for example http://localhost:11434/v1.
	BaseURL string
	// Model is the model identifier the endpoint knows.
	Model string
	// Revision is an immutable revision where the operator can state one. It is
	// recorded and never invented.
	Revision string
	// Credential names where the token lives. A local endpoint may leave it
	// unset.
	Credential CredentialRef
	// EndpointClass is local, hosted or gateway.
	EndpointClass string
	// Dimension is the output dimension expected of every answer. Zero accepts
	// whatever the endpoint returns and records it on the first response.
	Dimension int
	// RequestedDimension asks the endpoint for a specific output size where it
	// supports one. Zero omits the field.
	RequestedDimension int
	// MaxBatch is the largest number of inputs per request. Zero sends them one
	// at a time, which is the answer that cannot be wrong.
	MaxBatch int
	// Timeout bounds one request.
	Timeout time.Duration
	// HTTPClient is used when set, which is what a test substitutes.
	HTTPClient *http.Client
}

// openAICompatible is an embedding endpoint speaking the OpenAI embeddings
// shape.
type openAICompatible struct {
	options OpenAICompatibleOptions
	client  *http.Client
	host    string
}

// NewOpenAICompatible builds the adapter, refusing a configuration it cannot
// honour rather than discovering it mid-run.
func NewOpenAICompatible(options OpenAICompatibleOptions) (Provider, error) {
	if strings.TrimSpace(options.BaseURL) == "" {
		return nil, fmt.Errorf("embedding provider %q: a base URL is required", options.Name)
	}
	if strings.TrimSpace(options.Model) == "" {
		return nil, fmt.Errorf("embedding provider %q: a model identifier is required", options.Name)
	}
	parsed, err := url.Parse(options.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("embedding provider %q: parse base URL: %w", options.Name, err)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("embedding provider %q: base URL %q names no host", options.Name, options.BaseURL)
	}

	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: options.Timeout}
	}
	return &openAICompatible{options: options, client: client, host: parsed.Host}, nil
}

// Profile describes the endpoint without revealing the credential.
func (p *openAICompatible) Profile() Profile {
	return Profile{
		Name:          p.options.Name,
		Provider:      "openai-compatible",
		EndpointClass: p.options.EndpointClass,
		EndpointHost:  p.host,
		Model:         p.options.Model,
		Revision:      p.options.Revision,
		Dimension:     p.options.Dimension,
		MaxBatch:      p.options.MaxBatch,
		// The reference, never the value.
		CredentialSource: p.options.Credential.String(),
		// Ptah holds the input for the length of the request and does not
		// store it. What the PROVIDER retains is outside Ptah's knowledge and
		// this does not speak for it.
		Retains: false,
	}
}

// embeddingRequest is the wire request.
type embeddingRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions,omitempty"`
}

// embeddingResponse is the wire response, including the error shape a provider
// may return inside a 200.
type embeddingResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Model string `json:"model"`
	Usage struct {
		PromptTokens int `json:"prompt_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// Embed asks the endpoint for one vector per input.
//
// There is no fallback here and no retry against another model: a failure is
// returned to the caller, which is the only place a decision about sending
// source content somewhere else may be made, and this build makes none.
func (p *openAICompatible) Embed(ctx context.Context, inputs []string) (Result, error) {
	if len(inputs) == 0 {
		return Result{}, nil
	}
	token, err := p.options.Credential.Resolve()
	if err != nil {
		return Result{}, fmt.Errorf("embedding provider %q: %w", p.options.Name, err)
	}

	body, err := json.Marshal(embeddingRequest{
		Model:      p.options.Model,
		Input:      inputs,
		Dimensions: p.options.RequestedDimension,
	})
	if err != nil {
		return Result{}, fmt.Errorf("embedding provider %q: encode request: %w", p.options.Name, err)
	}

	response, err := p.post(ctx, token, body)
	if err != nil {
		return Result{}, err
	}
	return p.decode(response, len(inputs))
}

// post sends one request and returns its body, mapping the transport and status
// answers onto the error classes a caller acts on.
func (p *openAICompatible) post(ctx context.Context, token string, body []byte) ([]byte, error) {
	endpoint := strings.TrimSuffix(p.options.BaseURL, "/") + "/embeddings"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embedding provider %q: build request: %w", p.options.Name, err)
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := p.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrUnreachable, p.host, err)
	}
	defer func() { _ = response.Body.Close() }()

	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("embedding provider %q: read response: %w", p.options.Name, err)
	}
	switch {
	case response.StatusCode == http.StatusUnauthorized, response.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("%w: %s answered %d", ErrUnauthorized, p.host, response.StatusCode)
	case response.StatusCode >= http.StatusBadRequest:
		return nil, fmt.Errorf("%w: %s answered %d: %s",
			ErrProvider, p.host, response.StatusCode, firstLine(string(payload)))
	}
	return payload, nil
}

// decode turns one response body into vectors in the INPUT order.
//
// The order is rebuilt from each entry's index rather than taken from the
// array's order. A provider is entitled to answer out of order, and a caller
// that trusted the array would attribute vectors to the wrong rows -- which
// produces a corpus that retrieves the wrong documents and looks entirely
// healthy.
func (p *openAICompatible) decode(payload []byte, inputs int) (Result, error) {
	var decoded embeddingResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return Result{}, fmt.Errorf("%w: %s: decode response: %w", ErrInvalidResponse, p.host, err)
	}
	// A provider error inside a nominal success is the case that looks like a
	// working request until the vectors are read.
	if decoded.Error != nil {
		return Result{}, fmt.Errorf("%w: %s: %s", ErrProvider, p.host, decoded.Error.Message)
	}

	sorted := decoded.Data
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Index < sorted[j].Index })

	result := Result{Vectors: make([]Vector, 0, len(sorted))}
	for position, entry := range sorted {
		if entry.Index != position {
			return Result{}, fmt.Errorf(
				"%w: %s answered with index %d where %d was expected; the association is not stable",
				ErrInvalidResponse, p.host, entry.Index, position)
		}
		result.Vectors = append(result.Vectors, entry.Embedding)
	}
	result.Usage = Usage{PromptTokens: decoded.Usage.PromptTokens, TotalTokens: decoded.Usage.TotalTokens}

	if err := ValidateResult(result, inputs, p.options.Dimension); err != nil {
		return Result{}, fmt.Errorf("%s: %w", p.host, err)
	}
	return result, nil
}

// firstLine keeps a provider's error body to one line for a diagnostic.
func firstLine(body string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(body), "\n")
	const bound = 200
	if len(line) > bound {
		return line[:bound] + "…"
	}
	return line
}
