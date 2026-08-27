// Package embedprovider is the contract an embedding endpoint is reached
// through, and the checks every answer from one has to survive.
//
// # Why a contract rather than a client
//
// The provider is the operator's choice, and Ptah's job is to be exact about
// what was asked and what came back -- not to be clever about either. Two rules
// from stokaro/ptah#2068 shape everything here:
//
// Credentials are REFERENCES, never values. A reference names where the secret
// lives; it is resolved at the moment of use and the value is never stored in
// configuration, state, reports, artifacts or logs.
//
// There is no fallback. If the selected provider fails, source content is not
// sent to another one -- not to a default, not to a retry against a different
// model. That is not a policy this package enforces with a flag; it is the
// absence of any code that could do it.
//
// # Why validation is not optional
//
// A wrong vector is indistinguishable from a right one once it is stored: it is
// a list of numbers either way, and the corpus retrieves whatever it holds. So
// every response is checked before it can become a row -- one output per
// accepted input, the expected dimension, finite values -- and a batch that
// came back short is a failure rather than a partial success.
package embedprovider

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// Vector is one embedding.
type Vector []float32

// Usage is what a provider reported about the cost of a request.
//
// It is recorded for an operator and is deliberately outside any identity: a
// token count is a fact about one request, not about what the vectors mean.
type Usage struct {
	PromptTokens int
	TotalTokens  int
}

// Result is one provider answer: the vectors, in the order of the inputs, and
// what the provider said about the request.
type Result struct {
	Vectors []Vector
	Usage   Usage
	// RequestID is the provider's own identifier for the call, kept for a
	// diagnostic. It is not part of any identity or hash -- see the epic's list
	// of what must NOT change a generation.
	RequestID string
}

// Provider embeds text.
//
// Embed returns one vector per input, in the input order. A provider that
// cannot answer for every input returns an error rather than a short slice: a
// caller cannot tell which input a short answer skipped, and guessing produces
// a corpus whose rows are attributed to the wrong sources.
type Provider interface {
	// Profile describes the endpoint without revealing a credential.
	Profile() Profile
	// Embed asks for one vector per input.
	Embed(ctx context.Context, inputs []string) (Result, error)
}

// Profile is what an operator may be shown about a configured provider, and
// what a run records about it. It carries no secret.
type Profile struct {
	// Name is the operator's name for this provider configuration.
	Name string
	// Provider is the adapter class, for example "openai-compatible".
	Provider string
	// EndpointClass separates a local endpoint from a hosted one.
	EndpointClass string
	// EndpointHost is the host the requests go to, WITHOUT credentials, scheme
	// or path. It is here because "which host received our source rows" is a
	// question an operator is entitled to answer.
	EndpointHost string
	// Model is the model identifier as the provider names it.
	Model string
	// Revision is an immutable model revision where the provider exposes one.
	Revision string
	// Dimension is the output dimension the provider reports.
	Dimension int
	// MaxBatch is the largest number of inputs one request may carry, zero
	// when the adapter does not batch.
	MaxBatch int
	// MaxInputBytes is the largest single input the endpoint accepts, zero when
	// unknown.
	MaxInputBytes int
	// CredentialSource names WHERE the credential comes from, never what it is
	// -- "env:PTAH_EMBED_TOKEN" rather than the token.
	CredentialSource string
	// Retains reports whether Ptah keeps the raw input after the request.
	//
	// It answers only for PTAH. Whether the provider retains anything is
	// outside Ptah's knowledge and this must never claim otherwise.
	Retains bool
}

// Errors a caller distinguishes.
var (
	// ErrUnreachable is an endpoint that could not be reached at all.
	ErrUnreachable = errors.New("embedding endpoint unreachable")
	// ErrUnauthorized is a credential the endpoint refused.
	ErrUnauthorized = errors.New("embedding endpoint refused the credential")
	// ErrProvider is an error the provider reported.
	ErrProvider = errors.New("embedding provider reported an error")
	// ErrInvalidResponse is an answer that did not survive validation. It is
	// separate from ErrProvider because the provider believed it succeeded,
	// which is the case a caller most needs to see.
	ErrInvalidResponse = errors.New("embedding response failed validation")
)

// ValidateResult checks one answer against the request it answers.
//
// Every rule here exists because its absence produces a corpus that is wrong
// rather than a run that fails, and the two are not equally recoverable
// (stokaro/ptah#2068).
func ValidateResult(result Result, inputs, dimension int) error {
	if len(result.Vectors) != inputs {
		// A partial batch is not a partial success. The caller cannot tell
		// which input was skipped, so attributing the vectors it did get would
		// put some of them on the wrong rows.
		return fmt.Errorf("%w: %d inputs and %d vectors; a partial batch is not a complete one",
			ErrInvalidResponse, inputs, len(result.Vectors))
	}
	for index, vector := range result.Vectors {
		if err := validateVector(vector, index, dimension); err != nil {
			return err
		}
	}
	return nil
}

// validateVector checks one vector's shape and values.
func validateVector(vector Vector, index, dimension int) error {
	if dimension > 0 && len(vector) != dimension {
		return fmt.Errorf("%w: vector %d has %d dimensions and the generation expects %d",
			ErrInvalidResponse, index, len(vector), dimension)
	}
	if len(vector) == 0 {
		return fmt.Errorf("%w: vector %d is empty", ErrInvalidResponse, index)
	}
	for position, value := range vector {
		// NaN and the infinities are the values that make a distance
		// meaningless: an index built over them answers queries with an order
		// that has no relation to similarity, and nothing downstream can tell.
		if math.IsNaN(float64(value)) {
			return fmt.Errorf("%w: vector %d component %d is NaN", ErrInvalidResponse, index, position)
		}
		if math.IsInf(float64(value), 0) {
			return fmt.Errorf("%w: vector %d component %d is infinite", ErrInvalidResponse, index, position)
		}
	}
	return nil
}
