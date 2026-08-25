package aiprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Defaults for a provider that states nothing.
const (
	// DefaultTimeout bounds one request. It is generous because a local model
	// loading weights on first use takes tens of seconds -- measured at 9.5s
	// for a three-token answer from a cold 27B on an Apple GPU -- and a limit
	// tuned to a hosted API would report that as a broken endpoint.
	DefaultTimeout = 5 * time.Minute
	// DefaultMaxRetries is how many times a retryable failure is retried.
	DefaultMaxRetries = 2
	// maxErrorBodyBytes bounds what is read from a failed response, so a
	// provider answering with a web page does not become a diagnostic nobody
	// can read.
	maxErrorBodyBytes = 4 << 10
)

// Config is one configured endpoint, with its credential already resolved.
//
// The credential arrives resolved rather than as a reference because resolution
// is the configuration layer's job and knowing how to read a keyring is not an
// HTTP client's. It is held in memory for the life of the provider and written
// nowhere.
type Config struct {
	// Profile is the operator's name for this configuration.
	Profile string
	// BaseURL is the endpoint root, with or without a trailing slash. For an
	// OpenAI-compatible provider it usually ends in /v1.
	BaseURL string
	// Model is the model identifier to send.
	Model string
	// APIKey is the resolved credential, empty for an endpoint that needs none
	// -- which a local model server usually does.
	APIKey string
	// Headers are extra headers the operator configured, for a gateway that
	// wants one.
	Headers map[string]string
	// Query are extra query parameters, which is how Azure OpenAI carries its
	// api-version.
	Query map[string]string
	// Timeout bounds one request.
	Timeout time.Duration
	// MaxRetries bounds retries of a retryable failure.
	MaxRetries int
	// HTTPClient is the client to use. A nil value builds one from Timeout.
	HTTPClient *http.Client
	// Sleep is how a retry waits, injectable so a test does not.
	Sleep func(context.Context, time.Duration) error
}

// withDefaults fills in what the operator left out.
func (c Config) withDefaults() Config {
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: c.Timeout}
	}
	if c.Sleep == nil {
		c.Sleep = sleepContext
	}
	return c
}

// sleepContext waits, or gives up when the caller does.
func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// endpoint joins the base URL with a path and the configured query parameters.
//
// The join tolerates a base with or without a trailing slash and a path with or
// without a leading one, because operators paste both and an endpoint that
// silently becomes `…/v1//chat/completions` is a 404 nobody can read.
func (c Config) endpoint(path string) (string, error) {
	base, err := url.Parse(strings.TrimSuffix(c.BaseURL, "/"))
	if err != nil {
		return "", fmt.Errorf("invalid base URL %q: %w", c.BaseURL, err)
	}
	if base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid base URL %q: want a scheme and a host", c.BaseURL)
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + "/" + strings.TrimPrefix(path, "/")
	if len(c.Query) > 0 {
		query := base.Query()
		for name, value := range c.Query {
			query.Set(name, value)
		}
		base.RawQuery = query.Encode()
	}
	return base.String(), nil
}

// call sends one JSON request and decodes the answer, retrying what is worth
// retrying.
//
// The retry loop lives here rather than in each adapter so both providers agree
// about what a 429 means, and so the one rule that matters is stated once: only
// a failure that produced no answer is retried. A provider that answered has
// already done the work, and repeating the request would be a second turn the
// caller did not ask for.
func (c Config) call(
	ctx context.Context,
	method, path string,
	body any,
	auth func(*http.Request),
	out any,
) error {
	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := c.Sleep(ctx, backoff(attempt, lastErr)); err != nil {
				return c.wrap(KindTimeout, 0, "waiting to retry: "+err.Error(), err)
			}
		}
		err := c.attempt(ctx, method, path, body, auth, out)
		if err == nil {
			return nil
		}
		lastErr = err
		providerErr, is := errors.AsType[*Error](err)
		if !is || !providerErr.Retryable() {
			return err
		}
	}
	return lastErr
}

// backoff is how long to wait before one retry.
//
// The provider's own Retry-After wins when it sent one, because a server saying
// how long to wait knows better than a formula. Otherwise it is exponential
// with jitter, so two Ptah sessions that started together do not retry
// together.
func backoff(attempt int, lastErr error) time.Duration {
	if providerErr, is := errors.AsType[*Error](lastErr); is && providerErr.RetryAfter > 0 {
		return providerErr.RetryAfter
	}
	base := time.Duration(1<<attempt) * 500 * time.Millisecond
	// #nosec G404 -- jitter spreads retries between processes; it guards nothing
	// and an unpredictable one would cost a syscall per retry for no property.
	return base + time.Duration(rand.Int64N(int64(base/2)))
}

// attempt performs one round trip.
func (c Config) attempt(
	ctx context.Context,
	method, path string,
	body any,
	auth func(*http.Request),
	out any,
) error {
	endpoint, err := c.endpoint(path)
	if err != nil {
		return c.wrap(KindProvider, 0, err.Error(), err)
	}

	var payload io.Reader
	if body != nil {
		encoded, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return c.wrap(KindProvider, 0, "encode request: "+marshalErr.Error(), marshalErr)
		}
		payload = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, endpoint, payload)
	if err != nil {
		return c.wrap(KindProvider, 0, err.Error(), err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range c.Headers {
		request.Header.Set(name, value)
	}
	if auth != nil {
		auth(request)
	}

	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return c.transportError(err)
	}
	defer response.Body.Close() //nolint:errcheck // the body is drained or abandoned; a close error adds nothing

	if response.StatusCode >= http.StatusBadRequest {
		return c.statusError(response)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		return c.wrap(KindMalformedResponse, response.StatusCode,
			"the endpoint answered with something this client could not read: "+err.Error(), err)
	}
	return nil
}

// streamCall is [Config.call] for a response consumed as it arrives.
//
// It retries the way call does, with one difference that is the whole reason it
// is a separate function: a retry is only safe until the first fragment reaches
// the caller. Once text has been shown, replaying the request would show it
// twice, and a duplicated half-answer is worse than the failure it came from.
// consume reports whether it delivered anything, and that decides.
func (c Config) streamCall(
	ctx context.Context,
	method, path string,
	body any,
	auth func(*http.Request),
	consume func(io.Reader) (delivered bool, err error),
) error {
	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := c.Sleep(ctx, backoff(attempt, lastErr)); err != nil {
				return c.wrap(KindTimeout, 0, "waiting to retry: "+err.Error(), err)
			}
		}
		delivered, err := c.streamAttempt(ctx, method, path, body, auth, consume)
		if err == nil {
			return nil
		}
		if delivered {
			// Past the point of no return. The caller has seen part of an
			// answer, so this failure is reported rather than retried.
			return err
		}
		lastErr = err
		providerErr, is := errors.AsType[*Error](err)
		if !is || !providerErr.Retryable() {
			return err
		}
	}
	return lastErr
}

// streamAttempt makes one streamed request, reporting whether the consumer got
// anything before it failed.
func (c Config) streamAttempt(
	ctx context.Context,
	method, path string,
	body any,
	auth func(*http.Request),
	consume func(io.Reader) (bool, error),
) (bool, error) {
	endpoint, err := c.endpoint(path)
	if err != nil {
		return false, c.wrap(KindProvider, 0, err.Error(), err)
	}

	var payload io.Reader
	if body != nil {
		encoded, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return false, c.wrap(KindProvider, 0, "encode request: "+marshalErr.Error(), marshalErr)
		}
		payload = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, endpoint, payload)
	if err != nil {
		return false, c.wrap(KindProvider, 0, err.Error(), err)
	}
	// A streamed answer is server-sent events, and the endpoint is told so
	// rather than left to infer it from the request body.
	request.Header.Set("Accept", "text/event-stream")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range c.Headers {
		request.Header.Set(name, value)
	}
	if auth != nil {
		auth(request)
	}

	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return false, c.transportError(err)
	}
	defer response.Body.Close() //nolint:errcheck // the body is drained or abandoned; a close error adds nothing

	if response.StatusCode >= http.StatusBadRequest {
		// The error body is JSON even on a request that asked for a stream, so
		// this is the same classification an ordinary call gets.
		return false, c.statusError(response)
	}

	delivered, err := consume(response.Body)
	if err != nil {
		return delivered, c.wrap(KindMalformedResponse, response.StatusCode,
			"the endpoint's stream could not be read: "+err.Error(), err)
	}
	return delivered, nil
}

// transportError classifies a failure that produced no HTTP answer.
func (c Config) transportError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
		return c.wrap(KindTimeout, 0, "the endpoint did not answer in time: "+err.Error(), err)
	}
	if errors.Is(err, context.Canceled) {
		return c.wrap(KindTimeout, 0, "canceled: "+err.Error(), err)
	}
	return c.wrap(KindUnreachable, 0, "the endpoint could not be reached: "+err.Error(), err)
}

// isTimeout reports a net.Error timeout without naming the package's concrete
// types, which vary by transport.
func isTimeout(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// statusError classifies an HTTP failure and keeps the provider's own words.
func (c Config) statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
	message := providerMessage(body)
	retryAfter := retryAfter(response.Header.Get("Retry-After"))

	kind := KindProvider
	switch {
	case response.StatusCode == http.StatusUnauthorized,
		response.StatusCode == http.StatusForbidden:
		kind = KindAuth
	case response.StatusCode == http.StatusNotFound:
		// A 404 from a chat endpoint is usually the model, and sometimes the
		// base URL. The message says both rather than guessing, because the two
		// remedies are different and the provider's own text distinguishes them
		// more often than the status does.
		kind = KindUnknownModel
	case response.StatusCode == http.StatusTooManyRequests:
		kind = KindRateLimited
	case response.StatusCode == http.StatusRequestEntityTooLarge:
		kind = KindTooLarge
	case response.StatusCode == http.StatusRequestTimeout,
		response.StatusCode == http.StatusGatewayTimeout:
		kind = KindTimeout
	case response.StatusCode >= http.StatusInternalServerError:
		kind = KindUnavailable
	case response.StatusCode == http.StatusBadRequest:
		kind = classifyBadRequest(message)
	}
	return &Error{
		Kind:       kind,
		Profile:    c.Profile,
		Model:      c.Model,
		Status:     response.StatusCode,
		Message:    message,
		RetryAfter: retryAfter,
	}
}

// classifyBadRequest reads the provider's own words for the two 400s that mean
// something a caller can act on.
//
// Substring matching, and stated as such: there is no standard code for either
// of these, the wording differs per provider, and a wrong guess here costs a
// less specific message rather than a wrong action.
func classifyBadRequest(message string) ErrorKind {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "context length"),
		strings.Contains(lower, "context window"),
		strings.Contains(lower, "too many tokens"),
		strings.Contains(lower, "maximum context"):
		return KindTooLarge
	case strings.Contains(lower, "tool"), strings.Contains(lower, "function call"):
		return KindToolsUnsupported
	case strings.Contains(lower, "model"):
		return KindUnknownModel
	}
	return KindProvider
}

// providerMessage extracts the human part of an error body.
//
// Three shapes cover what endpoints actually send: OpenAI's nested error
// object, a flat message, and plain text from a proxy that never got to the
// provider. The raw body is the fallback rather than a generic sentence,
// because the raw body is the only thing that says what happened.
func providerMessage(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "the endpoint answered with an error and no message"
	}
	var envelope struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    any    `json:"code"`
		} `json:"error"`
		Message string `json:"message"`
		Detail  string `json:"detail"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		for _, candidate := range []string{envelope.Error.Message, envelope.Message, envelope.Detail} {
			if candidate != "" {
				return candidate
			}
		}
	}
	return trimmed
}

// retryAfter reads the header in both the forms the specification allows.
func retryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if stamp, err := http.ParseTime(value); err == nil {
		if delay := time.Until(stamp); delay > 0 {
			return delay
		}
	}
	return 0
}

// wrap builds a provider error for this configuration.
func (c Config) wrap(kind ErrorKind, status int, message string, err error) error {
	return &Error{
		Kind:    kind,
		Profile: c.Profile,
		Model:   c.Model,
		Status:  status,
		Message: message,
		Err:     err,
	}
}
