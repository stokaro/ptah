// Package awsparamstore is the runtimevar driver behind Atlas awsparamstore://
// URLs: a variable read from AWS Systems Manager Parameter Store with the
// GetParameter call.
//
// It stands in for gocloud.dev/runtimevar/awsparamstore and keeps its
// contract: the same URL parameters (decoder, wait, and every AWS parameter
// gocloud.dev/aws accepts), the same snapshot and watch semantics, the same
// NotFound mapping, and the same error text. The request goes out through the
// SDK's configuration loading, credential chain, retryer, SigV4 signer and
// middleware; what this package writes itself is the part the SDK generates
// per service: the GetParameter codec, the SSM endpoint rules and the modeled
// error list.
//
// The reason is the cost of linking the SDK's SSM client. Its schema package
// builds a schema for every shape of the service as package variables, about
// 1.4 MB that every ptah and ptah-compat process keeps from initialization to
// exit, whether or not a URL names this scheme. This package builds the few
// schemas GetParameter needs on first use.
//
// A snapshot's As accepts **GetParameterOutput, where the Go CDK driver
// exposes **ssm.GetParameterOutput. Errors expose what the SDK's do:
// *smithy.OperationError, the SDK's HTTP response error, smithy.APIError, and
// for a modeled error *ServiceError.
package awsparamstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
	gcaws "gocloud.dev/aws"
	"gocloud.dev/gcerrors"
	"gocloud.dev/runtimevar"
	"gocloud.dev/runtimevar/driver"
)

// Scheme is the URL scheme this driver serves.
const Scheme = "awsparamstore"

// URLOpener opens awsparamstore URLs such as "awsparamstore://myvar".
//
// The URL host and path name the parameter. The query accepts decoder (a
// runtimevar.DecoderByName name; URLOpener.Decoder, or bytes, when absent),
// wait (the poll interval, in time.ParseDuration form; 30 seconds when absent)
// and the AWS parameters gocloud.dev/aws.V2ConfigFromURLParams reads, such as
// region, profile, endpoint, fips and dualstack. Any other parameter is an
// error.
type URLOpener struct {
	// Decoder is the decoder used when the URL names none.
	Decoder *runtimevar.Decoder

	// Options are the defaults the URL's own parameters override.
	Options Options
}

// Options configures a variable.
type Options struct {
	// WaitDuration is the poll interval. Zero or less means 30 seconds.
	WaitDuration time.Duration
}

// OpenVariableURL implements runtimevar.VariableURLOpener.
func (o *URLOpener) OpenVariableURL(ctx context.Context, u *url.URL) (*runtimevar.Variable, error) {
	query := u.Query()
	decoderName := query.Get("decoder")
	query.Del("decoder")
	decoder, err := runtimevar.DecoderByName(ctx, decoderName, o.Decoder)
	if err != nil {
		return nil, fmt.Errorf("open variable %v: invalid decoder: %w", u, err)
	}
	options := o.Options
	if wait := query.Get("wait"); wait != "" {
		query.Del("wait")
		duration, err := time.ParseDuration(wait)
		if err != nil {
			return nil, fmt.Errorf("open variable %v: invalid wait %q: %w", u, wait, err)
		}
		options.WaitDuration = duration
	}
	cfg, err := gcaws.V2ConfigFromURLParams(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("open variable %v: %w", u, err)
	}
	return OpenVariable(cfg, path.Join(u.Host, u.Path), decoder, &options)
}

// OpenVariable returns a variable that polls the named parameter with cfg.
// The parameter's bytes reach decoder; a SecureString arrives decrypted. A nil
// opts means the defaults. The error is always nil; it is there for the shape
// of the Go CDK driver's constructor.
func OpenVariable(cfg aws.Config, name string, decoder *runtimevar.Decoder, opts *Options) (*runtimevar.Variable, error) {
	if opts == nil {
		opts = &Options{}
	}
	return runtimevar.New(&watcher{
		client:  newClient(cfg),
		name:    name,
		wait:    driver.WaitDuration(opts.WaitDuration),
		decoder: decoder,
	}), nil
}

// watcher implements driver.Watcher over GetParameter. A parameter version is
// the change signal: a poll that returns the version the last snapshot had
// reports no change, and an error equivalent to the last one reports nothing
// new either.
type watcher struct {
	client  *client
	name    string
	wait    time.Duration
	decoder *runtimevar.Decoder
}

// state implements driver.State.
type state struct {
	value      any
	output     *GetParameterOutput
	updateTime time.Time
	version    int64
	err        error
}

// Value implements driver.State.
func (s *state) Value() (any, error) {
	return s.value, s.err
}

// UpdateTime implements driver.State.
func (s *state) UpdateTime() time.Time {
	return s.updateTime
}

// As implements driver.State; it accepts **GetParameterOutput.
func (s *state) As(target any) bool {
	output, ok := target.(**GetParameterOutput)
	if ok {
		*output = s.output
	}
	return ok
}

// WatchVariable implements driver.Watcher.
func (w *watcher) WatchVariable(ctx context.Context, prev driver.State) (driver.State, time.Duration) {
	previous, _ := prev.(*state)
	output, err := w.client.getParameter(ctx, w.name)
	if err == nil && output.Parameter == nil {
		err = fmt.Errorf("unable to get %q parameter", w.name)
	}
	if err != nil {
		return errorState(err, previous), w.wait
	}
	parameter := output.Parameter
	if previous != nil && previous.version == parameter.Version {
		return nil, w.wait
	}
	value, err := w.decoder.Decode(ctx, []byte(aws.ToString(parameter.Value)))
	if err != nil {
		return errorState(err, previous), w.wait
	}
	return &state{
		value:      value,
		output:     output,
		updateTime: aws.ToTime(parameter.LastModifiedDate),
		version:    parameter.Version,
	}, w.wait
}

// errorState returns the state for err, or nil when the previous state
// already reported an equivalent error.
func errorState(err error, previous *state) driver.State {
	if apiErrorCode(err) == "CancelledError" {
		if strings.Contains(err.Error(), "context deadline exceeded") {
			err = context.DeadlineExceeded
		} else {
			err = context.Canceled
		}
	}
	if previous != nil && previous.err != nil && equivalentErrors(err, previous.err) {
		return nil
	}
	return &state{err: err}
}

// equivalentErrors reports whether two errors are the same failure: the same
// value, the same text, or the same service error code.
func equivalentErrors(a, b error) bool {
	if a.Error() == b.Error() {
		return true
	}
	code := apiErrorCode(a)
	return code != "" && code == apiErrorCode(b)
}

func apiErrorCode(err error) string {
	if apiError, ok := errors.AsType[smithy.APIError](err); ok {
		return apiError.ErrorCode()
	}
	return ""
}

// Close implements driver.Watcher.
func (w *watcher) Close() error {
	return nil
}

// ErrorAs implements driver.Watcher.
func (w *watcher) ErrorAs(err error, target any) bool {
	return errors.As(err, target)
}

// ErrorCode implements driver.Watcher: ParameterNotFound is NotFound, and
// every other failure is Unknown.
func (w *watcher) ErrorCode(err error) gcerrors.ErrorCode {
	if apiErrorCode(err) == "ParameterNotFound" {
		return gcerrors.NotFound
	}
	return gcerrors.Unknown
}
