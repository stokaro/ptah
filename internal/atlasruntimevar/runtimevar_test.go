package atlasruntimevar_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"gocloud.dev/gcerrors"

	"ptah.run/internal/atlasruntimevar"
)

// parameterStore answers GetParameter with status and body, and records the
// target of the last request.
func parameterStore(c *qt.C, status int, body string) (endpoint string, target *atomic.Value) {
	target = &atomic.Value{}
	target.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		target.Store(r.Header.Get("X-Amz-Target"))
		w.Header().Set("X-Amzn-Requestid", "request-1")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	c.Cleanup(server.Close)
	return server.URL, target
}

// isolateAWSEnvironment leaves the default credential chain only static test
// credentials to find.
func isolateAWSEnvironment(c *qt.C) {
	for _, entry := range os.Environ() {
		if name, _, _ := strings.Cut(entry, "="); strings.HasPrefix(name, "AWS_") {
			c.Unsetenv(name)
		}
	}
	dir := c.TempDir()
	c.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	c.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	c.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	c.Setenv("AWS_ACCESS_KEY_ID", "AKIDPTAHTEST")
	c.Setenv("AWS_SECRET_ACCESS_KEY", "ptah-test-secret")
}

func parameterURL(endpoint string) string {
	return "awsparamstore://ptah/db?region=us-east-1&decoder=string&endpoint=" + url.QueryEscape(endpoint)
}

// TestOpenParameterStore_HappyPath reads an awsparamstore:// URL through Open,
// the entry point atlas.hcl reaches, so the scheme's registration is what the
// test depends on.
func TestOpenParameterStore_HappyPath(t *testing.T) {
	c := qt.New(t)
	isolateAWSEnvironment(c)
	endpoint, target := parameterStore(c, http.StatusOK,
		`{"Parameter":{"Name":"ptah/db","Type":"SecureString","Value":"postgres://ptah@db/app","Version":4,"LastModifiedDate":1.7E9}}`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	variable, err := atlasruntimevar.Open(ctx, parameterURL(endpoint))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(variable.Close(), qt.IsNil) }()
	snapshot, err := variable.Latest(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Value, qt.Equals, "postgres://ptah@db/app")
	c.Assert(snapshot.UpdateTime.Equal(time.Unix(1_700_000_000, 0)), qt.IsTrue)
	c.Assert(target.Load(), qt.Equals, "AmazonSSM.GetParameter")
}

// TestOpenParameterStore_FailurePath pins the text a missing parameter
// produces, end to end: the Go CDK code, the SDK operation and response
// wrapping, and the modeled error.
func TestOpenParameterStore_FailurePath(t *testing.T) {
	c := qt.New(t)
	isolateAWSEnvironment(c)
	endpoint, _ := parameterStore(c, http.StatusBadRequest,
		`{"__type":"ParameterNotFound","message":"Parameter ptah/db not found."}`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	variable, err := atlasruntimevar.Open(ctx, parameterURL(endpoint))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(variable.Close(), qt.IsNil) }()
	_, err = variable.Watch(ctx)

	c.Assert(err, qt.ErrorMatches, `runtimevar \(code=NotFound\): operation error SSM: GetParameter, https response error StatusCode: 400, RequestID: request-1, ParameterNotFound: Parameter ptah/db not found\.`)
	c.Assert(gcerrors.Code(err), qt.Equals, gcerrors.NotFound)
}
