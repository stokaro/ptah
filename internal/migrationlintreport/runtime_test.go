package migrationlintreport_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/migrationfile"
)

func selectedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return builtintest.Runtime()
}

func TestBuildRequiresRuntimeBeforeSourceAccess(t *testing.T) {
	c := qt.New(t)
	report, err := migrationlintreport.Build(t.Context(), migrationlintreport.Options{Dir: "missing"}, projectconfig.Config{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report.Findings, qt.HasLen, 0)
	c.Assert(report.Error, qt.Equals, "")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err = migrationlintreport.Build(ctx, migrationlintreport.Options{Runtime: selectedRuntime(c), Dir: "missing"}, projectconfig.Config{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report.Findings, qt.HasLen, 0)
	c.Assert(report.Error, qt.Equals, "")
}

type baselineContextKey struct{}

type baselineRuntime struct {
	*engine.Runtime
	contexts []context.Context
	requests []schemaext.ConversionRequest
	fail     error
	cancel   context.CancelFunc
}

func (r *baselineRuntime) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	r.contexts = append(r.contexts, ctx)
	r.requests = append(r.requests, request)
	if r.cancel != nil {
		r.cancel()
	}
	return nil, r.fail
}

func TestBuildReplayReadersUseSelectedRuntimeAndCleanAfterFailure(t *testing.T) {
	for _, test := range []struct {
		name    string
		capture bool
	}{
		{name: "baseline"}, {name: "schema capture", capture: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.WithValue(t.Context(), baselineContextKey{}, "selected baseline request")
			failure := errors.New("model service unavailable")
			runtime := &baselineRuntime{Runtime: selectedRuntime(c), fail: failure}
			assertFailedReplayRead(c, ctx, runtime, test.capture, failure)
		})
	}
}

func TestBuildReplayReadersCleanAfterCancellation(t *testing.T) {
	for _, test := range []struct {
		name    string
		capture bool
	}{
		{name: "baseline"}, {name: "schema capture", capture: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), baselineContextKey{}, "selected baseline request"))
			defer cancel()
			runtime := &baselineRuntime{Runtime: selectedRuntime(c), cancel: cancel}
			assertFailedReplayRead(c, ctx, runtime, test.capture, context.Canceled)
		})
	}
}

func assertFailedReplayRead(c *qt.C, ctx context.Context, runtime *baselineRuntime, capture bool, want error) {
	c.Helper()
	devURL := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "dev.db"))
	report, err := migrationlintreport.Build(ctx, migrationlintreport.Options{
		Runtime: runtime, CaptureSchema: capture, Dir: writeIndexMigrations(c), DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect: "sqlite", DevURL: devURL, FailOn: migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{Dialect: true, DevURL: true},
	}, projectconfig.Config{})
	c.Assert(err, qt.ErrorIs, want)
	c.Assert(report.Error, qt.Contains, want.Error())
	c.Assert(report.SchemaCurrent, qt.Equals, "")
	c.Assert(report.SchemaDesired, qt.Equals, "")
	c.Assert(runtime.requests, qt.HasLen, 1)
	c.Assert(runtime.contexts[0].Value(baselineContextKey{}), qt.Equals, "selected baseline request")
	c.Assert(runtime.requests[0].Target, qt.Equals, "sqlite")
	c.Assert(runtime.requests[0].From, qt.Equals, schemaext.Observed)
	c.Assert(runtime.requests[0].To, qt.Equals, schemaext.Desired)
	conn, err := dbschema.ConnectToDatabase(c.Context(), devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 0)
}
