package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlext"
	"ptah.run/engine"
)

// noSQL answers every claim with an empty value.
type noSQL struct{}

func (noSQL) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	return sqlext.Result{Values: make([]sqlext.Value, len(request.Claims))}, nil
}

func sqlProvider(id, target string, extension sqlext.Extension) engine.Provider {
	return engine.Provider{ID: id, Targets: []engine.Target{{Name: target}}, SQL: []sqlext.Extension{extension}}
}

func bucketExtension(owner string, targets ...string) sqlext.Extension {
	return sqlext.Extension{Owner: owner, Targets: targets, Parser: noSQL{},
		Patterns: []sqlext.Pattern{{Name: "bucket", Site: sqlext.TableElement, Words: []string{"BUCKET"}}}}
}

func TestRuntimeSQL_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, sqlProvider("example.org/custom", "custom", bucketExtension("example.org/custom", "custom")))

	set := runtime.SQL()

	c.Assert(set.Selected(), qt.IsTrue)
	c.Assert(set.Candidates("custom", sqlext.TableElement), qt.HasLen, 1)
	c.Assert(mustRuntime(c).SQL().Selected(), qt.IsTrue)
}

func TestRuntimeSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		providers []engine.Provider
		wantErr   string
	}{
		{name: "another owner", providers: []engine.Provider{
			sqlProvider("example.org/custom", "custom", bucketExtension("example.org/other", "custom"))},
			wantErr: `.*provider "example.org/custom" registers SQL syntax owned by "example.org/other"`},
		{name: "a target nobody registers", providers: []engine.Provider{
			sqlProvider("example.org/custom", "custom", bucketExtension("example.org/custom", "elsewhere"))},
			wantErr: `.*provider "example.org/custom" registers SQL syntax for target "elsewhere" it does not register`},
		{name: "another provider's target", providers: []engine.Provider{
			{ID: "example.org/first", Targets: []engine.Target{{Name: "first"}}},
			sqlProvider("example.org/custom", "custom", bucketExtension("example.org/custom", "first"))},
			wantErr: `.*provider "example.org/custom" registers SQL syntax for target "first" it does not register`},
		{name: "two owners of one syntax", providers: []engine.Provider{
			sqlProvider("example.org/first", "first", bucketExtension("example.org/first", "first")),
			sqlProvider("example.org/second", "second", bucketExtension("example.org/second", "second"))},
			wantErr: `.*can match the same table-element syntax`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(test.providers...)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(runtime, qt.IsNil)
		})
	}
}
