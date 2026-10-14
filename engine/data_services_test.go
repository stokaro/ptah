package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/dataext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine"
)

// quotedLiterals writes every value as a quoted string.
type quotedLiterals struct{}

func (quotedLiterals) Literal(string, any) (string, bool, error) { return "'x'", true, nil }

// noMoments holds no moment in any column.
type noMoments struct{}

func (noMoments) HoldsMoment(string) bool { return false }

// unchangedRows hands every row back as it came.
type unchangedRows struct{}

func (unchangedRows) TypeRows(
	_ capability.Capabilities, _ *catalog.Table, _ map[string]string, _, _ []map[string]any,
) (dataext.TypedRows, error) {
	return dataext.TypedRows{}, nil
}

func dataProvider(services dataext.Services) engine.Provider {
	return engine.Provider{ID: "example.org/data", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Data: services}}}
}

func TestDataServices_HappyPath(t *testing.T) {
	t.Run("the registered services under an alias", func(t *testing.T) {
		c := qt.New(t)
		services := dataext.Services{Literals: quotedLiterals{}, Columns: noMoments{}, Rows: unchangedRows{}}
		runtime := mustRuntime(c, dataProvider(services))

		got, err := runtime.DataServices("alternate")

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, services)
	})

	t.Run("a target with no services", func(t *testing.T) {
		c := qt.New(t)
		runtime := mustRuntime(c, dataProvider(dataext.Services{}))

		got, err := runtime.DataServices("custom")

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, dataext.Services{})
	})
}

func TestDataServices_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, dataProvider(dataext.Services{Rows: unchangedRows{}}))

	got, err := runtime.DataServices("unregistered")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(got, qt.Equals, dataext.Services{})
}

func TestNew_RefusesTypedNilDataServices(t *testing.T) {
	var literals *quotedLiterals
	var columns *noMoments
	var rows *unchangedRows
	tests := []struct {
		name     string
		services dataext.Services
		message  string
	}{
		{name: "literals", services: dataext.Services{Literals: literals}, message: `.*typed-nil data literal service`},
		{name: "columns", services: dataext.Services{Columns: columns}, message: `.*typed-nil data column service`},
		{name: "rows", services: dataext.Services{Rows: rows}, message: `.*typed-nil data row service`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(dataProvider(test.services))

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(runtime, qt.IsNil)
		})
	}
}
