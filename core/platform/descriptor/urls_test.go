package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// withURLs returns head(name) reading its URLs as urls.
func withURLs(name string, urls descriptor.URLs) descriptor.Dialect {
	described := head(name)
	described.URLs = urls
	return described
}

func TestNewRegistry_URLs_HappyPath(t *testing.T) {
	c := qt.New(t)
	alpha := withURLs("alpha", descriptor.URLs{
		DefaultPort: 7000, Grammar: descriptor.URLGrammarGoSQLDriver, FileSchemes: []string{"alpha", "file"},
	})
	beta := withURLs("beta", descriptor.URLs{ComparedAs: "alpha", Grammar: descriptor.URLGrammarGoSQLDriver})
	gamma := withURLs("gamma", descriptor.URLs{ComparedAs: "gamma"})

	registry, err := descriptor.NewRegistry(alpha, beta, gamma)

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("beta")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.URLs, qt.DeepEquals, beta.URLs)
	got, found = registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.URLs, qt.DeepEquals, alpha.URLs)
}

func TestNewRegistry_URLs_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialects []descriptor.Dialect
		wantIs   error
		wantErr  string
	}{
		{
			name:     "an undeclared grammar",
			dialects: []descriptor.Dialect{withURLs("alpha", descriptor.URLs{Grammar: "jdbc"})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha": URL grammar "jdbc" is not one of .*`,
		},
		{
			name:     "a compared-as dialect spelled in upper case",
			dialects: []descriptor.Dialect{withURLs("alpha", descriptor.URLs{ComparedAs: "Beta"})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha": compared as "Beta", which is not spelled as a dialect name`,
		},
		{
			name:     "a compared-as dialect the registry does not hold",
			dialects: []descriptor.Dialect{withURLs("alpha", descriptor.URLs{ComparedAs: "beta"})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha" is compared as "beta", which is no dialect's name`,
		},
		{
			name:     "a compared-as dialect named by an alias",
			dialects: []descriptor.Dialect{head("beta", "b"), withURLs("alpha", descriptor.URLs{ComparedAs: "b"})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha" is compared as "b", which is no dialect's name`,
		},
		{
			name: "a compared-as dialect compared as another",
			dialects: []descriptor.Dialect{
				head("gamma"), withURLs("beta", descriptor.URLs{ComparedAs: "gamma"}), withURLs("alpha", descriptor.URLs{ComparedAs: "beta"}),
			},
			wantIs:  descriptor.ErrInvalidDescriptor,
			wantErr: `invalid dialect descriptor: dialect "alpha" is compared as "beta", which is itself compared as "gamma"`,
		},
		{
			name:     "a file scheme with a colon",
			dialects: []descriptor.Dialect{withURLs("alpha", descriptor.URLs{FileSchemes: []string{"file:"}})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha": file scheme "file:" must be .*`,
		},
		{
			name:     "a file scheme stated twice",
			dialects: []descriptor.Dialect{withURLs("alpha", descriptor.URLs{FileSchemes: []string{"file", "file"}})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha" states file scheme "file" twice`,
		},
		{
			name: "a file scheme two dialects claim",
			dialects: []descriptor.Dialect{
				withURLs("alpha", descriptor.URLs{FileSchemes: []string{"file"}}),
				withURLs("beta", descriptor.URLs{FileSchemes: []string{"file"}}),
			},
			wantIs:  descriptor.ErrConflict,
			wantErr: `conflicting dialect descriptor: dialect "beta" claims file scheme "file", which dialect "alpha" already claims`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialects...)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// TestRegistry_With_RefusesAClaimedFileScheme pins that a registration cannot
// take a file scheme from a dialect the registry already holds, so a scheme
// keeps the dialect it selects.
func TestRegistry_With_RefusesAClaimedFileScheme(t *testing.T) {
	c := qt.New(t)
	base, err := descriptor.NewRegistry(withURLs("alpha", descriptor.URLs{FileSchemes: []string{"file"}}))
	c.Assert(err, qt.IsNil)

	extended, err := base.With(withURLs("beta", descriptor.URLs{FileSchemes: []string{"file"}}))

	c.Assert(err, qt.ErrorIs, descriptor.ErrConflict)
	c.Assert(extended, qt.IsNil)
}
