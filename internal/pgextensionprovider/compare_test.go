package pgextensionprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/pgextensionprovider"
)

func newRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	var targets []engine.Target
	for _, name := range pgextensionprovider.Targets() {
		targets = append(targets, engine.Target{Name: name})
	}
	return must.Must(engine.New(engine.Provider{ID: "example.org/targets", Targets: targets}, pgextensionprovider.Provider()))
}

func declared(name string, extension pgextension.DesiredExtension) schemaext.Object {
	return must.Must(pgextension.DesiredObject(pgextension.Ref(name), extension))
}

func installed(name string, extension pgextension.ObservedExtension) schemaext.Object {
	return must.Must(pgextension.ObservedObject(pgextension.Ref(name), extension))
}

// trgm is pg_trgm 1.5 in public, which may move.
func trgm() pgextension.ObservedExtension {
	return pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true}
}

// comparison compares desired with current on PostgreSQL, both sources
// describing every extension, on a target that stores an extension's comment.
func comparison(desired, current []schemaext.Object) schemaext.ObjectComparisonRequest {
	return schemaext.ObjectComparisonRequest{
		Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres17(),
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(desired...)), Coverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(current...)), Coverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed))},
	}
}

func excluding(names ...string) []schemaext.Exclusion {
	exclusions := make([]schemaext.Exclusion, 0, len(names))
	for _, name := range names {
		exclusions = append(exclusions, schemaext.Exclusion{Kind: coverage.Extension, Name: name})
	}
	return exclusions
}

func column(name, columnType string) schemaext.DeclaredColumn {
	return schemaext.DeclaredColumn{Table: objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "documents"), Name: name, Type: columnType}
}

// TestCompareExtensions_LeavesWhatAgrees pins the declarations and reads that
// plan nothing: an extension that agrees once the server's defaults are
// resolved, one the run excludes, one a declared column's type needs, one the
// server installs everywhere, and one the desired source did not describe.
func TestCompareExtensions_LeavesWhatAgrees(t *testing.T) {
	commented := trgm()
	commented.Comment = new("text similarity")
	tests := []struct {
		name    string
		request func() schemaext.ObjectComparisonRequest
	}{
		{name: "a declaration naming nothing", request: func() schemaext.ObjectComparisonRequest {
			return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{})}, []schemaext.Object{installed("pg_trgm", trgm())})
		}},
		{name: "the default schema and the installed version written out", request: func() schemaext.ObjectComparisonRequest {
			return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Schema: "public", Version: " 1.5 "})},
				[]schemaext.Object{installed("pg_trgm", trgm())})
		}},
		{name: "the default schema named where a document compared as the current side names none", request: func() schemaext.ObjectComparisonRequest {
			return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Schema: "public"})},
				[]schemaext.Object{installed("pg_trgm", pgextension.ObservedExtension{Version: "1.5"})})
		}},
		{name: "a held comment with surrounding space", request: func() schemaext.ObjectComparisonRequest {
			spaced := trgm()
			spaced.Comment = new(" text similarity\n")
			return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Comment: "text similarity"})},
				[]schemaext.Object{installed("pg_trgm", spaced)})
		}},
		{name: "the comment the extension holds", request: func() schemaext.ObjectComparisonRequest {
			return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Comment: " text similarity "})},
				[]schemaext.Object{installed("pg_trgm", commented)})
		}},
		{name: "a comment where the target stores none", request: func() schemaext.ObjectComparisonRequest {
			request := comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Comment: "trigrams"})},
				[]schemaext.Object{installed("pg_trgm", trgm())})
			request.Capabilities = capability.Postgres17().With(capability.ExtensionComments, false)
			return request
		}},
		{name: "an excluded extension installed and not declared", request: func() schemaext.ObjectComparisonRequest {
			request := comparison(nil, []schemaext.Object{installed("plpgsql", pgextension.ObservedExtension{Schema: "pg_catalog", Version: "1.0"})})
			request.Exclusions = excluding("plpgsql")
			return request
		}},
		{name: "an excluded extension declared and not installed", request: func() schemaext.ObjectComparisonRequest {
			request := comparison([]schemaext.Object{declared("adminpack", pgextension.DesiredExtension{})}, nil)
			request.Exclusions = excluding("adminpack")
			return request
		}},
		{name: "an excluded extension declared elsewhere than it is installed", request: func() schemaext.ObjectComparisonRequest {
			request := comparison([]schemaext.Object{declared("plpgsql", pgextension.DesiredExtension{Schema: "ext"})},
				[]schemaext.Object{installed("plpgsql", pgextension.ObservedExtension{Schema: "pg_catalog", Version: "1.0"})})
			request.Exclusions = excluding("plpgsql")
			return request
		}},
		{name: "an extension a declared column's type needs", request: func() schemaext.ObjectComparisonRequest {
			request := comparison(nil, []schemaext.Object{installed("vector", pgextension.ObservedExtension{Schema: "public", Version: "0.8.0"})})
			request.DeclaredColumns = []schemaext.DeclaredColumn{column("embedding", "VECTOR(384)[]")}
			return request
		}},
		{name: "an extension YugabyteDB installs everywhere", request: func() schemaext.ObjectComparisonRequest {
			request := comparison(nil, []schemaext.Object{installed("pg_stat_statements", pgextension.ObservedExtension{Schema: "pg_catalog", Version: "1.10"})})
			request.Target = "yugabytedb"
			return request
		}},
		{name: "an extension the desired source did not describe", request: func() schemaext.ObjectComparisonRequest {
			request := comparison(nil, []schemaext.Object{installed("pg_trgm", trgm())})
			request.Desired.Coverage = must.Must(pgextension.Coverage(schemaext.Desired,
				schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the document left extensions out"}, nil))
			return request
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(), test.request())

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareExtensions_FindsChanges pins each difference a comparison plans,
// and the controls that show the rules above are narrow: a server's
// everywhere-extension is its own only on that server, and a column's type
// needs only the extension that supplies it.
func TestCompareExtensions_FindsChanges(t *testing.T) {
	tests := []struct {
		name    string
		request func() schemaext.ObjectComparisonRequest
		want    *pgextension.Change
	}{
		{name: "an installation", want: &pgextension.Change{After: &pgextension.DesiredExtension{Version: "1.6"}},
			request: func() schemaext.ObjectComparisonRequest {
				return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Version: "1.6"})}, nil)
			}},
		{name: "a drop", want: &pgextension.Change{Before: new(trgm())},
			request: func() schemaext.ObjectComparisonRequest {
				return comparison(nil, []schemaext.Object{installed("pg_trgm", trgm())})
			}},
		{name: "a raised version", want: &pgextension.Change{Before: new(trgm()), After: &pgextension.DesiredExtension{Version: "1.6"}},
			request: func() schemaext.ObjectComparisonRequest {
				return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Version: "1.6"})}, []schemaext.Object{installed("pg_trgm", trgm())})
			}},
		{name: "a move", want: &pgextension.Change{Before: new(trgm()), After: &pgextension.DesiredExtension{Schema: "ext"}, Schema: "ext"},
			request: func() schemaext.ObjectComparisonRequest {
				return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Schema: "ext"})}, []schemaext.Object{installed("pg_trgm", trgm())})
			}},
		{name: "a comment", want: &pgextension.Change{Before: new(trgm()), After: &pgextension.DesiredExtension{Comment: "trigrams"}, Comment: true},
			request: func() schemaext.ObjectComparisonRequest {
				return comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Comment: "trigrams"})}, []schemaext.Object{installed("pg_trgm", trgm())})
			}},
		{name: "control: YugabyteDB's everywhere-extension on PostgreSQL", want: &pgextension.Change{Before: &pgextension.ObservedExtension{Schema: "public", Version: "1.10"}},
			request: func() schemaext.ObjectComparisonRequest {
				return comparison(nil, []schemaext.Object{installed("pg_stat_statements", pgextension.ObservedExtension{Schema: "public", Version: "1.10"})})
			}},
		{name: "control: a column type another extension supplies", want: &pgextension.Change{Before: new(trgm())},
			request: func() schemaext.ObjectComparisonRequest {
				request := comparison(nil, []schemaext.Object{installed("pg_trgm", trgm())})
				request.DeclaredColumns = []schemaext.DeclaredColumn{column("embedding", "vector(3)"), column("tag", "vector_id")}
				return request
			}},
		{name: "control: an exclusion spelled otherwise", want: &pgextension.Change{Before: new(trgm())},
			request: func() schemaext.ObjectComparisonRequest {
				request := comparison(nil, []schemaext.Object{installed("pg_trgm", trgm())})
				request.Exclusions = excluding("PG_TRGM", " pg_trgm", "")
				return request
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(), test.request())

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Value, qt.DeepEquals, test.want)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareExtensions_WaitsForARead pins that a declaration is not
// installed where the read could not say whether the extension is there: an
// extension installed in another schema would make CREATE EXTENSION IF NOT
// EXISTS a no-op that leaves the declared placement unapplied.
func TestCompareExtensions_WaitsForARead(t *testing.T) {
	c := qt.New(t)
	request := comparison([]schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Schema: "ext"})}, nil)
	request.Current.Coverage = must.Must(pgextension.Coverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "this target cannot report extensions"}, nil))

	result, err := newRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: pgextension.ExtensionKind, Subject: pgextension.Ref("pg_trgm"),
		Reason: "whether the extension is installed was not established: this target cannot report extensions"}})
}
