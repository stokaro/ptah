package yamlext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

// scopedOwner reads the entries of rls_policies scoped to targets, and the
// unscoped ones where unscoped is set, recording each batch in read.
func scopedOwner(owner string, read *[][]yamlext.Entry, unscoped bool, targets ...string) yamlext.Extension {
	return yamlext.Extension{Owner: owner, Coverage: emptyClaim,
		TargetScopes: []yamlext.TargetScope{{Key: "rls_policies", Targets: targets, Unscoped: unscoped, Label: owner + " targets"}},
		Entries: func(entries []yamlext.Entry, _ yamlext.Tables) ([]yamlext.Contribution, error) {
			*read = append(*read, entries)
			return nil, nil
		},
	}
}

func TestSet_TargetOwner_RoutesByScope(t *testing.T) {
	var read [][]yamlext.Entry
	set := must.Must(yamlext.NewSet(scopedOwner("postgres", &read, true, "postgres", "cockroachdb"),
		scopedOwner("sqlserver", &read, false, "sqlserver")))
	tests := []struct {
		name      string
		targets   []string
		wantOwner string
		wantFound bool
	}{
		{name: "no target", wantOwner: "postgres", wantFound: true},
		{name: "one owner's targets", targets: []string{"cockroachdb"}, wantOwner: "postgres", wantFound: true},
		{name: "an alias", targets: []string{"mssql"}, wantOwner: "sqlserver", wantFound: true},
		{name: "a target no owner reads", targets: []string{"mysql"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owner, found, err := set.TargetOwner("rls_policies", test.targets)

			c.Assert(err, qt.IsNil)
			c.Assert(owner, qt.Equals, test.wantOwner)
			c.Assert(found, qt.Equals, test.wantFound)
		})
	}
}

func TestSet_TargetOwner_RefusesAMixedScope(t *testing.T) {
	c := qt.New(t)
	var read [][]yamlext.Entry
	set := must.Must(yamlext.NewSet(scopedOwner("postgres", &read, true, "postgres")))

	owner, found, err := set.TargetOwner("rls_policies", []string{"postgres", "mysql"})

	c.Assert(err, qt.ErrorMatches, `.*a declaration scoped to postgres,mysql mixes postgres targets with others;.*`+
		`declare one scoped to postgres and another scoped to mysql`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(owner, qt.Equals, "")
	c.Assert(found, qt.IsFalse)
}

// TestSet_ReadEntries_HandsEachOwnerItsEntriesInOrder pins that an owner gets
// every entry routed to it in one call, in the order the frontend wrote them,
// with the scope it was written with.
func TestSet_ReadEntries_HandsEachOwnerItsEntriesInOrder(t *testing.T) {
	c := qt.New(t)
	var postgres, sqlServer [][]yamlext.Entry
	set := must.Must(yamlext.NewSet(scopedOwner("postgres", &postgres, true, "postgres"),
		scopedOwner("sqlserver", &sqlServer, false, "sqlserver")))
	entries := []yamlext.Entry{
		{Key: "rls_policies", Origin: "rls_policies.b"},
		{Key: "rls_policies", Origin: "rls_policies.s", Targets: []string{"sqlserver"}},
		{Key: "rls_policies", Origin: "rls_policies.a", Targets: []string{"postgres"}},
	}

	contributions, err := set.ReadEntries(entries, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(contributions, qt.HasLen, 0)
	c.Assert(postgres, qt.DeepEquals, [][]yamlext.Entry{{entries[0], entries[2]}})
	c.Assert(sqlServer, qt.DeepEquals, [][]yamlext.Entry{{entries[1]}})
}

func TestSet_ReadEntries_FailurePath(t *testing.T) {
	var read [][]yamlext.Entry
	foreign := scopedOwner("postgres", &read, true, "postgres")
	foreign.Entries = func([]yamlext.Entry, yamlext.Tables) ([]yamlext.Contribution, error) {
		return []yamlext.Contribution{{Facet: &other{}}}, nil
	}
	tests := []struct {
		name    string
		owner   yamlext.Extension
		entry   yamlext.Entry
		wantErr string
	}{
		{name: "an entry no owner reads", owner: scopedOwner("postgres", &read, false, "postgres"),
			entry: yamlext.Entry{Key: "rls_policies", Origin: "rls_policies.a"}, wantErr: `rls_policies\.a: no selected owner reads it`},
		{name: "a mixed scope", owner: scopedOwner("postgres", &read, false, "postgres"),
			entry:   yamlext.Entry{Key: "rls_policies", Origin: "rls_policies.a", Targets: []string{"postgres", "mysql"}},
			wantErr: `rls_policies\.a: .*mixes postgres targets with others.*`},
		{name: "a facet of no table", owner: foreign, entry: yamlext.Entry{Key: "rls_policies", Origin: "rls_policies.a"},
			wantErr: `.*scoped entries contributed a facet of no table the document declares`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(yamlext.NewSet(test.owner))

			contributions, err := set.ReadEntries([]yamlext.Entry{test.entry}, nil)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(contributions, qt.IsNil)
		})
	}
}

// TestSet_Cover_HandsAnOwnerItsOwnObjects pins that an owner narrowing the
// claim sees the document's objects of its own models and no others.
func TestSet_Cover_HandsAnOwnerItsOwnObjects(t *testing.T) {
	c := qt.New(t)
	var seen schemaext.Objects
	extension := yamlext.Extension{Owner: "example.org/widget", Kinds: []schemaext.Kind{levelKind}, Coverage: emptyClaim,
		Cover: func(claim schemaext.Coverage, objects schemaext.Objects) (schemaext.Coverage, error) {
			seen = objects
			return levelCoverage()
		}}
	set := must.Must(yamlext.NewSet(extension))
	named := func(kind objectidentity.Kind) objectidentity.ID {
		return objectidentity.ID{Kind: kind, Name: objectidentity.Part{Source: "a", Normalized: "a"}}
	}
	objects := must.Must(schemaext.Objects{}.With(schemaext.Object{Ref: named(objectidentity.Kind(levelKind)), Value: &level{}}))
	objects = must.Must(objects.With(schemaext.Object{Ref: named("example.org/other"), Value: &other{}}))

	claim, err := set.Cover(schemaext.Coverage{}, objects)

	c.Assert(err, qt.IsNil)
	c.Assert(seen.Len(), qt.Equals, 1)
	c.Assert(claim.Lookup(levelKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

func TestNewSet_TargetScopes_FailurePath(t *testing.T) {
	var read [][]yamlext.Entry
	noReader := scopedOwner("postgres", &read, true, "postgres")
	noReader.Entries = nil
	tests := []struct {
		name       string
		extensions []yamlext.Extension
		wantErr    string
	}{
		{name: "no reader", extensions: []yamlext.Extension{noReader},
			wantErr: `YAML extension of postgres reads scoped entries and declares no reader`},
		{name: "no target", extensions: []yamlext.Extension{scopedOwner("postgres", &read, false)},
			wantErr: `YAML extension of postgres declares a scope without a key or a target`},
		{name: "a target two owners read", extensions: []yamlext.Extension{
			scopedOwner("postgres", &read, false, "postgres"), scopedOwner("pg", &read, false, "postgresql")},
			wantErr: `duplicate.*entries of YAML key "rls_policies" scoped to postgres are read by postgres and pg`},
		{name: "unscoped entries two owners read", extensions: []yamlext.Extension{
			scopedOwner("postgres", &read, true, "postgres"), scopedOwner("sqlserver", &read, true, "sqlserver")},
			wantErr: `duplicate.*unscoped entries of YAML key "rls_policies" are read by postgres and sqlserver`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := yamlext.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}
