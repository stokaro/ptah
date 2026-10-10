package yamlext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

// other is a model no test owner declares.
type other struct{}

func (*other) Kind() schemaext.Kind   { return "example.org/other" }
func (*other) Clone() schemaext.Value { return &other{} }
func (*other) Equal(value schemaext.Value) bool {
	_, ok := value.(*other)
	return ok
}

func emptyClaim() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }

func noDecode(func(any) error, yamlext.Tables) ([]yamlext.Contribution, error) { return nil, nil }

// sectionOwner reads the YAML key levels with decode.
func sectionOwner(owner, key string, decode func(func(any) error, yamlext.Tables) ([]yamlext.Contribution, error)) yamlext.Extension {
	return yamlext.Extension{Owner: owner, Kinds: []schemaext.Kind{levelKind}, Coverage: emptyClaim,
		Sections: []yamlext.Section{{Key: key, Decode: decode}}}
}

// returning decodes to the contributions it is given.
func returning(contributions ...yamlext.Contribution) func(func(any) error, yamlext.Tables) ([]yamlext.Contribution, error) {
	return func(func(any) error, yamlext.Tables) ([]yamlext.Contribution, error) { return contributions, nil }
}

func TestSet_Section_HappyPath(t *testing.T) {
	c := qt.New(t)
	set, err := yamlext.NewSet(sectionOwner("example.org/widget", "levels", noDecode),
		yamlext.Extension{Owner: "example.org/gauge", Coverage: emptyClaim,
			Sections: []yamlext.Section{{Key: "gauges", Decode: noDecode}}})
	c.Assert(err, qt.IsNil)

	owner, found := set.Section("levels")
	_, unread := set.Section("tables")

	c.Assert(found, qt.IsTrue)
	c.Assert(owner, qt.Equals, "example.org/widget")
	c.Assert(unread, qt.IsFalse)
	c.Assert(set.SectionKeys(), qt.DeepEquals, []string{"gauges", "levels"})
}

// TestSet_Decode_HandsTheOwnerItsSection pins what the owner is handed: the
// decoder the frontend built, and the document's tables.
func TestSet_Decode_HandsTheOwnerItsSection(t *testing.T) {
	c := qt.New(t)
	var seenTables yamlext.Tables
	set, err := yamlext.NewSet(sectionOwner("example.org/widget", "levels",
		func(decode func(any) error, tables yamlext.Tables) ([]yamlext.Contribution, error) {
			seenTables = tables
			var written map[string]string
			if err := decode(&written); err != nil {
				return nil, err
			}
			return []yamlext.Contribution{{Facet: &level{Value: written["docs"]}, Table: 0, Label: "a level"}}, nil
		}))
	c.Assert(err, qt.IsNil)
	tables := yamlext.Tables{{Key: "docs", Name: "docs"}}

	contributions, err := set.Decode("levels", func(target any) error {
		*target.(*map[string]string) = map[string]string{"docs": "high"}
		return nil
	}, tables)

	c.Assert(err, qt.IsNil)
	c.Assert(seenTables, qt.DeepEquals, tables)
	c.Assert(contributions, qt.DeepEquals, []yamlext.Contribution{{Facet: &level{Value: "high"}, Label: "a level"}})
}

func TestSet_Decode_FailurePath(t *testing.T) {
	object := schemaext.Object{Value: &level{}}
	tests := []struct {
		name    string
		owner   yamlext.Extension
		key     string
		wantErr string
	}{
		{name: "a key no owner reads", owner: sectionOwner("example.org/widget", "levels", noDecode), key: "gauges",
			wantErr: `no selected owner reads YAML key "gauges"`},
		{name: "both an object and a facet", key: "levels",
			owner:   sectionOwner("example.org/widget", "levels", returning(yamlext.Contribution{Object: &object, Facet: &level{}})),
			wantErr: `.*YAML key "levels" contributed neither or both of an object and a facet`},
		{name: "a facet of no table", key: "levels",
			owner:   sectionOwner("example.org/widget", "levels", returning(yamlext.Contribution{Facet: &level{}, Table: 1})),
			wantErr: `.*YAML key "levels" contributed a facet of no table the document declares`},
		{name: "a model the owner does not declare", key: "levels",
			owner:   sectionOwner("example.org/widget", "levels", returning(yamlext.Contribution{Facet: &other{}})),
			wantErr: `.*YAML key "levels" contributed a model example.org/widget does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := yamlext.NewSet(test.owner)
			c.Assert(err, qt.IsNil)

			contributions, err := set.Decode(test.key, func(any) error { return nil }, yamlext.Tables{{Name: "docs"}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(contributions, qt.IsNil)
		})
	}
}

func TestNewSet_Sections_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		extensions []yamlext.Extension
		wantErr    string
	}{
		{name: "a section without a key", extensions: []yamlext.Extension{sectionOwner("example.org/widget", " ", noDecode)},
			wantErr: `YAML extension of example.org/widget declares a section without a key or a decoder`},
		{name: "a section without a decoder", extensions: []yamlext.Extension{sectionOwner("example.org/widget", "levels", nil)},
			wantErr: `YAML extension of example.org/widget declares a section without a key or a decoder`},
		{name: "a key two owners read", extensions: []yamlext.Extension{
			sectionOwner("example.org/widget", "levels", noDecode),
			{Owner: "example.org/gauge", Coverage: emptyClaim, Sections: []yamlext.Section{{Key: "levels", Decode: noDecode}}},
		}, wantErr: `duplicate.*YAML key "levels" is read by example.org/widget and example.org/gauge`},
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

func TestTables_Find_HappyPath(t *testing.T) {
	tables := yamlext.Tables{
		{Key: "a", Name: "events", Schema: "archive", Struct: "ArchivedEvent"},
		{Key: "b", Name: "events", Schema: "public", Struct: "Event"},
		{Key: "c", Name: "items", Struct: "Item"},
	}
	tests := []struct {
		name, structName, table string
		want                    int
	}{
		{name: "by struct", structName: "Item", want: 2},
		{name: "by name", table: "items", want: 2},
		{name: "by qualified name", table: "public.events", want: 1},
		{name: "a table the document does not declare", table: "other", want: -1},
		{name: "no struct and no name", want: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index, err := tables.Find(test.structName, test.table)

			c.Assert(err, qt.IsNil)
			c.Assert(index, qt.Equals, test.want)
		})
	}
}

func TestTables_Find_FailurePath(t *testing.T) {
	tables := yamlext.Tables{{Name: "events", Schema: "archive"}, {Name: "events", Schema: "public"}}
	tests := []struct {
		name, table, wantErr string
	}{
		{name: "a name two schemas declare", table: "events",
			wantErr: `.*table "events" is declared in schemas "archive" and "public"; name the schema`},
		{name: "not a table reference", table: "a.b.c", wantErr: `.*"a.b.c" is not a table reference`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index, err := tables.Find("", test.table)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(index, qt.Equals, -1)
		})
	}
}
