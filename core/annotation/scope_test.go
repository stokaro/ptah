package annotation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// scoping reads the declarations of ptah:schema:rls:policy scoped to targets,
// and unscoped ones where unscoped is set, through a file decoder that
// records them in files.
func scoping(owner string, files *[]*recordingFile, unscoped bool, targets ...string) annotation.Extension {
	extension := fileWidget(owner, "ptah:schema:"+owner, files, nil)
	extension.Directives = nil
	extension.Kinds = nil
	extension.TargetScopes = []annotation.TargetScope{{Directive: "ptah:schema:rls:policy", Targets: targets,
		Unscoped: unscoped, Label: owner + " targets"}}
	return extension
}

// narrowingFile is a file decoder that also narrows the coverage claim.
type narrowingFile struct{ recordingFile }

func (f *narrowingFile) Cover(schemaext.Coverage) (schemaext.Coverage, error) { return levelCoverage() }

func TestSet_TargetOwner_HappyPath(t *testing.T) {
	var files []*recordingFile
	set := must.Must(annotation.NewSet(scoping("postgres", &files, true, "postgres", "cockroachdb"),
		scoping("sqlserver", &files, false, "sqlserver")))
	tests := []struct {
		name      string
		targets   []string
		wantOwner string
		wantFound bool
	}{
		{name: "no target", wantOwner: "postgres", wantFound: true},
		{name: "one owner's targets", targets: []string{"postgres", "cockroachdb"}, wantOwner: "postgres", wantFound: true},
		{name: "an alias of a target", targets: []string{"mssql"}, wantOwner: "sqlserver", wantFound: true},
		{name: "a target no owner reads", targets: []string{"mysql"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owner, found, err := set.TargetOwner("ptah:schema:rls:policy", test.targets)

			c.Assert(err, qt.IsNil)
			c.Assert(owner, qt.Equals, test.wantOwner)
			c.Assert(found, qt.Equals, test.wantFound)
		})
	}
}

func TestSet_TargetOwner_FailurePath(t *testing.T) {
	var files []*recordingFile
	set := must.Must(annotation.NewSet(scoping("postgres", &files, true, "postgres"), scoping("sqlserver", &files, false, "sqlserver")))
	tests := []struct {
		name    string
		targets []string
		wantErr string
	}{
		{name: "an owner's target beside one no owner reads", targets: []string{"postgres", "mysql"},
			wantErr: `.*a declaration scoped to postgres,mysql mixes postgres targets with others; they declare different objects, ` +
				`so declare one scoped to postgres and another scoped to mysql`},
		{name: "two owners' targets", targets: []string{"sqlserver", "postgres"},
			wantErr: `.*scoped to sqlserver,postgres mixes sqlserver targets with others;.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owner, found, err := set.TargetOwner("ptah:schema:rls:policy", test.targets)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(owner, qt.Equals, "")
			c.Assert(found, qt.IsFalse)
		})
	}
}

// TestReader_ATargetScopedDeclarationReachesItsOwner pins the routing a
// declaration of the frontend's directive takes: to the owner its targets
// name, with the targets it was scoped to, and to no other owner.
func TestReader_ATargetScopedDeclarationReachesItsOwner(t *testing.T) {
	c := qt.New(t)
	var postgres, sqlServer []*recordingFile
	set, err := annotation.NewSet(scoping("postgres", &postgres, true, "postgres"), scoping("sqlserver", &sqlServer, false, "sqlserver"))
	c.Assert(err, qt.IsNil)
	reader := set.Reader()

	_, err = reader.Decode(annotation.Declaration{Directive: "ptah:schema:rls:policy", Targets: []string{"sqlserver"}})
	c.Assert(err, qt.IsNil)
	_, err = reader.Decode(annotation.Declaration{Directive: "ptah:schema:rls:policy"})
	c.Assert(err, qt.IsNil)

	c.Assert(sqlServer, qt.HasLen, 1)
	c.Assert(sqlServer[0].decoded, qt.HasLen, 1)
	c.Assert(sqlServer[0].decoded[0].Targets, qt.DeepEquals, []string{"sqlserver"})
	c.Assert(postgres, qt.HasLen, 1)
	c.Assert(postgres[0].decoded, qt.HasLen, 1)
	c.Assert(postgres[0].decoded[0].Targets, qt.HasLen, 0)
}

func TestReader_Decode_ARefusedScopeIsRefused(t *testing.T) {
	c := qt.New(t)
	var files []*recordingFile
	set, err := annotation.NewSet(scoping("postgres", &files, true, "postgres"))
	c.Assert(err, qt.IsNil)

	contributions, err := set.Reader().Decode(annotation.Declaration{Directive: "ptah:schema:rls:policy", Targets: []string{"postgres", "mysql"}})

	c.Assert(err, qt.ErrorMatches, `.*mixes postgres targets with others.*`)
	c.Assert(contributions, qt.IsNil)
	c.Assert(files, qt.HasLen, 0)
}

// TestReader_Cover_NarrowsTheClaimByEachFile pins that a file decoder that
// narrows the claim is asked to, and one that does not is left alone.
func TestReader_Cover_NarrowsTheClaimByEachFile(t *testing.T) {
	c := qt.New(t)
	extension := widget("example.org/widget", "ptah:schema:widget")
	extension.Decode = nil
	extension.File = func() annotation.FileDecoder { return &narrowingFile{} }
	set, err := annotation.NewSet(extension)
	c.Assert(err, qt.IsNil)
	reader := set.Reader()
	untouched, err := reader.Cover(schemaext.Coverage{})
	c.Assert(err, qt.IsNil)
	_, err = reader.Decode(annotation.Declaration{Directive: "ptah:schema:widget"})
	c.Assert(err, qt.IsNil)

	narrowed, err := reader.Cover(schemaext.Coverage{})

	c.Assert(err, qt.IsNil)
	c.Assert(untouched.IsZero(), qt.IsTrue)
	c.Assert(narrowed.Lookup(levelKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

func TestNewSet_TargetScopes_FailurePath(t *testing.T) {
	var files []*recordingFile
	noTarget := scoping("postgres", &files, false)
	noDecoder := scoping("postgres", &files, true, "postgres")
	noDecoder.File = nil
	tests := []struct {
		name       string
		extensions []annotation.Extension
		wantErr    string
	}{
		{name: "a scope without a target", extensions: []annotation.Extension{noTarget},
			wantErr: `annotation extension of postgres declares a scope without a directive or a target`},
		{name: "a scope without a decoder", extensions: []annotation.Extension{noDecoder},
			wantErr: `annotation extension of postgres declares directives and no decoder`},
		{name: "a target two owners read", extensions: []annotation.Extension{
			scoping("postgres", &files, false, "postgres"), scoping("pg", &files, false, "postgresql")},
			wantErr: `duplicate.*declarations of "ptah:schema:rls:policy" scoped to postgres are read by postgres and pg`},
		{name: "unscoped declarations two owners read", extensions: []annotation.Extension{
			scoping("postgres", &files, true, "postgres"), scoping("sqlserver", &files, true, "sqlserver")},
			wantErr: `duplicate.*unscoped declarations of "ptah:schema:rls:policy" are read by postgres and sqlserver`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := annotation.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}
