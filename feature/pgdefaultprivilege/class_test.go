package pgdefaultprivilege_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestClassRef_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		class pgdefaultprivilege.Class
	}{
		{"all roles global", pgdefaultprivilege.Class{ObjectType: "TABLES"}},
		{"all roles scoped", pgdefaultprivilege.Class{Schema: " app ", ObjectType: "TABLES"}},
		{"unknown class", pgdefaultprivilege.Class{Grantor: "owner", ObjectType: "x"}},
		{"punctuation", pgdefaultprivilege.Class{Grantor: ` a|b,"c `, Schema: `x.y`, ObjectType: "TABLES"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := pgdefaultprivilege.ClassRef(test.class, identifier.ForDialect("postgres"))
			c.Assert(err, qt.IsNil)
			restored, err := pgdefaultprivilege.ClassOf(ref)
			c.Assert(err, qt.IsNil)
			c.Assert(restored, qt.Equals, test.class)
			c.Assert(ref.Kind, qt.Equals, pgdefaultprivilege.ClassSubjectKind)
			c.Assert(ref.Parent, qt.Equals, objectidentity.Part{})
			_, err = pgdefaultprivilege.IdentityOf(ref)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestClassCoverage_RefusesAuthorityOverUnknownACL(t *testing.T) {
	c := qt.New(t)
	ref, err := pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{ObjectType: "TABLES"}, identifier.ForDialect("postgres"))
	c.Assert(err, qt.IsNil)
	for _, state := range []schemaext.KnowledgeState{schemaext.Complete, schemaext.Absent, schemaext.Defaulted} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			known, err := pgdefaultprivilege.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
				{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: state}},
			})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(known.IsZero(), qt.IsTrue)
		})
	}
}

func TestClassCoverage_CodecPreservesScopeAndReason(t *testing.T) {
	c := qt.New(t)
	var codecs []schemaext.OwnedCodec
	for _, codec := range pgdefaultprivilege.Codecs() {
		codecs = append(codecs, schemaext.OwnedCodec{Owner: pgdefaultprivilege.Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(codecs...)
	c.Assert(err, qt.IsNil)
	ref, err := pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{ObjectType: "TABLES"}, identifier.ForDialect("postgres"))
	c.Assert(err, qt.IsNil)
	before, err := pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "FOR ALL ROLES has no named grantor"}},
	})
	c.Assert(err, qt.IsNil)
	wire, err := registry.EncodeCoverage(t.Context(), schemaext.Observed, before)
	c.Assert(err, qt.IsNil)
	after, err := registry.DecodeCoverage(t.Context(), wire)
	c.Assert(err, qt.IsNil)
	c.Assert(after.KindRecords(), qt.DeepEquals, before.KindRecords())
	c.Assert(after.SubjectRecords(), qt.DeepEquals, before.SubjectRecords())
}

func TestClassOf_FailurePath(t *testing.T) {
	c := qt.New(t)
	valid, err := pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{ObjectType: "TABLES"}, identifier.ForDialect("postgres"))
	c.Assert(err, qt.IsNil)
	malformed := valid
	malformed.Name.Source = `["", "TABLES"]`
	unknownRole := valid
	unknownRole.Name.Normalized = `["unknown (OID=0)","TABLES"]`
	wrongClass := valid
	wrongClass.Name.Normalized = `["","SEQUENCES"]`
	parent := valid
	parent.Parent.Source = "table"
	for _, ref := range []objectidentity.ID{malformed, unknownRole, wrongClass, parent} {
		t.Run(ref.String(), func(t *testing.T) {
			c := qt.New(t)
			class, err := pgdefaultprivilege.ClassOf(ref)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(class, qt.Equals, pgdefaultprivilege.Class{})
		})
	}
}
