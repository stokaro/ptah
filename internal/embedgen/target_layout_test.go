package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedgen"
)

// identityBeforeTheLayoutFieldExisted is baseSpec's identity measured on the
// commit before Target.Layout was declared.
//
// A literal rather than a second computation, because the thing it has to
// disagree with is a change to how the identity is computed. It was taken by
// running the same fixture in a worktree of origin/master, so it is a
// measurement rather than a value copied out of the code it is guarding.
const identityBeforeTheLayoutFieldExisted = "f795a4fd294cf12fdcde88ae075ba40ccec57d9321bcce6e5738049129403c7a"

// ownTableSpec is baseSpec with its vectors in a relation of their own, which
// is the only configuration LayoutOwnTable accepts: the relation Ptah creates
// and later drops cannot be the source.
func ownTableSpec() embedgen.Spec {
	spec := baseSpec()
	spec.Target.Table = "article_vectors"
	spec.Target.Layout = embedgen.LayoutOwnTable
	return spec
}

// TestTargetObjects_OwnTableHappyPath pins what the own-table layout derives
// beyond the column every layout has.
func TestTargetObjects_OwnTableHappyPath(t *testing.T) {
	c := qt.New(t)
	spec := ownTableSpec()

	objects, err := spec.TargetObjects()

	c.Assert(err, qt.IsNil)
	c.Assert(objects.OwnsTable, qt.IsTrue)
	c.Assert(objects.ForeignKeyName, qt.Equals,
		embedgen.ForeignKeyName("article_vectors", "embedding_v1", spec.Identity().Digest))
	c.Assert(objects.TableComment, qt.Equals, embedgen.TableComment(spec.Identity().Digest))
}

// TestTargetObjects_SourceColumnsDerivesNoTable is the control the assertion
// above needs: the three fields are set BECAUSE the layout owns a table, not
// unconditionally. Without it, a derivation that ignored the layout would
// satisfy the happy path and hand retirement a table comment for a generation
// whose columns live on the application's own rows.
func TestTargetObjects_SourceColumnsDerivesNoTable(t *testing.T) {
	c := qt.New(t)

	objects, err := baseSpec().TargetObjects()

	c.Assert(err, qt.IsNil)
	c.Assert(objects.OwnsTable, qt.IsFalse)
	c.Assert(objects.ForeignKeyName, qt.Equals, "")
	c.Assert(objects.TableComment, qt.Equals, "")
}

// TestTargetObjects_LayoutFailurePath is the pair of refusals that exist
// because the layout decides what Ptah may destroy.
func TestTargetObjects_LayoutFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*embedgen.Spec)
		wantErr string
	}{
		{
			name:    "a layout this build does not have",
			change:  func(s *embedgen.Spec) { s.Target.Layout = embedgen.TargetLayout("own-table") },
			wantErr: `target objects: "own-table" is not a target layout`,
		},
		{
			name: "its own table is the source table",
			change: func(s *embedgen.Spec) {
				s.Target.Layout = embedgen.LayoutOwnTable
				s.Target.Schema, s.Target.Table = s.Source.Schema, s.Source.Table
			},
			wantErr: `target objects: layout "own_table" names the source relation public\.article as its target.*`,
		},
		{
			name: "its own table is the source table, both unqualified",
			change: func(s *embedgen.Spec) {
				s.Source.Schema, s.Target.Schema = "", ""
				s.Target.Layout = embedgen.LayoutOwnTable
				s.Target.Table = s.Source.Table
			},
			wantErr: `target objects: layout "own_table" names the source relation article as its target.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			test.change(&spec)

			objects, err := spec.TargetObjects()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(objects, qt.DeepEquals, embedgen.TargetObjects{})
		})
	}
}

// TestTargetLayout_OwnsTable pins the predicate both creation and destruction
// read, because they have to read the same one.
func TestTargetLayout_OwnsTable(t *testing.T) {
	c := qt.New(t)

	c.Assert(embedgen.LayoutOwnTable.OwnsTable(), qt.IsTrue)
	c.Assert(embedgen.LayoutSourceColumns.OwnsTable(), qt.IsFalse)
	c.Assert(embedgen.TargetLayout("own-table").OwnsTable(), qt.IsFalse)
}

// TestKnownLayout pins that a misspelling is not the zero value.
//
// It is the property the specification reader depends on: a layout folded to
// the default would put a generation's columns on the application's rows after
// its author asked for a relation of the generation's own.
func TestKnownLayout(t *testing.T) {
	c := qt.New(t)

	c.Assert(embedgen.KnownLayout(embedgen.LayoutSourceColumns), qt.IsTrue)
	c.Assert(embedgen.KnownLayout(embedgen.LayoutOwnTable), qt.IsTrue)
	c.Assert(embedgen.KnownLayout(embedgen.TargetLayout("own-table")), qt.IsFalse)
	c.Assert(embedgen.KnownLayout(embedgen.TargetLayout("source_columns")), qt.IsFalse)
}

// TestTargetLayout_IsOutsideTheIdentity is the measurement the exclusion in
// excludedFromIdentity claims.
//
// Two assertions, and the second is the one that matters. The first says the
// layout does not move a digest. The second says adding the field moved NO
// existing digest: baseSpec carries the zero layout, so its identity is what it
// was before the field existed, and the literal below is that value. A field
// silently joining identityComponents would redden this while every test that
// merely compares two live digests to each other stayed green.
func TestTargetLayout_IsOutsideTheIdentity(t *testing.T) {
	c := qt.New(t)

	sourceColumns := baseSpec()
	ownTable := baseSpec()
	ownTable.Target.Layout = embedgen.LayoutOwnTable

	c.Assert(ownTable.Identity().Digest, qt.Equals, sourceColumns.Identity().Digest)
	c.Assert(sourceColumns.Identity().Digest, qt.Equals, identityBeforeTheLayoutFieldExisted)
}
