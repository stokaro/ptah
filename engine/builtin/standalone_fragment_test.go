package builtin_test

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/astrouteguard"
)

// carrierName names the object of the statement a standalone fragment is
// compared with. It is distinctive so that rewriting it out of a message
// cannot touch anything else the message says.
const carrierName = "standalone_carrier"

// TestVisitNode_StandaloneFragmentAnswersAsItsStatement holds every fragment,
// on every target, to one rule: a fragment that arrives without the statement
// that carries it gets the answer that statement would get. An alter
// operation is carried by an ALTER TABLE, a type definition by a CREATE TYPE,
// and a type operation by an ALTER TYPE.
//
// Where the statement refuses the fragment, the fragment alone is refused with
// the same sentinels and the same reason, without the name the statement gives
// its object. Only where the statement renders does the fragment alone say it
// needs the statement that carries it; saying that of a fragment the target
// refuses anyway, such as a changefeed on PostgreSQL or an enum value on YDB,
// sends the caller to a wrapper that cannot help.
//
// The matrix is derived on every side: the fragments are every type in
// core/ast carrying one of the markers, and the targets are every dialect the
// renderer accepts. Every entry point is asked, since each has to give the
// answer VisitNode gives.
func TestVisitNode_StandaloneFragmentAnswersAsItsStatement(t *testing.T) {
	families := fragmentFamilies()
	root, err := astrouteguard.ModuleRoot()
	qt.New(t).Assert(err, qt.IsNil)

	for _, family := range families {
		kinds, err := astrouteguard.MarkedKinds(root, family.marker)
		qt.New(t).Assert(err, qt.IsNil)
		for _, dialect := range renderedDialects() {
			for _, kind := range kinds {
				t.Run(dialect+"/"+kind.Name, func(t *testing.T) {
					c := qt.New(t)
					fragment := fragmentFixture(c, family, kind.Name)

					carried := visit(c, dialect, family.carry(fragment, carrierName))
					standalone := visit(c, dialect, fragment)

					c.Assert(standalone, qt.IsNotNil)
					c.Assert(needsParent(standalone, fragment), qt.Equals, carried == nil,
						qt.Commentf("inside its statement: %v\nalone: %v", carried, standalone))

					// Where the statement refuses, cmp.Or picks its refusal
					// and the fragment alone has to carry the same one, which
					// names a sentinel a caller can branch on. Where it
					// renders, the fragment alone is a malformed input on
					// every target, ErrInvalidSchemaDiff, in the words the
					// assertion above already identified.
					c.Assert(sentinels(cmp.Or(carried, ptaherr.ErrInvalidSchemaDiff)), qt.Not(qt.HasLen), 0)
					c.Assert(sentinels(standalone), qt.DeepEquals, sentinels(cmp.Or(carried, ptaherr.ErrInvalidSchemaDiff)))
					c.Assert(standalone.Error(), qt.Equals, withoutCarrierName(cmp.Or(carried, standalone).Error()))

					visited := visitAnswer(c, dialect, fragment)
					c.Assert(renderAnswer(c, dialect, fragment), qt.DeepEquals, visited)
					c.Assert(renderSQLAnswer(dialect, fragment), qt.DeepEquals, visited)
				})
			}
		}
	}
}

// TestStandaloneFragment_ReadsWithoutAName pins the wording the rewrite in the
// matrix above stands for. A refusal names the capability or the fact the
// target lacks, and refers to the object without a name rather than naming
// one called "".
func TestStandaloneFragment_ReadsWithoutAName(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		fragment ast.Node
		want     string
	}{
		{
			name:     "a changefeed on PostgreSQL",
			dialect:  platform.Postgres,
			fragment: &ast.ExtensionAlterOperation{Payload: &ydbast.AddChangefeed{Changefeed: ydbschema.ChangefeedSpec{Name: "cf", Mode: "UPDATES", Format: "JSON"}}},
			want:     `target "postgres" does not support extension "ptah.run/ydb/add-changefeed" in role "alter-table"`,
		},
		{
			name:     "an enum on YDB",
			dialect:  platform.YDB,
			fragment: ast.NewEnumTypeDef("active", "inactive"),
			want:     `a type, which requires target capability enum_custom_type, unavailable on this ydb target`,
		},
		{
			name:     "an enum value on YDB",
			dialect:  platform.YDB,
			fragment: ast.NewAddEnumValueOperation("archived"),
			want:     `ALTER TYPE, which requires target capability enum_custom_type, unavailable on this ydb target`,
		},
		{
			name:     "an enum value on Oracle",
			dialect:  platform.Oracle,
			fragment: ast.NewAddEnumValueOperation("archived"),
			want:     `unsupported feature: oracle: ALTER TYPE: user types are not rendered for Oracle`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := visit(c, test.dialect, test.fragment)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestStandaloneFragmentFixtures_CoverEveryFragment keeps the fixture tables
// and the derived corpora equal in both directions, and the families equal to
// the markers core/ast declares. A fragment without a fixture would fail the
// matrix for the fixture's sake, and a new kind of fragment, a marker no
// family names, would not be checked at all: the renderer has to learn the
// statement that carries it first.
func TestStandaloneFragmentFixtures_CoverEveryFragment(t *testing.T) {
	c := qt.New(t)
	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)

	markers, err := astrouteguard.Markers(root)
	c.Assert(err, qt.IsNil)
	var named []astrouteguard.Marker
	for _, family := range fragmentFamilies() {
		named = append(named, family.marker)

		kinds, err := astrouteguard.MarkedKinds(root, family.marker)
		c.Assert(err, qt.IsNil)
		names := make([]string, 0, len(kinds))
		for _, kind := range kinds {
			names = append(names, kind.Name)
		}
		c.Assert(slices.Sorted(maps.Keys(family.fixtures)), qt.DeepEquals, names)
	}
	c.Assert(slices.Sorted(slices.Values(named)), qt.DeepEquals, markers)
}

// TestStandaloneFragmentFixtures_RenderOnSomeTarget is the fixtures' own
// control. Each one is built with the fields its handlers read, so that a
// refusal the matrix compares is about the fragment and the target. A fixture
// every target refuses would compare one broken fragment with itself and pass.
func TestStandaloneFragmentFixtures_RenderOnSomeTarget(t *testing.T) {
	for _, family := range fragmentFamilies() {
		for _, kind := range slices.Sorted(maps.Keys(family.fixtures)) {
			t.Run(kind, func(t *testing.T) {
				c := qt.New(t)

				var refusals []string
				for _, dialect := range renderedDialects() {
					err := visit(c, dialect, family.carry(fragmentFixture(c, family, kind), carrierName))
					refusals = append(refusals, fmt.Sprint(err))
				}

				c.Assert(refusals, qt.Contains, "<nil>", qt.Commentf("every target refuses the fixture: %q", refusals))
			})
		}
	}
}

// fragmentFamily is one kind of fragment: the marker core/ast gives it, a
// fixture of every fragment it holds, and the statement that carries one.
type fragmentFamily struct {
	marker   astrouteguard.Marker
	fixtures map[string]func() ast.Node
	// carry wraps fragment in the statement that carries it, naming the
	// statement's object name.
	carry func(fragment ast.Node, name string) ast.Node
}

func fragmentFamilies() []fragmentFamily {
	return []fragmentFamily{
		{
			marker:   astrouteguard.AlterOperationMarker,
			fixtures: alterOperationFixtures(),
			carry: func(fragment ast.Node, name string) ast.Node {
				return &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{fragment.(ast.AlterOperation)}}
			},
		},
		{
			marker:   astrouteguard.TypeDefinitionMarker,
			fixtures: typeDefinitionFixtures(),
			carry: func(fragment ast.Node, name string) ast.Node {
				return &ast.CreateTypeNode{Name: name, TypeDef: fragment.(ast.TypeDefinition)}
			},
		},
		{
			marker:   astrouteguard.TypeOperationMarker,
			fixtures: typeOperationFixtures(),
			carry: func(fragment ast.Node, name string) ast.Node {
				return &ast.AlterTypeNode{Name: name, Operations: []ast.TypeOperation{fragment.(ast.TypeOperation)}}
			},
		},
	}
}

// renderedDialects is every target the renderer accepts, once each.
func renderedDialects() []string {
	var dialects []string
	for _, name := range builtin.SupportedDialects() {
		dialects = append(dialects, platform.NormalizeDialect(name))
	}
	slices.Sort(dialects)
	return slices.Compact(dialects)
}

// fragmentFixture is a fresh copy of kind's fixture in family.
func fragmentFixture(c *qt.C, family fragmentFamily, kind string) ast.Node {
	c.Helper()
	build, ok := family.fixtures[kind]
	c.Assert(ok, qt.IsTrue, qt.Commentf("no %s fixture for %s", family.marker, kind))
	return build()
}

// alterOperationFixtures builds each alter operation with the fields its
// handlers read and nothing more.
//
// A zero value would not do: several handlers return early or refuse on an
// empty field before they reach the question the matrix asks. PostgreSQL
// writes nothing for a SetRowTTLOperation without options, for example, and
// would then report as rendered an operation it refuses for a missing
// capability once it has one.
func alterOperationFixtures() map[string]func() ast.Node {
	changefeed := ydbschema.ChangefeedSpec{Name: "cf", Mode: "UPDATES", Format: "JSON"}
	return map[string]func() ast.Node{
		"ExtensionAlterOperation": func() ast.Node {
			return &ast.ExtensionAlterOperation{Payload: &ydbast.AddChangefeed{Changefeed: changefeed}}
		},
		"AddColumnOperation": func() ast.Node {
			return &ast.AddColumnOperation{Column: ast.NewColumn("c", "INTEGER")}
		},
		"AddConstraintOperation": func() ast.Node {
			return &ast.AddConstraintOperation{Constraint: ast.NewUniqueConstraint("uq_c", "c")}
		},
		"AddIndexOperation": func() ast.Node {
			return &ast.AddIndexOperation{Index: &ast.IndexNode{Name: "ix_c", Columns: []string{"c"}}}
		},
		"AddSkippingIndexOperation": func() ast.Node {
			return &ast.AddSkippingIndexOperation{Name: "ix_c", Expression: "c", IndexType: "minmax", Granularity: 1}
		},
		"AlterColumnOperation": func() ast.Node {
			return &ast.AlterColumnOperation{ColumnName: "c", Action: ast.AlterColumnDropDefault}
		},
		"AlterGeneratedColumnExpressionOperation": func() ast.Node {
			return &ast.AlterGeneratedColumnExpressionOperation{ColumnName: "c", Expression: "a + 1"}
		},
		"AlterIndexVisibilityOperation": func() ast.Node {
			return &ast.AlterIndexVisibilityOperation{IndexName: "ix_c", Invisible: true}
		},
		"DropColumnOperation": func() ast.Node {
			return &ast.DropColumnOperation{ColumnName: "c"}
		},
		"DropConstraintOperation": func() ast.Node {
			return &ast.DropConstraintOperation{ConstraintName: "ck_c"}
		},
		"DropRowDeletionPolicyOperation": func() ast.Node {
			return &ast.DropRowDeletionPolicyOperation{}
		},
		"ModifyColumnOperation": func() ast.Node {
			return &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "INTEGER")}
		},
		"ModifyTTLOperation": func() ast.Node {
			return &ast.ModifyTTLOperation{Expression: "d + INTERVAL 1 DAY"}
		},
		"RenameColumnOperation": func() ast.Node {
			return &ast.RenameColumnOperation{OldName: "a", NewName: "b"}
		},
		"RenameConstraintOperation": func() ast.Node {
			return &ast.RenameConstraintOperation{From: "ck_a", To: "ck_b"}
		},
		"RenameIndexOperation": func() ast.Node {
			return &ast.RenameIndexOperation{From: "ix_a", To: "ix_b"}
		},
		"RenameTableOperation": func() ast.Node {
			return &ast.RenameTableOperation{NewName: "renamed"}
		},
		"ReplaceIndexOperation": func() ast.Node {
			return &ast.ReplaceIndexOperation{Index: &ast.IndexNode{Name: "ix_c", Columns: []string{"c"}}}
		},
		"ResetRowTTLOperation": func() ast.Node {
			return &ast.ResetRowTTLOperation{Parameters: []string{"ttl"}}
		},
		"SetCommentOperation": func() ast.Node {
			return &ast.SetCommentOperation{Comment: "orders placed online"}
		},
		"SetConstraintCommentOperation": func() ast.Node {
			return &ast.SetConstraintCommentOperation{Constraint: "ck_c", Comment: "positive"}
		},
		"SetIndexPartitioningOperation": func() ast.Node {
			return &ast.SetIndexPartitioningOperation{
				IndexName:    "ix_c",
				Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 2},
			}
		},
		"SetYDBColumnFamiliesOperation": func() ast.Node {
			return &ast.SetYDBColumnFamiliesOperation{
				Families: []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4", Columns: []string{"c"}}},
			}
		},
		"SetRowDeletionPolicyOperation": func() ast.Node {
			return &ast.SetRowDeletionPolicyOperation{Column: "created_at", Interval: "1 day"}
		},
		"SetRowTTLOperation": func() ast.Node {
			return &ast.SetRowTTLOperation{Options: []string{"ttl_expiration_expression = 'created_at + INTERVAL ''1 day'''"}}
		},
		"SetYDBTablePartitioningOperation": func() ast.Node {
			return &ast.SetYDBTablePartitioningOperation{Partitioning: &ast.YDBTablePartitioningSpec{MinPartitions: 2}}
		},
		"ValidateConstraintOperation": func() ast.Node {
			return &ast.ValidateConstraintOperation{ConstraintName: "ck_c"}
		},
	}
}

// typeDefinitionFixtures builds each type definition with the fields a CREATE
// TYPE reads.
func typeDefinitionFixtures() map[string]func() ast.Node {
	return map[string]func() ast.Node{
		"CompositeTypeDef": func() ast.Node {
			return ast.NewCompositeTypeDef(&ast.CompositeField{Name: "street", Type: "TEXT"})
		},
		"DomainTypeDef": func() ast.Node { return ast.NewDomainTypeDef("INTEGER") },
		"EnumTypeDef":   func() ast.Node { return ast.NewEnumTypeDef("active", "inactive") },
		"RangeTypeDef":  func() ast.Node { return ast.NewRangeTypeDef("INTEGER") },
	}
}

// typeOperationFixtures builds each type operation with the fields an ALTER
// TYPE reads.
func typeOperationFixtures() map[string]func() ast.Node {
	return map[string]func() ast.Node{
		"AddEnumValueOperation": func() ast.Node { return ast.NewAddEnumValueOperation("archived") },
		"CompositeAttributeOperation": func() ast.Node {
			return ast.NewAddCompositeAttributeOperation("city", "TEXT")
		},
		"DomainConstraintOperation": func() ast.Node { return ast.NewAddDomainConstraintOperation("VALUE > 0") },
		"DomainDefaultOperation":    func() ast.Node { return ast.NewSetDomainDefaultOperation("0") },
		"DomainNotNullOperation":    func() ast.Node { return ast.NewDomainNotNullOperation(true) },
		"RenameEnumValueOperation": func() ast.Node {
			return ast.NewRenameEnumValueOperation("inactive", "dormant")
		},
		"RenameTypeOperation": func() ast.Node { return ast.NewRenameTypeOperation("renamed") },
	}
}

// visit sends node to a fresh renderer for dialect and returns its answer.
func visit(c *qt.C, dialect string, node ast.Node) error {
	c.Helper()
	r, err := builtin.NewRendererWithCapabilities(dialect, capability.ForDialect(dialect))
	c.Assert(err, qt.IsNil)
	return r.VisitNode(node)
}

// needsParent reports whether err is a renderer's answer to a fragment that
// arrived without the statement that carries it: the fragment's own type, and
// the sentence every renderer ends that answer with.
func needsParent(err error, fragment ast.Node) bool {
	return claimsItsParent(fmt.Sprint(err), fragment)
}

// claimsItsParent reports whether message is the needs-parent answer about
// node itself, rather than about a statement node holds.
func claimsItsParent(message string, node ast.Node) bool {
	if _, ok := node.(*ast.ExtensionAlterOperation); ok {
		return strings.Contains(message, "requires an ALTER TABLE parent")
	}
	return strings.Contains(message, fmt.Sprintf("%T", node)) && strings.HasSuffix(message, needsParentSuffix)
}

// needsParentSuffix is how every renderer ends its refusal of a fragment that
// arrived without the statement that carries it.
const needsParentSuffix = "the statement that carries it, not on its own"

// sentinels names the ptaherr sentinels err satisfies, in a fixed order.
func sentinels(err error) []string {
	var matched []string
	for _, sentinel := range []error{
		ptaherr.ErrUnsupportedDialect,
		ptaherr.ErrUnknownAttribute,
		ptaherr.ErrMissingRequiredAttribute,
		ptaherr.ErrInvalidAttributeValue,
		ptaherr.ErrRetiredAttribute,
		ptaherr.ErrUnsupportedFeature,
		ptaherr.ErrInvalidSchemaDiff,
	} {
		if errors.Is(err, sentinel) {
			matched = append(matched, sentinel.Error())
		}
	}
	return matched
}

// withoutCarrierName is message as a standalone fragment phrases it. Where the
// statement's refusal names its object, the fragment's does without the name:
// "a table" for `table "orders"`, "a type" for "type status", and the bare
// keyword for "ALTER TYPE status".
func withoutCarrierName(message string) string {
	return strings.NewReplacer(
		fmt.Sprintf("table %q", carrierName), "a table",
		"TYPE "+carrierName, "TYPE",
		"type "+carrierName, "a type",
	).Replace(message)
}
