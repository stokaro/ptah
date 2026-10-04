package renderer_test

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/astrouteguard"
)

// carrierTable names the table of the ALTER TABLE a standalone operation is
// compared with. It is distinctive so that rewriting it out of a message
// cannot touch anything else the message says.
const carrierTable = "standalone_carrier"

// TestVisitNode_StandaloneAlterOperationAnswersAsItsAlterTable holds every
// alter operation, on every target, to one rule: an operation that arrives
// without its ALTER TABLE gets the answer that ALTER TABLE would get.
//
// Where the ALTER TABLE refuses the operation, the operation alone is refused
// with the same sentinels and the same reason, naming "a table" where the
// ALTER TABLE names its table. Only where the ALTER TABLE renders does the
// operation alone say it needs the statement that carries it; saying that of
// an operation the target refuses anyway, such as a changefeed on PostgreSQL,
// sends the caller to a wrapper that cannot help.
//
// Both sides of the matrix are derived. The operations are every type in
// core/ast carrying the alter-operation marker, and the targets are every
// dialect the renderer accepts, so an operation or a target added later is
// held to the rule without an edit here.
func TestVisitNode_StandaloneAlterOperationAnswersAsItsAlterTable(t *testing.T) {
	kinds := alterOperationKinds(qt.New(t))

	for _, dialect := range renderedDialects() {
		for _, kind := range kinds {
			t.Run(dialect+"/"+kind, func(t *testing.T) {
				c := qt.New(t)
				operation := alterOperationFixture(c, kind)

				carried := visit(c, dialect, &ast.AlterTableNode{
					Name:       carrierTable,
					Operations: []ast.AlterOperation{operation},
				})
				standalone := visit(c, dialect, operation)

				c.Assert(standalone, qt.IsNotNil)
				c.Assert(needsParent(standalone, operation), qt.Equals, carried == nil,
					qt.Commentf("inside ALTER TABLE: %v\nalone: %v", carried, standalone))

				// Where the ALTER TABLE refuses, cmp.Or picks its refusal and
				// the operation alone has to carry the same one. Where it
				// renders, the expectation is the needs-parent answer the
				// assertion above already identified, in the dialect's own
				// words and with the dialect's own sentinel.
				expected := cmp.Or(carried, standalone)
				c.Assert(sentinels(standalone), qt.DeepEquals, sentinels(expected))
				c.Assert(standalone.Error(), qt.Equals, withoutCarrierTable(expected.Error()))
			})
		}
	}
}

// TestRender_StandaloneAlterOperationAnswersAsVisitNode covers the second
// entry point. Render prepares its node through its own switch and hands the
// result to the dialect without passing VisitNode, so an operation that only
// VisitNode checked would reach the dialect's needs-parent answer from here
// whatever an ALTER TABLE says.
func TestRender_StandaloneAlterOperationAnswersAsVisitNode(t *testing.T) {
	kinds := alterOperationKinds(qt.New(t))

	for _, dialect := range renderedDialects() {
		for _, kind := range kinds {
			t.Run(dialect+"/"+kind, func(t *testing.T) {
				c := qt.New(t)
				operation := alterOperationFixture(c, kind)

				visited := visit(c, dialect, operation)
				r, err := renderer.NewRendererWithCapabilities(dialect, capability.ForDialect(dialect))
				c.Assert(err, qt.IsNil)
				output, rendered := r.Render(operation)

				c.Assert(rendered, qt.ErrorMatches, regexp.QuoteMeta(fmt.Sprint(visited)))
				c.Assert(output, qt.Equals, "")
			})
		}
	}
}

// TestStandaloneAlterOperation_ReadsWithoutATable pins the wording the rewrite
// in the matrix above stands for, on the operation the defect was found with.
// The refusal names the capability the target lacks and says "a table" rather
// than naming a table called "".
func TestStandaloneAlterOperation_ReadsWithoutATable(t *testing.T) {
	c := qt.New(t)

	err := visit(c, platform.Postgres, alterOperationFixture(c, "AddChangefeedOperation"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `changing the changefeeds of a table, which requires target capability changefeeds, unavailable on this postgres target`)
}

// TestStandaloneAlterOperationFixtures_CoverEveryAlterOperation keeps the
// fixture table and the derived corpus equal in both directions: an operation
// without a fixture would fail every subtest above for the fixture's sake, and
// a fixture for an operation that no longer exists is dead weight.
func TestStandaloneAlterOperationFixtures_CoverEveryAlterOperation(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	kinds, err := astrouteguard.AlterOperationKinds(root)
	c.Assert(err, qt.IsNil)

	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.Name)
	}
	c.Assert(slices.Sorted(maps.Keys(alterOperationFixtures())), qt.DeepEquals, names)
}

// TestStandaloneAlterOperationFixtures_RenderOnSomeTarget is the fixtures'
// own control. Each one is built with the fields its handlers read, so that a
// refusal the matrix compares is about the operation and the target. A fixture
// every target refuses would compare one broken operation with itself and
// pass.
func TestStandaloneAlterOperationFixtures_RenderOnSomeTarget(t *testing.T) {
	for _, kind := range slices.Sorted(maps.Keys(alterOperationFixtures())) {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)

			var refusals []string
			for _, dialect := range renderedDialects() {
				err := visit(c, dialect, &ast.AlterTableNode{
					Name:       carrierTable,
					Operations: []ast.AlterOperation{alterOperationFixture(c, kind)},
				})
				refusals = append(refusals, fmt.Sprint(err))
			}

			c.Assert(refusals, qt.Contains, "<nil>", qt.Commentf("every target refuses the fixture: %q", refusals))
		})
	}
}

// alterOperationKinds is the derived corpus, by type name.
func alterOperationKinds(c *qt.C) []string {
	c.Helper()
	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	kinds, err := astrouteguard.AlterOperationKinds(root)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.Name)
	}
	return names
}

// renderedDialects is every target the renderer accepts, once each.
func renderedDialects() []string {
	var dialects []string
	for _, name := range renderer.SupportedDialects() {
		dialects = append(dialects, platform.NormalizeDialect(name))
	}
	slices.Sort(dialects)
	return slices.Compact(dialects)
}

// alterOperationFixture is a fresh copy of kind's fixture.
func alterOperationFixture(c *qt.C, kind string) ast.AlterOperation {
	c.Helper()
	build, ok := alterOperationFixtures()[kind]
	c.Assert(ok, qt.IsTrue, qt.Commentf("no fixture for %s", kind))
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
func alterOperationFixtures() map[string]func() ast.AlterOperation {
	changefeed := ast.ChangefeedSpec{Name: "cf", Mode: "UPDATES", Format: "JSON"}
	return map[string]func() ast.AlterOperation{
		"AddChangefeedOperation": func() ast.AlterOperation {
			return &ast.AddChangefeedOperation{Changefeed: changefeed}
		},
		"AddColumnOperation": func() ast.AlterOperation {
			return &ast.AddColumnOperation{Column: ast.NewColumn("c", "INTEGER")}
		},
		"AddConstraintOperation": func() ast.AlterOperation {
			return &ast.AddConstraintOperation{Constraint: ast.NewUniqueConstraint("uq_c", "c")}
		},
		"AddIndexOperation": func() ast.AlterOperation {
			return &ast.AddIndexOperation{Index: &ast.IndexNode{Name: "ix_c", Columns: []string{"c"}}}
		},
		"AddSkippingIndexOperation": func() ast.AlterOperation {
			return &ast.AddSkippingIndexOperation{Name: "ix_c", Expression: "c", IndexType: "minmax", Granularity: 1}
		},
		"AlterChangefeedTopicOperation": func() ast.AlterOperation {
			grown := changefeed
			grown.TopicMinActivePartitions = 2
			return &ast.AlterChangefeedTopicOperation{Changefeed: grown, Previous: changefeed}
		},
		"AlterColumnOperation": func() ast.AlterOperation {
			return &ast.AlterColumnOperation{ColumnName: "c", Action: ast.AlterColumnDropDefault}
		},
		"AlterGeneratedColumnExpressionOperation": func() ast.AlterOperation {
			return &ast.AlterGeneratedColumnExpressionOperation{ColumnName: "c", Expression: "a + 1"}
		},
		"AlterIndexVisibilityOperation": func() ast.AlterOperation {
			return &ast.AlterIndexVisibilityOperation{IndexName: "ix_c", Invisible: true}
		},
		"DropChangefeedOperation": func() ast.AlterOperation {
			return &ast.DropChangefeedOperation{Name: "cf"}
		},
		"DropColumnOperation": func() ast.AlterOperation {
			return &ast.DropColumnOperation{ColumnName: "c"}
		},
		"DropConstraintOperation": func() ast.AlterOperation {
			return &ast.DropConstraintOperation{ConstraintName: "ck_c"}
		},
		"DropRowDeletionPolicyOperation": func() ast.AlterOperation {
			return &ast.DropRowDeletionPolicyOperation{}
		},
		"ModifyColumnOperation": func() ast.AlterOperation {
			return &ast.ModifyColumnOperation{Column: ast.NewColumn("c", "INTEGER")}
		},
		"ModifyTTLOperation": func() ast.AlterOperation {
			return &ast.ModifyTTLOperation{Expression: "d + INTERVAL 1 DAY"}
		},
		"RenameColumnOperation": func() ast.AlterOperation {
			return &ast.RenameColumnOperation{OldName: "a", NewName: "b"}
		},
		"RenameConstraintOperation": func() ast.AlterOperation {
			return &ast.RenameConstraintOperation{From: "ck_a", To: "ck_b"}
		},
		"RenameIndexOperation": func() ast.AlterOperation {
			return &ast.RenameIndexOperation{From: "ix_a", To: "ix_b"}
		},
		"RenameTableOperation": func() ast.AlterOperation {
			return &ast.RenameTableOperation{NewName: "renamed"}
		},
		"ReplaceIndexOperation": func() ast.AlterOperation {
			return &ast.ReplaceIndexOperation{Index: &ast.IndexNode{Name: "ix_c", Columns: []string{"c"}}}
		},
		"ResetRowTTLOperation": func() ast.AlterOperation {
			return &ast.ResetRowTTLOperation{Parameters: []string{"ttl"}}
		},
		"SetCommentOperation": func() ast.AlterOperation {
			return &ast.SetCommentOperation{Comment: "orders placed online"}
		},
		"SetConstraintCommentOperation": func() ast.AlterOperation {
			return &ast.SetConstraintCommentOperation{Constraint: "ck_c", Comment: "positive"}
		},
		"SetIndexPartitioningOperation": func() ast.AlterOperation {
			return &ast.SetIndexPartitioningOperation{
				IndexName:    "ix_c",
				Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 2},
			}
		},
		"SetRowDeletionPolicyOperation": func() ast.AlterOperation {
			return &ast.SetRowDeletionPolicyOperation{Column: "created_at", Interval: "1 day"}
		},
		"SetRowTTLOperation": func() ast.AlterOperation {
			return &ast.SetRowTTLOperation{Options: []string{"ttl_expiration_expression = 'created_at + INTERVAL ''1 day'''"}}
		},
		"ValidateConstraintOperation": func() ast.AlterOperation {
			return &ast.ValidateConstraintOperation{ConstraintName: "ck_c"}
		},
	}
}

// visit sends node to a fresh renderer for dialect and returns its answer.
func visit(c *qt.C, dialect string, node ast.Node) error {
	c.Helper()
	r, err := renderer.NewRendererWithCapabilities(dialect, capability.ForDialect(dialect))
	c.Assert(err, qt.IsNil)
	return r.VisitNode(node)
}

// needsParent reports whether err is a renderer's answer to a fragment that
// arrived without the statement that carries it: the operation's own type,
// and the sentence every renderer ends that answer with.
func needsParent(err error, operation ast.AlterOperation) bool {
	message := fmt.Sprint(err)
	return strings.Contains(message, fmt.Sprintf("%T", operation)) &&
		strings.HasSuffix(message, "the statement that carries it, not on its own")
}

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

// withoutCarrierTable is message as a standalone operation phrases it: where
// the ALTER TABLE's refusal names its table, the operation's names "a table".
func withoutCarrierTable(message string) string {
	return strings.ReplaceAll(message, fmt.Sprintf("table %q", carrierTable), "a table")
}
