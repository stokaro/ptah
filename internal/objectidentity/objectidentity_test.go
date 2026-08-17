package objectidentity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
)

// postgresSemantics is the folding rule the four cited defects were all found
// under: an unquoted identifier folds to lower case, a quoted one does not.
func postgresSemantics() identifier.Semantics {
	return identifier.Semantics{
		DefaultSchema: "public",
		TableNames:    identifier.ComparisonASCIIInsensitive,
		ColumnNames:   identifier.ComparisonASCIIInsensitive,
		IndexNames:    identifier.ComparisonASCIIInsensitive,
	}
}

// TestIdentity_DefectFixtures is the regression set stokaro/ptah#1345 names.
// Each row is the collision one closed defect was: the identity model has to
// keep the two objects apart, and every one of these keys collapsed them.
func TestIdentity_DefectFixtures(t *testing.T) {
	builder := objectidentity.NewBuilder(postgresSemantics())

	tests := []struct {
		name  string
		left  objectidentity.ID
		right objectidentity.ID
	}{
		{
			// stokaro/ptah#1276: a policy name is scoped to its table, so the
			// same name on two tables is two policies. A key on the name alone
			// collapsed them.
			name:  "one policy name on two tables",
			left:  builder.Policy("public.orders", "tenant_isolation"),
			right: builder.Policy("public.invoices", "tenant_isolation"),
		},
		{
			// stokaro/ptah#1311: the fix for the row above joined table and
			// policy with a dot, which collides the moment either component
			// legitimately contains one.
			//
			// The components are BARE, which is how a catalog reports them: a
			// table named `orders.2024` arrives as those bytes with no quoting
			// to mark where the name begins. Written WITH quotes the two
			// spellings survive a joined key by accident -- the quotes stay in
			// the folded form and separate the halves -- so a quoted fixture
			// leaves the concatenation defect alive.
			name:  "a dot inside a component is not a separator",
			left:  builder.PolicyParts("public", "orders.2024", "p"),
			right: builder.PolicyParts("public", "orders", "2024.p"),
		},
		{
			// The quoted spelling of the same shape, which a schema file
			// carries. It is a separate row rather than a replacement: both
			// must hold, and only the bare one constrains the encoding.
			name:  "a dot inside a quoted component is not a separator",
			left:  builder.Policy(`public."orders.2024"`, "p"),
			right: builder.Policy("public.orders", `"2024".p`),
		},
		{
			// stokaro/ptah#1302: a domain compared without schema identity read
			// as absent, and the comparator planned a destructive drop for an
			// object that existed.
			name:  "one domain name in two schemas",
			left:  builder.Domain("public.email"),
			right: builder.Domain("billing.email"),
		},
		{
			// stokaro/ptah#1283: grants were keyed by a delimiter-joined
			// string, so two distinct grants collapsed. Two privileges on one
			// object for one role are two grants.
			name:  "one role and object with two privileges",
			left:  builder.Constraint("public.users", "SELECT"),
			right: builder.Constraint("public.users", "INSERT"),
		},
		{
			// The quoting axis: on a folding target these are two objects, and
			// a key that unquoted before folding made them one.
			name:  "a quoted name is not its folded spelling",
			left:  builder.Table(`public."Users"`),
			right: builder.Table("public.Users"),
		},
		{
			// The overload axis: two routines with one name and different
			// argument types are two objects.
			name:  "two overloads of one routine name",
			left:  builder.Function("public.f", "int"),
			right: builder.Function("public.f", "text"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.left.Equal(test.right), qt.IsFalse,
				qt.Commentf("%s and %s must be different objects", test.left, test.right))
			c.Assert(test.left.Key(), qt.Not(qt.Equals), test.right.Key())

			set := objectidentity.NewSet()
			_, firstCollided := set.Add(test.left)
			_, secondCollided := set.Add(test.right)
			c.Assert(firstCollided, qt.IsFalse)
			c.Assert(secondCollided, qt.IsFalse)
			c.Assert(set.Len(), qt.Equals, 2)
		})
	}
}

// TestIdentity_EqualityHoldsWhereItShould is the control for the table above. A
// model that answered "different" to everything would satisfy every row there
// and be useless, so these are the pairs that MUST collapse.
func TestIdentity_EqualityHoldsWhereItShould(t *testing.T) {
	builder := objectidentity.NewBuilder(postgresSemantics())

	tests := []struct {
		name  string
		left  objectidentity.ID
		right objectidentity.ID
	}{
		{
			name:  "an unqualified name takes the default schema",
			left:  builder.Table("users"),
			right: builder.Table("public.users"),
		},
		{
			name:  "an unquoted name folds on a folding target",
			left:  builder.Table("public.Users"),
			right: builder.Table("public.users"),
		},
		{
			name:  "the same policy on the same table",
			left:  builder.Policy("public.orders", "tenant_isolation"),
			right: builder.Policy("orders", "Tenant_Isolation"),
		},
		{
			name:  "a routine signature folds case and spacing",
			left:  builder.Function("public.f", "( INT , TEXT )"),
			right: builder.Function("public.f", "int,text"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.left.Equal(test.right), qt.IsTrue,
				qt.Commentf("%s and %s must be one object", test.left, test.right))

			set := objectidentity.NewSet()
			set.Add(test.left)
			_, collided := set.Add(test.right)
			c.Assert(collided, qt.IsTrue)
			c.Assert(set.Len(), qt.Equals, 1)
		})
	}
}

// TestIdentity_SourceSpellingSurvivesNormalization pins that the two values are
// distinct. A diagnostic quotes what the author wrote and a renderer emits it;
// keeping one value for both is how a rename that changes nothing gets planned.
func TestIdentity_SourceSpellingSurvivesNormalization(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())

	id := builder.Table("Billing.Orders")

	c.Assert(id.Schema.Source, qt.Equals, "Billing")
	c.Assert(id.Name.Source, qt.Equals, "Orders")
	c.Assert(id.Schema.Normalized, qt.Equals, "billing")
	c.Assert(id.Name.Normalized, qt.Equals, "orders")
	c.Assert(id.String(), qt.Equals, "table Billing.Orders")
}

// TestIdentity_IndexNamespaceDecidesTheParent pins that the index scope comes
// from the target's rule rather than from this package: where index names are
// unique per schema, two tables cannot carry the same index name, and an
// identity that kept the table would call them different objects.
func TestIdentity_IndexNamespaceDecidesTheParent(t *testing.T) {
	tests := []struct {
		name      string
		namespace identifier.IndexNamespace
		wantEqual bool
	}{
		{name: "table-scoped index names differ per table", namespace: identifier.IndexNamespaceTable},
		{name: "schema-scoped index names collide across tables", namespace: identifier.IndexNamespaceSchema, wantEqual: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := postgresSemantics()
			semantics.IndexNamespace = test.namespace
			builder := objectidentity.NewBuilder(semantics)

			left := builder.Index("public.orders", "idx_created")
			right := builder.Index("public.invoices", "idx_created")

			c.Assert(left.Equal(right), qt.Equals, test.wantEqual)
		})
	}
}

// TestIdentity_ExactSemanticsDoNotFold is the other side of the folding axis: a
// target that compares identifiers exactly must not have its spellings folded
// away, which is what a hard-coded strings.ToLower in a per-family key did.
func TestIdentity_ExactSemanticsDoNotFold(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.Semantics{
		DefaultSchema: "main",
		TableNames:    identifier.ComparisonExact,
		ColumnNames:   identifier.ComparisonExact,
	})

	c.Assert(builder.Table("main.Users").Equal(builder.Table("main.users")), qt.IsFalse)
}

// TestIdentity_PartsAreNotRejoinedAndResplit pins the invariant that separates
// the two ways to build a table identity.
//
// A caller holding a schema and a name as two values must use the Parts form.
// Rejoining them with a dot and re-splitting is lossy: the unqualified table
// whose own name contains a dot, written `"tenant.data"`, comes back as schema
// `"tenant` and name `data"`, which is a different object. The comparator hit
// exactly this when it routed a two-component caller through the one-string
// constructor, and two distinct tables collapsed into one.
func TestIdentity_PartsAreNotRejoinedAndResplit(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())

	literalDot := builder.TableParts("", "tenant.data")
	qualified := builder.TableParts("tenant", "data")

	c.Assert(literalDot.Equal(qualified), qt.IsFalse,
		qt.Commentf("%s and %s are two tables", literalDot, qualified))
	c.Assert(literalDot.Name.Source, qt.Equals, "tenant.data")
	c.Assert(literalDot.Schema.Normalized, qt.Equals, "public")
	c.Assert(qualified.Name.Source, qt.Equals, "data")
	// The component is bare on purpose. Written `"tenant.data"` the quotes
	// survive a rejoin -- splitting is quote-aware -- so a quoted fixture
	// leaves the rejoin defect alive. A catalog reports the name unquoted, and
	// then nothing marks where it begins.
	c.Assert(builder.Table(`"tenant.data"`).Name.Source, qt.Equals, `"tenant.data"`)
}

// TestIdentity_ColumnPartsCarriesTheSameOwner is the column half of the rule
// above: a column on `"tenant.data"` belongs to that table and not to a table
// `data` in a schema `tenant`.
func TestIdentity_ColumnPartsCarriesTheSameOwner(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())

	literalDot := builder.ColumnParts("", `"tenant.data"`, "id")
	qualified := builder.ColumnParts("tenant", "data", "id")

	c.Assert(literalDot.Equal(qualified), qt.IsFalse)
	c.Assert(literalDot.Parent.Source, qt.Equals, `"tenant.data"`)
}

// TestIdentity_SchemaScopedPartsSeparatesFamilies pins that a sequence and a
// table sharing one schema-qualified name are two objects.
//
// Keeping the kind is what lets one map hold both. Every family that instead
// reuses the table key relies on living in a separate map, and a family that
// later shares one silently merges.
func TestIdentity_SchemaScopedPartsSeparatesFamilies(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())

	sequence := builder.SchemaScopedParts(objectidentity.KindSequence, "public", "orders")
	table := builder.TableParts("public", "orders")

	c.Assert(sequence.Equal(table), qt.IsFalse)
	c.Assert(sequence.Key().Kind(), qt.Equals, objectidentity.KindSequence)
	c.Assert(table.Key().Kind(), qt.Equals, objectidentity.KindTable)
}

// TestIdentity_ConstraintPartsVerbatimFoldsNothing pins the constructor the
// planner uses.
//
// Both spellings it pairs arrive from one diff, already normalized by the
// comparator that produced it. Folding again there would apply the rule twice
// on one side of the pipeline and once on the other, so this constructor must
// leave both components exactly as handed over -- including whitespace, which
// a fold-and-trim constructor would absorb.
func TestIdentity_ConstraintPartsVerbatimFoldsNothing(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())

	upper := builder.ConstraintPartsVerbatim("Orders", "FK_Customer")
	lower := builder.ConstraintPartsVerbatim("orders", "fk_customer")
	padded := builder.ConstraintPartsVerbatim(" orders ", "fk_customer")

	c.Assert(upper.Equal(lower), qt.IsFalse)
	c.Assert(padded.Equal(lower), qt.IsFalse)
	c.Assert(upper.Name.Source, qt.Equals, "FK_Customer")
}
