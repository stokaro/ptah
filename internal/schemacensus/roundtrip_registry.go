package schemacensus

import "slices"

// RecordedLoss is a set of census fixtures whose round trip is known to plan
// a change, and why: every fixture, on every dialect, in every direction
// named. Directions nil means both.
//
// It is a ratchet entry, not a description. The gate requires the measured
// losses to be exactly the recorded ones, so a new loss fails, and so does a
// repaired one until its entry is removed.
type RecordedLoss struct {
	Fixtures   []string
	Dialects   []string
	Directions []Direction
	Reason     string
}

// Unreached is a compared field a change of which does not reach the plan,
// on the named dialects and directions, and why.
//
// Dialects nil means the dialect-free requirement of a [Comparison] field.
// Directions nil means both. [RoundTripGaps] holds the entries to repair or
// to prove deliberate, and [RoundTripExemptions] the ones that are right; the
// gate requires the measured set to be exactly their union.
type Unreached struct {
	Field      string
	Dialects   []string
	Directions []Direction
	Reason     string
}

// UnmeasuredEntry is a compared field the round trip cannot ask about, and
// why that is acceptable for now.
type UnmeasuredEntry struct {
	Field  string
	Reason string
}

// The reasons a fixture's round trip is known to plan a change. Each one is a
// false migration a user of `ptah schema diff` would be shown.
const (
	lossViewAttributes    = "a view's CHECK OPTION and comment do not survive goschematodb, so the view is replaced"
	lossPrimaryKeyInclude = "a primary key's INCLUDE columns do not survive goschematodb, so the key is dropped and added again"
	lossPrimaryKeyFacets  = "a primary key's KEY_BLOCK_SIZE and COMMENT do not survive goschematodb, so the key is added again"
	lossIndexParser       = "a FULLTEXT index's parser does not survive goschematodb, so the index is dropped and added again"
	lossOperatorClass     = "a GIN index's operator class does not survive goschematodb, so the index is dropped and created again"
	lossRefreshCoverage   = "goschematodb leaves a refreshable materialized view's schedule marked as not inspected, so the comparison is refused as incomplete"
	lossClickHouseEnum    = "a standalone enum reaches the ClickHouse comparison, which has only inline Enum8 and Enum16 columns"
	lossRoutineAttributes = "goschematodb drops LEAKPROOF, STRICT and PARALLEL; stokaro/ptah#4140 step 8.3c moves them to " +
		"PostgreSQL facets, which the conversion already carries"
	lossRoutineKind           = "goschematodb drops a routine's kind, so a procedure reads as a function and the plan creates the procedure beside a DROP FUNCTION"
	lossRoutinePublicExecute  = "dbschematogo drops the EXECUTE that PUBLIC holds on a procedure, so the converted-back declaration revokes it"
	lossRoutinesWithoutTarget = "the comparison lists a routine on a target that has none, so the plan carries notes about it or is refused"
)

// The reasons a compared field is known not to reach the plan.
const (
	gapToCatalog         = "goschematodb drops it: the catalog is the same with and without it"
	gapFromCatalog       = "dbschematogo drops it: the converted-back declaration is the same with and without it"
	gapComparison        = "both conversions carry it, and the comparison plans nothing when it changes"
	gapRoutineAttributes = "goschematodb drops it; stokaro/ptah#4140 step 8.3c moves it to a PostgreSQL facet"
	gapRoutineKind       = "the conversions drop a routine's kind; the other direction is a recorded loss"
	gapPrimaryKeyInclude = "goschematodb drops a primary key's INCLUDE columns; the other direction is a recorded loss"
	gapViewAttributes    = "goschematodb drops a view's attributes; the other direction is a recorded loss"
)

// The reasons a compared field does not reach the plan by design.
const (
	exemptCoupledGranularity = "a granularity's state changes only with its value: an explicit one carries a value and no " +
		"other state may, so no single change of the state is valid. The value beside it is reached"
	exemptCoupledSetting = "a setting's state changes only with its value: an explicit one carries a value and no " +
		"other state may, so no single change of the state is valid. The value beside it is reached"
	exemptCoupledRoleSelection = "TO ALL changes only with its exceptions: exceptions stand only beside TO ALL, and the " +
		"fixture that sets it names one. The selection's names and exceptions are reached"
	exemptCoupledPredicateType = "the fixture's other predicate is a BLOCK for every operation, and a second BLOCK beside " +
		"it is refused, so no single change of the type is valid there. The predicate's other fields are reached"
	exemptCoupledConsistency = "the fixture declares a commit interval, which belongs to the global level alone, so no " +
		"other level is valid beside it. The commit interval is reached"
	exemptCoupledChangefeedFormat = "the fixture's changefeed asks for virtual timestamps, which YDB writes only in JSON, " +
		"so no other format is valid beside them. The timestamps are reached"
	exemptChangefeedIdentity = "repeats the changefeed's identity, which its subject key holds; a definition that " +
		"disagrees with its key is refused, and a changefeed under another key is another object"
	exemptRoleKeyword = "a role keyword such as CURRENT_USER has no observation until a server resolves it, and the " +
		"conversion refuses it; a connected comparison reads the server's resolution from pgpolicy.NormalizedPolicy.Roles"
	exemptClickHouseKey = "every fixture's only key column is its primary one, and a ClickHouse MergeTree table " +
		"without a sorting key is refused"
	exemptFeatureCoverage = "it is what a feature comparison may conclude from: without it the comparison refuses the " +
		"objects as not inspected, which is the comparison reading it"
	exemptNotDescribed = "records what a description does not describe, which turns an absence on one side into no " +
		"conclusion. A schema compared with its own conversion holds the same objects on both sides, so no change of it " +
		"can plan something; migration/schemadiff's coverage tests compare it with a read that disagrees"
	exemptSharedRowSecurity = "a shared row-level security declaration reaches no catalog: each target converts it to " +
		"its owner's model or skips it, so only the dialect-free comparison of two declarations reads it"
	exemptClearedAttributes = "a cleared range attribute converts to the empty attribute an absent one converts to, so " +
		"it differs only from a current range that holds the attribute; compare.TestRanges_AttributeChangesAreReported measures that"
	exemptDomainNormalized = "an offline comparison leaves a domain's CHECK and DEFAULT create-only and compares them " +
		"only once a server has normalized them"
	exemptIfNotExists = "IF NOT EXISTS says how a statement behaves when the object exists; no catalog records it"
	exemptCustomSQL   = "statements the author appends verbatim; no catalog describes them, so a comparison has " +
		"nothing to hold them against"
	exemptRoleSelection = "removing the selection either changes nothing, on a target the role is selected for, or puts " +
		"on the converted side a role the declaration does not name, and the comparison never removes such a role " +
		"(compare.TestRolesComparison)"
	exemptGrantSelection = "removing the selection puts on the converted side a grant to a role the declaration does " +
		"not manage on that target, and the comparison removes grants only from the roles it manages"
	exemptPassword    = "a catalog holds no readable password, so a declaration converted back from one has none"
	exemptSecretValue = "names the environment variable a secret's value is read from when it is applied; YDB never " +
		"shows a secret's value"
	exemptStateReset = "permits a plan to reset a streaming query's state when its body changes; the permission is " +
		"not stored in the database"
	exemptWholeServer = "a MySQL-family schema is a database, and only a whole-server comparison compares databases " +
		"(stokaro/ptah#3789); this comparison reads one database"
	exemptVirtualTable = "the comparison refuses any change of a SQLite virtual table, because rebuilding it destroys " +
		"the module's storage (stokaro/ptah#1028); the refusal names the table, which is the comparison reading it"
	exemptForeignDefiner = "a routine that states no security mode is a definer routine here, a definer routine is " +
		"replaced only beside a recorded definer and connected account, and a description converted from a catalog " +
		"records neither, so the comparison refuses the replacement and names the routine"
)

// RoundTripUnmeasured is every compared field the round trip cannot ask
// about for a reason other than a recorded loss, and why.
func RoundTripUnmeasured() []UnmeasuredEntry {
	const unknownFacet = "declared only by the census's unknown-facet fixture, which every target refuses"
	const noServerSpelling = "no census fixture declares a server's spelling; a connected comparison attaches one"
	return []UnmeasuredEntry{
		{Field: "mssqlschema.DesiredSecurityPolicy.Normalized", Reason: noServerSpelling},
		{Field: "schemamodel.Database.Facets", Reason: unknownFacet},
		{Field: "schemamodel.Enum.Facets", Reason: unknownFacet},
		{Field: "schemamodel.Field.UniqueExpr", Reason: "every target refuses it: uniqueness over an expression is not implemented"},
		{Field: "schemamodel.Schema.Facets", Reason: unknownFacet},
		{Field: "schemamodel.Sequence.Facets", Reason: unknownFacet},
		{Field: "tsschema.DesiredContinuousAggregate.Normalized", Reason: noServerSpelling},
		{Field: "tsschema.NormalizedBody.Body", Reason: noServerSpelling},
		{Field: "ydbschema.ChangefeedSpec.Disabled", Reason: "every target refuses it: YDB has no statement that disables a changefeed"},
		{Field: "ydbschema.ColumnFamily.KeepInMemory", Reason: "only a read sets it; no declaration carries it"},
	}
}

// alternates holds valid values for each field that removing or perturbing
// cannot change without a refusal, so the probe can change it and still be
// compared. A refusal proves the conversion validates the field; it does not
// prove the comparison reads it.
//
// The n-th instance of the field a fixture declares takes the n-th value,
// cycling, so a field whose instances must differ -- the tiers of one TTL --
// stays valid.
func alternates() map[string][]any {
	return map[string][]any{
		// Removing a ClickHouse index's type leaves an index the comparison
		// refuses as not inspected; another type is a change it can plan.
		"schemamodel.Index.Overrides": {map[string]map[string]string{"clickhouse": {"type": "minmax", "granularity": "4"}}},
		// Another host string names no table, so the index moves to the other
		// table of the two-table fixtures instead.
		"schemamodel.Index.StructName":            {"A"},
		"schemamodel.Index.TableName":             {"a"},
		"spannerschema.Policy.Interval":           {"7 days"},
		"ydbschema.ChangefeedSpec.Mode":           {"UPDATES"},
		"ydbschema.DesiredVectorIndex.Distance":   {"euclidean"},
		"ydbschema.DesiredVectorIndex.Similarity": {"cosine"},
		"ydbschema.DesiredVectorIndex.VectorType": {"uint8"},
		"ydbschema.TTL.Interval":                  {"PT12H"},
		"ydbschema.TTLTier.Interval":              {"PT12H", "P3D"},
		"ydbtopic.Spec.AutoPartitioningStrategy":  {"scale_up_and_down"},
	}
}

// Alternates is every field with alternate values, sorted, for the gate that
// checks each names a field and fits its type.
func Alternates() []string {
	names := make([]string, 0, len(alternates()))
	for name := range alternates() {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// RoundTripLosses is every fixture whose round trip is known to plan a change.
func RoundTripLosses() []RecordedLoss { return slices.Clone(roundTripLosses) }

var roundTripLosses = []RecordedLoss{
	{
		Fixtures:   []string{"view"},
		Dialects:   []string{"cockroachdb", "postgres", "sqlserver", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     lossViewAttributes,
	},
	{
		Fixtures:   []string{"table-pk-include"},
		Dialects:   []string{"postgres", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     lossPrimaryKeyInclude,
	},
	{
		Fixtures:   []string{"constraint-pk-options"},
		Dialects:   []string{"mariadb", "mysql"},
		Directions: []Direction{ToCatalog},
		Reason:     lossPrimaryKeyFacets,
	},
	{Fixtures: []string{"index-mysql-parser"}, Dialects: []string{"mariadb", "mysql"}, Directions: []Direction{ToCatalog}, Reason: lossIndexParser},
	{Fixtures: []string{"index-fulltext"}, Dialects: []string{"postgres"}, Directions: []Direction{ToCatalog}, Reason: lossOperatorClass},
	{Fixtures: []string{"matview-refresh"}, Dialects: []string{"clickhouse"}, Directions: nil, Reason: lossRefreshCoverage},
	{Fixtures: []string{"enum"}, Dialects: []string{"clickhouse"}, Directions: []Direction{ToCatalog}, Reason: lossClickHouseEnum},
	{
		Fixtures:   []string{"function-planner-properties"},
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     lossRoutineAttributes,
	},
	{
		Fixtures:   []string{"function-procedure"},
		Dialects:   []string{"cockroachdb", "mariadb", "mysql", "oracle", "postgres", "sqlserver", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     lossRoutineKind,
	},
	{
		Fixtures:   []string{"grant-routine"},
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     lossRoutineKind,
	},
	{Fixtures: []string{"grant-routine"}, Dialects: []string{"postgres"}, Directions: []Direction{FromCatalog}, Reason: lossRoutinePublicExecute},
	{
		Fixtures:   []string{"function-procedure"},
		Dialects:   []string{"clickhouse", "spanner", "sqlite"},
		Directions: []Direction{ToCatalog},
		Reason:     lossRoutinesWithoutTarget,
	},
	{
		Fixtures:   []string{"grant-routine"},
		Dialects:   []string{"spanner", "sqlite"},
		Directions: []Direction{ToCatalog},
		Reason:     lossRoutinesWithoutTarget,
	},
}

// RoundTripGaps is every compared field a conversion or the comparison is
// known to drop.
func RoundTripGaps() []Unreached { return slices.Clone(roundTripGaps) }

var roundTripGaps = []Unreached{
	{Field: "mssqlproperty.Property.Column", Dialects: []string{"sqlserver"}, Directions: nil, Reason: gapComparison},
	{Field: "mssqlproperty.Property.Name", Dialects: []string{"sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "mssqlproperty.Property.Schema", Dialects: []string{"sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "mssqlproperty.Property.Table", Dialects: []string{"sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "mysqlschema.DesiredColumnSettings.Charset", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapToCatalog},
	{Field: "mysqlschema.DesiredColumnSettings.OnUpdate", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapToCatalog},
	{Field: "mysqlschema.DesiredIndex.Parser", Dialects: []string{"mariadb", "mysql"}, Directions: []Direction{FromCatalog}, Reason: gapComparison},
	{Field: "mysqlschema.DesiredIndexBlockSize.KeyBlockSize", Dialects: []string{"mysql"}, Directions: nil, Reason: gapComparison},
	{Field: "mysqlschema.DesiredTable.AutoIncrement", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapToCatalog},
	{Field: "mysqlschema.DesiredTable.Charset", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapComparison},
	{Field: "mysqlschema.DesiredTable.Engine", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapToCatalog},
	{
		Field:      "schemamodel.CompositeType.Dialects",
		Dialects:   []string{"clickhouse", "mariadb", "mysql", "oracle", "spanner", "sqlite", "sqlserver", "ydb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Constraint.Facets", Dialects: []string{"mysql"}, Directions: []Direction{FromCatalog}, Reason: gapComparison},
	{Field: "schemamodel.Constraint.NotValid", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Database.Schemas",
		Dialects:   []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "postgres", "spanner", "sqlserver", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{
		Field:      "schemamodel.Domain.Dialects",
		Dialects:   []string{"clickhouse", "mariadb", "mysql", "oracle", "spanner", "sqlite", "sqlserver", "ydb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "schemamodel.EmbeddedField.Overrides", Dialects: []string{"mysql"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.EmbeddedField.Type", Dialects: []string{"cockroachdb", "sqlite"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Enum.Schema", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Extension.Dialects", Dialects: []string{"spanner", "ydb"}, Directions: []Direction{ToCatalog}, Reason: gapComparison},
	{
		Field:      "schemamodel.Field.AutoInc",
		Dialects:   []string{"mariadb", "mysql", "oracle", "sqlite", "sqlserver"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Field.Collate", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Field.DefaultSet",
		Dialects:   []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlite", "sqlserver", "ydb", "yugabytedb"},
		Directions: []Direction{FromCatalog},
		Reason:     gapFromCatalog,
	},
	{
		Field:      "schemamodel.Field.DefaultSet",
		Dialects:   []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlite", "sqlserver", "ydb", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Field.Facets", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Field.GeneratedExpression", Dialects: []string{"oracle"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Field.IdentityGeneration",
		Dialects:   []string{"cockroachdb", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlserver", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Field.IdentityIncrement", Dialects: []string{"sqlserver"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Field.IdentityOptions",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{Field: "schemamodel.Field.IdentityStart", Dialects: []string{"sqlserver"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Field.Overrides", Dialects: []string{"mysql"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Field.TypeIsDeclaredText", Dialects: []string{"clickhouse", "sqlite", "sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Field.TypeRawSQL", Dialects: []string{"clickhouse", "sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Function.Dialects", Dialects: []string{"clickhouse", "oracle", "spanner", "sqlite"}, Directions: nil, Reason: gapToCatalog},
	{
		Field:      "schemamodel.Function.Dialects",
		Dialects:   []string{"cockroachdb", "mariadb", "mysql", "sqlserver", "ydb", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{
		Field:      "schemamodel.Function.Kind",
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: []Direction{FromCatalog},
		Reason:     gapRoutineKind,
	},
	{
		Field:      "schemamodel.Function.Kind",
		Dialects:   []string{"mariadb", "mysql", "sqlserver"},
		Directions: []Direction{FromCatalog},
		Reason:     gapRoutineKind,
	},
	{
		Field:      "schemamodel.Function.Leakproof",
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: []Direction{FromCatalog},
		Reason:     gapRoutineAttributes,
	},
	{
		Field:      "schemamodel.Function.Parallel",
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: []Direction{FromCatalog},
		Reason:     gapRoutineAttributes,
	},
	{
		Field:      "schemamodel.Function.Strict",
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: []Direction{FromCatalog},
		Reason:     gapRoutineAttributes,
	},
	{
		Field:      "schemamodel.Index.Comment",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Index.Facets", Dialects: []string{"mysql"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Index.Fields",
		Dialects:   []string{"clickhouse", "cockroachdb", "oracle", "spanner", "sqlite", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Index.Operator", Dialects: []string{"cockroachdb", "spanner", "yugabytedb"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Index.Operator", Dialects: []string{"postgres"}, Directions: []Direction{FromCatalog}, Reason: gapToCatalog},
	{
		Field:      "schemamodel.Index.Parts",
		Dialects:   []string{"clickhouse", "cockroachdb", "oracle", "spanner", "sqlite", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Index.StorageParams", Dialects: []string{"cockroachdb", "spanner", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Index.Type", Dialects: []string{"cockroachdb", "spanner", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Index.Type", Dialects: []string{"mariadb", "mysql"}, Directions: []Direction{FromCatalog}, Reason: gapToCatalog},
	{
		Field:      "schemamodel.Index.Unique",
		Dialects:   []string{"cockroachdb", "oracle", "spanner", "sqlite", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{
		Field:      "schemamodel.IndexPart.Desc",
		Dialects:   []string{"cockroachdb", "mariadb", "mysql", "oracle", "spanner", "sqlite", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{
		Field:      "schemamodel.IndexPart.Expr",
		Dialects:   []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "oracle", "spanner", "sqlite", "sqlserver", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{
		Field:      "schemamodel.IndexPart.Name",
		Dialects:   []string{"clickhouse", "cockroachdb", "oracle", "spanner", "sqlite", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{Field: "schemamodel.IndexPart.NullsOrder", Dialects: []string{"cockroachdb", "spanner", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.IndexPart.Operator", Dialects: []string{"cockroachdb", "spanner", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.IndexPart.Prefix", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.MaterializedView.Dialects",
		Dialects:   []string{"clickhouse", "mariadb", "mysql", "oracle", "sqlite", "sqlserver", "ydb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "schemamodel.MaterializedView.Dialects", Dialects: []string{"spanner"}, Directions: nil, Reason: gapToCatalog},
	{
		Field:      "schemamodel.PartitionPart.Expr",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{
		Field:      "schemamodel.PartitionPart.Name",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{
		Field:      "schemamodel.PartitionSpec.Parts",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{
		Field:      "schemamodel.PartitionSpec.Type",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{Field: "schemamodel.PrimaryKeyPart.Desc", Dialects: []string{"mariadb", "mysql", "sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.PrimaryKeyPart.Name", Dialects: []string{"clickhouse"}, Directions: []Direction{ToCatalog}, Reason: gapComparison},
	{
		Field:      "schemamodel.PrimaryKeyPart.Name",
		Dialects:   []string{"cockroachdb", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlite", "sqlserver", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{Field: "schemamodel.PrimaryKeyPart.Prefix", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapToCatalog},
	{
		Field:      "schemamodel.Range.Dialects",
		Dialects:   []string{"clickhouse", "mariadb", "mysql", "oracle", "spanner", "sqlite", "sqlserver", "ydb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Role.Comment", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Schema.Comment", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Schema.Name",
		Dialects:   []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "postgres", "spanner", "sqlserver", "yugabytedb"},
		Directions: nil,
		Reason:     gapComparison,
	},
	{
		Field:      "schemamodel.Sequence.Dialects",
		Dialects:   []string{"cockroachdb", "mariadb", "oracle", "spanner", "sqlserver", "ydb", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Table.Collate", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Table.Engine", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Table.Facets", Dialects: []string{"mariadb", "mysql", "sqlite"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Table.Overrides", Dialects: []string{"mysql"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Table.Partition",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{Field: "schemamodel.Table.PrimaryKeyFacets", Dialects: []string{"mysql"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Table.PrimaryKeyInclude",
		Dialects:   []string{"postgres", "yugabytedb"},
		Directions: []Direction{FromCatalog},
		Reason:     gapPrimaryKeyInclude,
	},
	{Field: "schemamodel.Table.PrimaryKeyMethod", Dialects: []string{"mysql"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "schemamodel.Table.PrimaryKeyName",
		Dialects:   []string{"cockroachdb", "oracle", "postgres", "spanner", "sqlserver", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
	{Field: "schemamodel.Table.PrimaryKeyParts", Dialects: []string{"mariadb", "mysql", "sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.Table.Unlogged", Dialects: []string{"postgres", "yugabytedb"}, Directions: nil, Reason: gapComparison},
	{Field: "schemamodel.Trigger.Dialects", Dialects: []string{"clickhouse", "spanner"}, Directions: nil, Reason: gapToCatalog},
	{
		Field:      "schemamodel.Trigger.Dialects",
		Dialects:   []string{"cockroachdb", "mariadb", "mysql", "oracle", "sqlite", "sqlserver", "ydb", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "schemamodel.Trigger.ExecuteFunction", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: gapToCatalog},
	{Field: "schemamodel.View.Attributes", Dialects: []string{"sqlserver"}, Directions: []Direction{FromCatalog}, Reason: gapViewAttributes},
	{
		Field:      "schemamodel.View.Dialects",
		Dialects:   []string{"clickhouse", "mariadb", "mysql", "oracle", "spanner", "sqlite", "ydb"},
		Directions: []Direction{ToCatalog},
		Reason:     gapComparison,
	},
	{Field: "sqlitetable.DesiredTable.Options", Dialects: []string{"sqlite"}, Directions: nil, Reason: gapComparison},
	{Field: "sqlitetable.Options.Strict", Dialects: []string{"sqlite"}, Directions: nil, Reason: gapComparison},
	{Field: "sqlitetable.Options.WithoutRowID", Dialects: []string{"sqlite"}, Directions: nil, Reason: gapComparison},
	{Field: "synonym.Synonym.Name", Dialects: []string{"oracle", "sqlserver"}, Directions: nil, Reason: gapToCatalog},
	{Field: "synonym.Synonym.Schema", Dialects: []string{"oracle", "sqlserver"}, Directions: nil, Reason: gapComparison},
	{
		Field:      "tsschema.DesiredContinuousAggregate.Body",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     gapToCatalog,
	},
}

// RoundTripExemptions is every compared field that does not reach a plan by
// design, with the reason.
func RoundTripExemptions() []Unreached { return slices.Clone(roundTripExemptions) }

var roundTripExemptions = []Unreached{
	{Field: "chschema.GranularitySetting.State", Dialects: []string{"clickhouse"}, Directions: nil, Reason: exemptCoupledGranularity},
	{Field: "chschema.RoleSelection.All", Dialects: []string{"clickhouse"}, Directions: nil, Reason: exemptCoupledRoleSelection},
	{Field: "chschema.Setting.State", Dialects: []string{"clickhouse"}, Directions: nil, Reason: exemptCoupledSetting},
	{Field: "coverage.Object.Kind", Dialects: nil, Directions: nil, Reason: exemptNotDescribed},
	{Field: "coverage.Object.Name", Dialects: nil, Directions: nil, Reason: exemptNotDescribed},
	{Field: "coverage.Object.Provenance", Dialects: nil, Directions: nil, Reason: exemptNotDescribed},
	{Field: "coverage.Object.Reason", Dialects: nil, Directions: nil, Reason: exemptNotDescribed},
	{Field: "coverage.Set.Objects", Dialects: nil, Directions: nil, Reason: exemptNotDescribed},
	{Field: "mssqlschema.Predicate.Type", Dialects: []string{"sqlserver"}, Directions: nil, Reason: exemptCoupledPredicateType},
	{Field: "pgextension.DesiredExtension.IfNotExists", Dialects: []string{"postgres", "yugabytedb"}, Directions: nil, Reason: exemptIfNotExists},
	{Field: "pgpolicy.RoleSelector.Keyword", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: exemptRoleKeyword},
	{Field: "schemamodel.Database.FeatureCoverage", Dialects: nil, Directions: nil, Reason: exemptFeatureCoverage},
	{Field: "schemamodel.Database.NotDescribed", Dialects: nil, Directions: nil, Reason: exemptNotDescribed},
	{Field: "schemamodel.Domain.Check", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: exemptDomainNormalized},
	{Field: "schemamodel.Domain.Default", Dialects: []string{"cockroachdb", "postgres", "yugabytedb"}, Directions: nil, Reason: exemptDomainNormalized},
	{
		Field:      "schemamodel.Domain.DefaultExpr",
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: nil,
		Reason:     exemptDomainNormalized,
	},
	{Field: "schemamodel.Extension.IfNotExists", Dialects: []string{"postgres", "yugabytedb"}, Directions: nil, Reason: exemptIfNotExists},
	{Field: "schemamodel.Field.Primary", Dialects: []string{"clickhouse"}, Directions: nil, Reason: exemptClickHouseKey},
	{Field: "schemamodel.Function.Body", Dialects: []string{"mariadb", "mysql"}, Directions: []Direction{FromCatalog}, Reason: exemptForeignDefiner},
	{
		Field:      "schemamodel.Grant.Dialects",
		Dialects:   []string{"clickhouse", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlite", "sqlserver"},
		Directions: nil,
		Reason:     exemptGrantSelection,
	},
	{
		Field:      "schemamodel.Grant.Dialects",
		Dialects:   []string{"cockroachdb", "yugabytedb"},
		Directions: []Direction{ToCatalog},
		Reason:     exemptGrantSelection,
	},
	{Field: "schemamodel.RLSEnabledTable.Forced", Dialects: nil, Directions: nil, Reason: exemptSharedRowSecurity},
	{Field: "schemamodel.RLSPolicy.Comment", Dialects: nil, Directions: nil, Reason: exemptSharedRowSecurity},
	{Field: "schemamodel.Range.ClearedAttributes", Dialects: nil, Directions: nil, Reason: exemptClearedAttributes},
	{
		Field:      "schemamodel.Role.Dialects",
		Dialects:   []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlite", "sqlserver", "ydb", "yugabytedb"},
		Directions: nil,
		Reason:     exemptRoleSelection,
	},
	{
		Field:      "schemamodel.Role.Password",
		Dialects:   []string{"cockroachdb", "postgres", "yugabytedb"},
		Directions: []Direction{FromCatalog},
		Reason:     exemptPassword,
	},
	{Field: "schemamodel.Schema.Charset", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: exemptWholeServer},
	{Field: "schemamodel.Schema.Collate", Dialects: []string{"mariadb", "mysql"}, Directions: nil, Reason: exemptWholeServer},
	{
		Field:      "schemamodel.Sequence.IfNotExists",
		Dialects:   []string{"cockroachdb", "mariadb", "oracle", "postgres", "sqlserver", "yugabytedb"},
		Directions: nil,
		Reason:     exemptIfNotExists,
	},
	{
		Field:      "schemamodel.Table.CustomSQL",
		Dialects:   []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlite", "sqlserver", "ydb", "yugabytedb"},
		Directions: nil,
		Reason:     exemptCustomSQL,
	},
	{Field: "sqlitetable.DesiredVirtual.Virtual", Dialects: []string{"sqlite"}, Directions: nil, Reason: exemptVirtualTable},
	{Field: "sqlitetable.Virtual.Arguments", Dialects: []string{"sqlite"}, Directions: nil, Reason: exemptVirtualTable},
	{Field: "sqlitetable.Virtual.Module", Dialects: []string{"sqlite"}, Directions: nil, Reason: exemptVirtualTable},
	{
		Field:      "tsschema.DesiredHypertable.IfNotExists",
		Dialects:   []string{"cockroachdb", "postgres", "spanner", "yugabytedb"},
		Directions: nil,
		Reason:     exemptIfNotExists,
	},
	{Field: "ydbreplication.ReplicationSpec.ConsistencyLevel", Dialects: []string{"ydb"}, Directions: nil, Reason: exemptCoupledConsistency},
	{Field: "ydbschema.ChangefeedSpec.Format", Dialects: []string{"ydb"}, Directions: nil, Reason: exemptCoupledChangefeedFormat},
	{Field: "ydbschema.ChangefeedSpec.Name", Dialects: []string{"ydb"}, Directions: nil, Reason: exemptChangefeedIdentity},
	{Field: "ydbsecret.Desired.ValueEnv", Dialects: []string{"ydb"}, Directions: nil, Reason: exemptSecretValue},
	{Field: "ydbstreaming.Desired.AllowStateReset", Dialects: []string{"ydb"}, Directions: nil, Reason: exemptStateReset},
}
