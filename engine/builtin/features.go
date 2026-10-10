package builtin

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/dialect/mysql/mysqlrender"
	"ptah.run/dialect/spanner/spannerrender"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/dialect/timescaledb/tsrender"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyrender"
	"ptah.run/feature/synonym"
	"ptah.run/internal/pgpolicyprovider"
	"ptah.run/internal/ydbextensions"
)

// This is built-in composition. Feature-specific shape and support decisions
// belong to the selected owner, including refusal of unrelated payload kinds.
func validateNamedFeatures(dialect string, caps capability.Capabilities, objects schemaext.Objects) error {
	if objects.IsZero() {
		return nil
	}
	if platform.NormalizeDialect(dialect) == platform.YDB {
		return ydbextensions.ValidateObjects(dialect, caps, objects)
	}
	if platform.IsPostgresFamily(dialect) {
		return validatePostgresFamilyObjects(dialect, objects)
	}
	if platform.NormalizeDialect(dialect) == platform.ClickHouse {
		return validateRowPolicyObjects(dialect, objects)
	}
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
		return validateSecurityPolicies(dialect, objects)
	}
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		return validateSynonyms(dialect, objects)
	}
	return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
}

// validateSynonyms accepts the synonyms the synonym owner plans, Oracle's
// only owner, and refuses every other named object.
func validateSynonyms(dialect string, objects schemaext.Objects) error {
	all, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range all {
		value, ok := object.Value.(*synonym.DesiredSynonym)
		if !ok {
			return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
		}
		if err := synonym.ValidateDesired(value); err != nil {
			return err
		}
	}
	return nil
}

// validateSecurityPolicies accepts the security policies, the extended
// properties and the synonyms the SQL Server owners plan, and refuses every
// other named object.
func validateSecurityPolicies(dialect string, objects schemaext.Objects) error {
	all, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range all {
		switch value := object.Value.(type) {
		case *mssqlschema.DesiredSecurityPolicy:
			if _, err := mssqlschema.DesiredSecurityPolicyObject(object.Ref, *value); err != nil {
				return err
			}
		case *mssqlproperty.DesiredProperty:
			if err := mssqlproperty.ValidateDesired(value); err != nil {
				return err
			}
		case *synonym.DesiredSynonym:
			if err := synonym.ValidateDesired(value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
		}
	}
	return nil
}

// validateRowPolicyObjects accepts the row policies a ClickHouse table
// declares, which the renderer writes after the table's CREATE TABLE, and
// refuses every other named object.
func validateRowPolicyObjects(dialect string, objects schemaext.Objects) error {
	all, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range all {
		value, ok := object.Value.(*chschema.DesiredRowPolicy)
		if !ok {
			return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
		}
		if err := chschema.ValidateRowPolicyRef(object.Ref); err != nil {
			return err
		}
		if err := chschema.ValidateDesiredRowPolicy(value); err != nil {
			return err
		}
	}
	return nil
}

// validatePostgresFamilyObjects accepts the continuous aggregates a
// PostgreSQL-family target's TimescaleDB owner plans, and the policies the
// row-security owner plans on a target it is registered for, and refuses
// every other named object. Capability gating stays with rendering, which
// writes the skip line on a target without the key.
func validatePostgresFamilyObjects(dialect string, objects schemaext.Objects) error {
	all, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range all {
		switch value := object.Value.(type) {
		case *tsschema.DesiredContinuousAggregate:
			if err := tsschema.ValidateContinuousAggregateRef(object.Ref); err != nil {
				return err
			}
			if err := tsschema.ValidateDesiredContinuousAggregate(value); err != nil {
				return err
			}
		case *pgpolicy.DesiredPolicy:
			if !slices.Contains(pgpolicyprovider.Targets(), platform.NormalizeDialect(dialect)) {
				return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
			}
			if _, err := pgpolicy.DesiredPolicyObject(object.Ref, *value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
		}
	}
	return nil
}

func prepareFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return refuseActiveFacets(dialect, projected)
}

func projectFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	if facets.IsZero() {
		return facets, nil
	}
	selected, err := resolveTargetSelection(dialect)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.ForTarget(selected)
}

func refuseActiveFacets(dialect string, projected schemaext.Facets) (schemaext.Facets, error) {
	if projected.Len() == 0 {
		return projected, nil
	}
	return schemaext.Facets{}, fmt.Errorf("%w: feature facet %q is not registered for target %q", ptaherr.ErrUnsupportedFeature, projected.Kinds()[0], dialect)
}

func prepareTableFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	if platform.IsPostgresFamily(dialect) {
		return preparePostgresTableFacets(dialect, projected)
	}
	var validate func(schemaext.Facets) error
	switch platform.NormalizeDialect(dialect) {
	case platform.ClickHouse:
		validate = clickhouse.ValidateTableFacets
	case platform.YDB:
		validate = ydbrender.ValidateTableFacets
	case platform.MySQL, platform.MariaDB:
		validate = mysqlrender.ValidateTableFacets
	case platform.SQLite:
		validate = func(facets schemaext.Facets) error {
			if _, err := sqlitetable.TableOptions(facets); err != nil {
				return err
			}
			_, err := sqlitetable.VirtualDeclaration(facets)
			return err
		}
	default:
		return refuseActiveFacets(dialect, projected)
	}
	if err := validate(projected); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

func prepareIndexFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	var validate func(schemaext.Facets) error
	switch platform.NormalizeDialect(dialect) {
	case platform.ClickHouse:
		validate = clickhouse.ValidateIndexFacets
	case platform.YDB:
		validate = ydbrender.ValidateIndexFacets
	case platform.MySQL, platform.MariaDB:
		validate = mysqlrender.ValidateIndexFacets
	default:
		return refuseActiveFacets(dialect, projected)
	}
	if err := validate(projected); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

// prepareColumnFacets projects a column's facets onto the target. The MySQL
// family writes its own column settings into the column definition; every
// other active value, and every value on another target, is refused rather
// than rendered without it.
func prepareColumnFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		if err := mysqlrender.ValidateColumnFacets(projected); err != nil {
			return schemaext.Facets{}, err
		}
		return projected, nil
	default:
		return refuseActiveFacets(dialect, projected)
	}
}

// preparePostgresTableFacets accepts the TimescaleDB settings every
// PostgreSQL-family renderer writes after CREATE TABLE, the row-security
// switches it writes there on a target the row-security owner is registered
// for, the row-level TTL a CockroachDB CREATE TABLE carries, and the row
// deletion policy a Spanner one carries. Every other kind is refused: no owner
// composed for this family renders it, and Spanner has no row security to
// compare or plan.
func preparePostgresTableFacets(dialect string, projected schemaext.Facets) (schemaext.Facets, error) {
	if err := tsrender.ValidateTableFacets(projected); err != nil {
		return schemaext.Facets{}, err
	}
	rest := projected.Without(tsschema.HypertableKind)
	if slices.Contains(pgpolicyprovider.Targets(), platform.NormalizeDialect(dialect)) {
		if err := policyrender.ValidateTableFacets(projected); err != nil {
			return schemaext.Facets{}, err
		}
		rest = rest.Without(pgpolicy.TableStateKind)
	}
	var validate func(schemaext.Facets) error
	switch platform.NormalizeDialect(dialect) {
	case platform.CockroachDB:
		validate = crdbrender.ValidateTableFacets
	case platform.Spanner:
		validate = spannerrender.ValidateTableFacets
	}
	if validate != nil {
		if err := validate(rest); err != nil {
			return schemaext.Facets{}, err
		}
		return projected, nil
	}
	if _, err := refuseActiveFacets(dialect, rest); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

// prepareMaterializedViewFacets projects a materialized view's facets onto the
// target. ClickHouse interprets a refresh schedule; any other active value, and
// every value on another target, is refused rather than rendered without it.
func prepareMaterializedViewFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return refuseActiveFacets(dialect, projected)
	}
	if err := clickhouse.ValidateMaterializedViewFacets(projected); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

func validateDeclaredFacets(dialect string, database *schemamodel.Database) error {
	owners := make(map[*schemaext.Facets]func(string, schemaext.Facets) (schemaext.Facets, error),
		len(database.Tables)+len(database.Indexes)+len(database.MaterializedViews))
	for i := range database.Tables {
		owners[&database.Tables[i].Facets] = prepareTableFacets
	}
	for i := range database.Indexes {
		owners[&database.Indexes[i].Facets] = prepareIndexFacets
	}
	for i := range database.MaterializedViews {
		owners[&database.MaterializedViews[i].Facets] = prepareMaterializedViewFacets
	}
	for i := range database.Fields {
		owners[&database.Fields[i].Facets] = prepareColumnFacets
	}
	for _, facets := range database.FacetSlots() {
		prepare := prepareFacets
		if owner, found := owners[facets]; found {
			prepare = owner
		}
		if _, err := prepare(dialect, *facets); err != nil {
			return err
		}
	}
	return nil
}

// validateDeclaredRowSecurity refuses PostgreSQL row-level security on a
// target the row-security owner is not registered for, before any other
// declaration is checked. The refusal names the declaration and the scope that
// keeps it to the targets that host it: a policy written without one is
// offered to every target, and SQL Server's security policy and ClickHouse's
// row policy are declared with their own scope.
func validateDeclaredRowSecurity(dialect string, database *schemamodel.Database) error {
	if platform.IsPostgresFamily(dialect) {
		if err := refuseSharedRowSecurity(dialect, database); err != nil {
			return err
		}
	}
	if platform.NormalizeDialect(dialect) == platform.ClickHouse && len(database.RLSPolicies)+len(database.RLSEnabledTables) > 0 {
		return fmt.Errorf("%w: shared row-level security declarations on %s; a ClickHouse row policy is the ClickHouse owner's, "+
			"declared with //ptah:schema:rowpolicy in Go or as an rls_policies entry scoped to clickhouse in YAML",
			ptaherr.ErrUnsupportedFeature, platform.ClickHouse)
	}
	if slices.Contains(pgpolicyprovider.Targets(), platform.NormalizeDialect(dialect)) {
		return nil
	}
	var subject string
	for _, ref := range database.FeatureObjects.Refs() {
		if ref.Kind == objectidentity.Kind(pgpolicy.PolicyKind) {
			subject = fmt.Sprintf("policy %q on %s", ref.Name.Source, pgpolicy.Table(ref))
			break
		}
	}
	for _, table := range database.Tables {
		if subject == "" && slices.Contains(table.Facets.Kinds(), pgpolicy.TableStateKind) {
			subject = fmt.Sprintf("row-level security on table %q", table.QualifiedName())
		}
	}
	if subject == "" {
		return nil
	}
	return fmt.Errorf("%w: PostgreSQL %s cannot be planned on %s; scope its declaration to the targets that host it, "+
		`as dialects="%s" does in a Go annotation`,
		ptaherr.ErrUnsupportedFeature, subject, platform.NormalizeDialect(dialect), strings.Join(pgpolicyprovider.Targets(), ","))
}

// refuseSharedRowSecurity refuses shared row-level security declarations on a
// PostgreSQL-family target. Every source hands PostgreSQL row-level security
// to the owner, so a shared declaration that reaches this family was built by
// hand; planning it would bypass the owner, and leaving it out would report a
// control applied that is not.
func refuseSharedRowSecurity(dialect string, database *schemamodel.Database) error {
	if len(database.RLSPolicies) == 0 && len(database.RLSEnabledTables) == 0 {
		return nil
	}
	return fmt.Errorf("%w: shared row-level security declarations on %s; the PostgreSQL family declares "+
		"row-level security through the row-security owner's models", ptaherr.ErrUnsupportedFeature, platform.NormalizeDialect(dialect))
}
