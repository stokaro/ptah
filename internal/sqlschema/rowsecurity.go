package sqlschema

import (
	"cmp"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

// OwnRowSecurity hands the row-level security a PostgreSQL-family SQL
// document declares to the PostgreSQL row-security owner, once every file of
// the document is read: a policy becomes an object, and an enablement the
// switches facet of its table, which the document must declare. A SQL
// document of any other dialect is left as it is, and so is a declaration a
// Go annotation scoped to SQL Server or ClickHouse in a schema read beside it.
//
// It runs after the whole document because SQL spells row-level security as
// statements that may sit in other files than the table: an enablement, a
// FORCE, a COMMENT ON POLICY or a DROP TABLE reaches what an earlier file
// declared. A SQL document names no Go struct, so neither model records one.
// [ReadOnto] runs it for a file read on its own; a caller reading a
// document file by file runs it on the merged model. Two statements that
// create one policy are refused, naming both (stokaro/ptah#2440).
func OwnRowSecurity(db *schemamodel.Database, dialect string) error {
	if db == nil || !platform.IsPostgresFamily(dialect) {
		return nil
	}
	var collector pgpolicysource.Collector
	var kept []schemamodel.RLSPolicy
	for ordinal, policy := range db.RLSPolicies {
		if len(policy.Dialects) != 0 {
			kept = append(kept, policy)
			continue
		}
		if err := ownPolicy(db, &collector, policy, ordinal+1); err != nil {
			return err
		}
	}
	var keptSwitches []schemamodel.RLSEnabledTable
	for _, enabled := range db.RLSEnabledTables {
		if len(enabled.Dialects) != 0 {
			keptSwitches = append(keptSwitches, enabled)
			continue
		}
		if err := ownSwitches(db, &collector, enabled); err != nil {
			return err
		}
	}
	objects, err := db.FeatureObjects.Merge(collector.Objects())
	if err != nil {
		return err
	}
	coverage, err := pgpolicysource.Claim(db.FeatureCoverage, objects)
	if err != nil {
		return err
	}
	db.FeatureObjects, db.FeatureCoverage = objects, coverage
	db.RLSPolicies, db.RLSEnabledTables = kept, keptSwitches
	return nil
}

// ownPolicy hands one policy over. ordinal counts the document's policy
// statements, which tells apart two that read the same once their names are
// folded.
func ownPolicy(db *schemamodel.Database, collector *pgpolicysource.Collector, policy schemamodel.RLSPolicy, ordinal int) error {
	origin := fmt.Sprintf("CREATE POLICY %s ON %s (policy statement %d)", policy.Name, policy.Table, ordinal)
	index, err := pgpolicysource.DeclaredTable(db.Tables, policy.StructName, policy.Table)
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	var schema, table string
	if index >= 0 {
		schema, table = db.Tables[index].Schema, db.Tables[index].Name
	} else if schema, table, err = pgpolicysource.TableParts(policy.Table); err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	declared, err := pgpolicysource.Attributes{
		For: policy.PolicyFor, To: policy.ToRoles, Using: policy.UsingExpression, WithCheck: policy.WithCheckExpression,
		Restrictive: policy.Restrictive, Comment: policy.Comment,
	}.Policy()
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	return collector.AddPolicy(origin, pgpolicysource.Ref(schema, table, policy.Name), declared, nil)
}

func ownSwitches(db *schemamodel.Database, collector *pgpolicysource.Collector, enabled schemamodel.RLSEnabledTable) error {
	origin := fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", cmp.Or(enabled.Table, enabled.StructName))
	index, err := pgpolicysource.DeclaredTable(db.Tables, enabled.StructName, enabled.Table)
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	if index < 0 {
		return fmt.Errorf("%s: relation %q does not exist in the document", origin, cmp.Or(enabled.Table, enabled.StructName))
	}
	table := &db.Tables[index]
	state := pgpolicy.DesiredTableState{Enabled: true, Forced: enabled.Forced, Comment: enabled.Comment}
	facets, err := collector.AddSwitches(origin, pgpolicysource.TableRef(table.Schema, table.Name), table.Facets, state, nil)
	if err != nil {
		return err
	}
	table.Facets = facets
	return nil
}
