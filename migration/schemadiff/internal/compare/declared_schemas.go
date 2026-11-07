package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// DeclaredSchemas records on diff each schema desired declares and the read does
// not hold, on an engine whose schemas are namespaces inside one database. Nothing
// else plans it: a creation comes from the qualified name of an object being
// created. Without this record, a schema with nothing in it reports synced, and a
// privilege on it plans a GRANT the server refuses. The pinned community binary
// v1.3.0 plans `CREATE SCHEMA "app"` for it, measured on PostgreSQL 18.
//
// The read is the comparison's to trust: a verb reads the schemas the document
// declares as well as the ones the URL covers (see
// [ptah.run/internal/schemascope.ComparisonScope]), so a declared schema missing
// from it is one the database does not hold. These stay out of the diff:
//
//   - The schema the connection resolved. The connector refuses a search_path that
//     resolves to nothing, so it exists, and creating it needs CREATE on the
//     database, which a role may not hold (stokaro/ptah#5233).
//   - A schema the desired state records as not described. The record says the
//     document claims nothing about it (stokaro/ptah#5227).
//
// It only ever adds. A schema the database holds and the document does not declare
// stays, whatever is in it, which is where this differs from the community binary:
// that plans `DROP SCHEMA ... CASCADE`, measured on the same server.
func DeclaredSchemas(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current []catalog.Schema,
	cov Coverage,
	semantics identifier.Semantics,
) {
	if desired == nil {
		return
	}
	held := make(map[string]struct{}, len(current)+1)
	for _, schema := range current {
		held[semantics.TableIdentityKey(schema.Name)] = struct{}{}
	}
	held[semantics.TableIdentityKey(semantics.DefaultSchema)] = struct{}{}
	for _, schema := range desired.Schemas {
		key := semantics.TableIdentityKey(schema.Name)
		if _, exists := held[key]; exists || strings.TrimSpace(schema.Name) == "" || !cov.Desired.DescribesSchema(schema.Name) {
			continue
		}
		held[key] = struct{}{}
		diff.SchemasAdded = append(diff.SchemasAdded, schema)
	}
	slices.SortFunc(diff.SchemasAdded, func(a, b schemamodel.Schema) int { return cmp.Compare(a.Name, b.Name) })
}

// DeclaredNamespaces is [DeclaredSchemas] for a target whose reads list the
// schemas of their scope through an executable provider. A read can leave the
// namespaces uninspected, and then its silence says nothing: a declared schema
// such a read does not describe is withheld and recorded as an undecided
// addition, not created, since creating a schema that exists fails.
func DeclaredNamespaces(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current []catalog.Schema,
	cov Coverage,
	semantics identifier.Semantics,
) {
	var found difftypes.SchemaDiff
	DeclaredSchemas(&found, desired, current, cov, semantics)
	kept, withheld := keepPlannedAdditions(cov, coverage.Schema, found.SchemasAdded,
		func(schema schemamodel.Schema) (string, []string) { return schema.Name, []string{schema.Name} },
		func(schema schemamodel.Schema) string { return schema.Name }, unguardedCreations())
	cov.recordUndecidedAdditions(withheld)
	diff.SchemasAdded = append(diff.SchemasAdded, kept...)
	slices.SortFunc(diff.SchemasAdded, func(a, b schemamodel.Schema) int { return cmp.Compare(a.Name, b.Name) })
}
