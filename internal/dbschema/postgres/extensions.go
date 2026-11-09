package postgres

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// readExtensionForeignTables finds foreign tables whose server uses a wrapper
// installed by an extension. These tables are outside Ptah's schema model, so
// the planner cannot drop them ahead of that extension.
func (r *Reader) readExtensionForeignTables(ctx context.Context) (map[string][]string, error) {
	const query = `
		SELECT e.extname, n.nspname, c.relname
		FROM pg_extension e
		JOIN pg_depend d ON d.refclassid = 'pg_extension'::regclass
			AND d.refobjid = e.oid AND d.classid = 'pg_foreign_data_wrapper'::regclass
			AND d.deptype = 'e'
		JOIN pg_foreign_data_wrapper fdw ON fdw.oid = d.objid
		JOIN pg_foreign_server s ON s.srvfdw = fdw.oid
		JOIN pg_foreign_table ft ON ft.ftserver = s.oid
		JOIN pg_class c ON c.oid = ft.ftrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT EXISTS (
			SELECT 1 FROM pg_depend member
			WHERE member.classid = 'pg_class'::regclass AND member.objid = c.oid
				AND member.refclassid = 'pg_extension'::regclass
				AND member.refobjid = e.oid AND member.deptype = 'e')
		ORDER BY e.extname, n.nspname, c.relname`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query extension foreign tables: %w", err)
	}
	defer rows.Close()

	tables := make(map[string][]string)
	for rows.Next() {
		var extension, schema, table string
		if err := rows.Scan(&extension, &schema, &table); err != nil {
			return nil, fmt.Errorf("scan extension foreign table: %w", err)
		}
		tables[extension] = append(tables[extension], catalog.QualifyTableName(schema, table))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read extension foreign tables: %w", err)
	}
	return tables, nil
}

// readExtensionObjects reports the installed extensions to the PostgreSQL
// extension owner, each as an observed object, and claims complete knowledge
// of them: the read asked pg_extension for every one, so an extension it does
// not report is not installed.
func (r *Reader) readExtensionObjects(ctx context.Context, schema *catalog.Database) error {
	extensions, err := r.readExtensions(ctx)
	if err != nil {
		return err
	}
	for _, extension := range extensions {
		object, err := pgextension.ObservedObject(pgextension.Ref(extension.name), extension.observed)
		if err != nil {
			return err
		}
		if schema.FeatureObjects, err = schema.FeatureObjects.With(object); err != nil {
			return err
		}
	}
	known, err := pgextension.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return err
	}
	schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(known)
	return err
}
