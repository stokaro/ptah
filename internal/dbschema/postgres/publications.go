package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/pgsystemschema"
)

// readPublicationObjects reads every logical replication publication of the
// database. Only PostgreSQL's are read, the one target whose publications
// Ptah models. The read is complete, so a publication it does not report is
// absent.
//
// A publication belongs to the database rather than to a schema, so only a
// read of every schema the database describes reads them. A read narrowed to
// some of its schemas, by --schemas or by a URL that pins a search_path,
// reports none and claims nothing about them: a comparison then neither
// creates, changes nor drops a publication, and says it could not decide.
// Reporting them would let a document that describes one schema drop every
// publication of the database.
//
// A member's row filter and column list, and TABLES IN SCHEMA, are read on a
// server that has them, the ones with [capability.PublicationSchemas], since
// pg_publication_rel gained prqual and prattrs and pg_publication_namespace
// appeared in the same release. publish_generated_columns is read where the
// server has it, through to_jsonb, so one query serves every line.
func (r *Reader) readPublicationObjects(ctx context.Context, database *catalog.Database) error {
	if r.dialect != platform.Postgres {
		return nil
	}
	if missing, err := r.unreadDescribedSchemas(ctx); err != nil || len(missing) != 0 {
		if err != nil {
			return err
		}
		unknown, err := pgpublication.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected,
			Reason: fmt.Sprintf("the read covers some of the database's schemas and not %s, and a publication belongs to the whole database",
				strings.Join(missing, ", "))}, nil)
		if err != nil {
			return err
		}
		database.FeatureCoverage, err = database.FeatureCoverage.Combine(unknown)
		return err
	}
	publications, order, err := r.readPublications(ctx)
	if err != nil {
		return err
	}
	if err := r.readPublicationTables(ctx, publications); err != nil {
		return err
	}
	if r.caps.Has(capability.PublicationSchemas) {
		if err := r.readPublicationSchemas(ctx, publications); err != nil {
			return err
		}
	}
	for _, name := range order {
		object, err := pgpublication.ObservedObject(pgpublication.Ref(name), *publications[name])
		if err != nil {
			return err
		}
		if database.FeatureObjects, err = database.FeatureObjects.With(object); err != nil {
			return err
		}
	}
	known, err := pgpublication.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}

// readPublications reads each publication's flags and owner, by name, and the
// names in catalog order.
func (r *Reader) readPublications(ctx context.Context) (map[string]*pgpublication.ObservedPublication, []string, error) {
	const query = `
		SELECT p.pubname, pg_catalog.pg_get_userbyid(p.pubowner), p.puballtables,
		       p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot,
		       COALESCE(pg_catalog.to_jsonb(p)->>'pubgencols', '')
		FROM pg_catalog.pg_publication p
		ORDER BY p.pubname`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("read publications: %w", err)
	}
	defer rows.Close()
	publications := make(map[string]*pgpublication.ObservedPublication)
	var order []string
	for rows.Next() {
		var name, generated string
		var publication pgpublication.ObservedPublication
		var insert, update, remove, truncate bool
		if err := rows.Scan(&name, &publication.Owner, &publication.AllTables, &insert, &update, &remove, &truncate,
			&publication.ViaPartitionRoot, &generated); err != nil {
			return nil, nil, fmt.Errorf("read publications: %w", err)
		}
		publication.Publish = make([]string, 0)
		for _, operation := range []struct {
			published bool
			name      string
		}{{insert, pgpublication.PublishInsert}, {update, pgpublication.PublishUpdate}, {remove, pgpublication.PublishDelete},
			{truncate, pgpublication.PublishTruncate}} {
			if operation.published {
				publication.Publish = append(publication.Publish, operation.name)
			}
		}
		switch generated {
		case "":
		case "s":
			publication.GeneratedColumns = pgpublication.GeneratedColumnsStored
		case "n":
			publication.GeneratedColumns = pgpublication.GeneratedColumnsNone
		default:
			return nil, nil, fmt.Errorf("read publications: publication %q has publish_generated_columns %q, which Ptah does not know", name, generated)
		}
		publications[name] = &publication
		order = append(order, name)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read publications: %w", err)
	}
	return publications, order, nil
}

// readPublicationTables reads the tables each publication names, with each
// one's row filter and column list where the server has them.
func (r *Reader) readPublicationTables(ctx context.Context, publications map[string]*pgpublication.ObservedPublication) error {
	query := `
		SELECT p.pubname, n.nspname, c.relname, '', '[]'
		FROM pg_catalog.pg_publication_rel pr
		JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
		JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		ORDER BY p.pubname, n.nspname, c.relname`
	if r.caps.Has(capability.PublicationSchemas) {
		query = `
		SELECT p.pubname, n.nspname, c.relname,
		       COALESCE(pg_catalog.pg_get_expr(pr.prqual, pr.prrelid), ''),
		       COALESCE((SELECT pg_catalog.json_agg(a.attname ORDER BY a.attnum)::text
		                 FROM pg_catalog.pg_attribute a
		                 WHERE a.attrelid = pr.prrelid AND a.attnum = ANY (pr.prattrs)), '[]')
		FROM pg_catalog.pg_publication_rel pr
		JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
		JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		ORDER BY p.pubname, n.nspname, c.relname`
	}
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("read publication tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, columns string
		var member pgpublication.Member
		if err := rows.Scan(&name, &member.Schema, &member.Table, &member.RowFilter, &columns); err != nil {
			return fmt.Errorf("read publication tables: %w", err)
		}
		if err := json.Unmarshal([]byte(columns), &member.Columns); err != nil {
			return fmt.Errorf("read publication tables: the column list of %s.%s in publication %q: %w", member.Schema, member.Table, name, err)
		}
		if len(member.Columns) == 0 {
			member.Columns = nil
		}
		if publication, found := publications[name]; found {
			publication.Tables = append(publication.Tables, member)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read publication tables: %w", err)
	}
	return nil
}

// readPublicationSchemas reads the schemas each publication publishes every
// table of.
func (r *Reader) readPublicationSchemas(ctx context.Context, publications map[string]*pgpublication.ObservedPublication) error {
	const query = `
		SELECT p.pubname, n.nspname
		FROM pg_catalog.pg_publication_namespace pn
		JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
		JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid
		ORDER BY p.pubname, n.nspname`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("read publication schemas: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, schema string
		if err := rows.Scan(&name, &schema); err != nil {
			return fmt.Errorf("read publication schemas: %w", err)
		}
		if publication, found := publications[name]; found {
			publication.Schemas = append(publication.Schemas, schema)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read publication schemas: %w", err)
	}
	return nil
}

// unreadDescribedSchemas returns the schemas the database describes that the
// read does not cover, sorted: the ones a realm read would read beside the
// read's own.
func (r *Reader) unreadDescribedSchemas(ctx context.Context) ([]string, error) {
	query := `SELECT n.nspname FROM pg_catalog.pg_namespace n WHERE ` + pgsystemschema.PostgresDescribedSchemasPredicate(r.dialect, r.caps) +
		` ORDER BY n.nspname`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read the schemas the database describes: %w", err)
	}
	defer rows.Close()
	read := r.schemasToRead()
	var missing []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("read the schemas the database describes: %w", err)
		}
		if !slices.Contains(read, schema) {
			missing = append(missing, schema)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the schemas the database describes: %w", err)
	}
	return missing, nil
}
