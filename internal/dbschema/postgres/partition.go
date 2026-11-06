package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/pgtablesource"
	"ptah.run/internal/sqlident"
)

// readTableParents records on each inspected table the tables it is a partition
// of or inherits from, which only pg_inherits holds, and on a partition its bound.
// A parent outside the read is recorded too: nothing the plan does depends on it,
// and it says what the catalog says.
func (r *Reader) readTableParents(ctx context.Context, database *catalog.Database) error {
	if !r.caps.Has(capability.CatalogPartitions) {
		return nil
	}
	tables := make(map[string]*catalog.Table, len(database.Tables))
	for i := range database.Tables {
		table := &database.Tables[i]
		tables[catalog.QualifyTableName(table.Schema, table.Name)] = table
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT child_namespace.nspname, child.relname, parent_namespace.nspname, parent.relname,
		       COALESCE(child.relispartition, false), pg_get_expr(child.relpartbound, child.oid)
		FROM pg_inherits inheritance
		JOIN pg_class child ON child.oid = inheritance.inhrelid
		JOIN pg_namespace child_namespace ON child_namespace.oid = child.relnamespace
		JOIN pg_class parent ON parent.oid = inheritance.inhparent
		JOIN pg_namespace parent_namespace ON parent_namespace.oid = parent.relnamespace
		WHERE child.relkind IN ('r', 'p')
		ORDER BY 1, 2, 3, 4`)
	if err != nil {
		return fmt.Errorf("read table parents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var childSchema, childName, parentSchema, parentName string
		var isPartition bool
		var bound sql.NullString
		if err := rows.Scan(&childSchema, &childName, &parentSchema, &parentName, &isPartition, &bound); err != nil {
			return fmt.Errorf("read table parents: %w", err)
		}
		// A catalog row names every schema, and a read of one schema gives its
		// tables no qualifier, so the row of a table in another schema has to keep
		// its own: it is not the table of that name this read holds.
		table, read := tables[catalog.QualifyTableName(r.referencedSchema(childSchema), childName)]
		if !read {
			continue
		}
		table.Parents = append(table.Parents, catalog.QualifyTableName(r.referencedSchema(parentSchema), parentName))
		if isPartition && pgtable.SupportsPartition(r.dialect) {
			member := &pgtable.ObservedPartitionMember{ParentSchema: r.referencedSchema(parentSchema), Parent: parentName, Bound: bound.String}
			if err := pgtable.ValidateObservedPartitionMember(member); err != nil {
				return fmt.Errorf("read partition bound of %s: %w", table.QualifiedName(), err)
			}
			if table.Facets, err = table.Facets.With(member); err != nil {
				return fmt.Errorf("read partition bound of %s: %w", table.QualifiedName(), err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read table parents: %w", err)
	}
	if !pgtable.SupportsPartition(r.dialect) {
		return nil
	}
	known, err := pgtable.PartitionMemberCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}

// readsPartitionCopies reports a server that holds partitions, whose catalog says
// which objects are copies of a parent's. CockroachDB has pg_inherits and no
// PARTITION OF, so the question has no answer there to read.
func (r *Reader) readsPartitionCopies() bool {
	return r.caps.Has(capability.CatalogPartitions) && pgtable.SupportsPartition(r.dialect)
}

// markInheritedConstraints marks each constraint of a read table that the table
// holds from another one: a partition's copy of a primary key, unique key or
// foreign key (conparentid), and a check it inherits (conislocal). The catalog is
// the only place that says so, and no statement on the table itself can drop one.
func (r *Reader) markInheritedConstraints(ctx context.Context, constraints []catalog.Constraint) error {
	if !r.readsPartitionCopies() || len(constraints) == 0 {
		return nil
	}
	type constraintKey struct{ table, name string }
	positions := make(map[constraintKey]int, len(constraints))
	for i, constraint := range constraints {
		positions[constraintKey{catalog.QualifyTableName(constraint.Schema, constraint.TableName), constraint.Name}] = i
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT n.nspname, c.relname, con.conname
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE con.conparentid <> 0 OR NOT con.conislocal
		ORDER BY 1, 2, 3`)
	if err != nil {
		return fmt.Errorf("read inherited constraints: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schemaName, table, name string
		if err := rows.Scan(&schemaName, &table, &name); err != nil {
			return fmt.Errorf("read inherited constraints: %w", err)
		}
		if position, read := positions[constraintKey{catalog.QualifyTableName(r.referencedSchema(schemaName), table), name}]; read {
			constraints[position].Inherited = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read inherited constraints: %w", err)
	}
	return nil
}

// readPartitions captures keys only for inspected partitioned parents. Ordinary
// tables have complete absence coverage. The execution-facing parent flag stays
// independent, so an unreadable key cannot permit a concurrent parent index.
func (r *Reader) readPartitions(ctx context.Context, database *catalog.Database) error {
	if !pgtable.SupportsPartition(r.dialect) {
		return nil
	}
	identities := objectidentity.NewBuilder(identifier.ForDialect(r.dialect))
	subjects := make([]schemaext.SubjectCoverage, 0, len(database.Tables))
	for i := range database.Tables {
		table := &database.Tables[i]
		knowledge := schemaext.Knowledge{State: schemaext.Complete}
		if table.Partitioned {
			value, err := r.readPartition(ctx, *table)
			if err != nil {
				return err
			}
			if value == nil {
				knowledge = schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "PostgreSQL does not return a partition key for the partitioned parent"}
			} else {
				table.Facets, err = table.Facets.With(value)
				if err != nil {
					return err
				}
			}
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgtable.PartitionKind, Subject: identities.TableParts(table.Schema, table.Name), Knowledge: knowledge})
	}
	known, err := pgtable.PartitionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables have inspected partition keys"}, subjects)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}

func (r *Reader) readPartition(ctx context.Context, table catalog.Table) (*pgtable.ObservedPartition, error) {
	name := sqlident.Qualified(platform.Postgres, table.Schema, table.Name)
	// By the table's oid rather than a name cast to regclass: PostgreSQL resolves
	// a name only in a schema the role holds USAGE on, and the read lists tables
	// in every schema.
	schemaName := table.Schema
	if schemaName == "" {
		schemaName = r.schema
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT pg_get_partkeydef(c.oid)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'p'`, schemaName, table.Name)
	if err != nil {
		return nil, fmt.Errorf("read partition key of %s: %w", name, err)
	}
	defer rows.Close()
	var definition sql.NullString
	for rows.Next() {
		if err := rows.Scan(&definition); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !definition.Valid || definition.String == "" {
		return nil, nil
	}
	value, err := pgtablesource.ParsePartitionSQL(definition.String)
	if err != nil {
		return nil, fmt.Errorf("read partition key of %s: %w", name, err)
	}
	return &pgtable.ObservedPartition{Type: value.Type, Parts: value.Parts}, nil
}
