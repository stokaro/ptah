package postgres

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
)

// A generated column rebuild takes a generated column out of its table before
// the column step and puts it back after it, around a type change of a column
// its expression reads. PostgreSQL refuses the type change otherwise, and has
// no statement that detaches an expression and attaches it again:
//
//	ALTER TABLE prices ALTER COLUMN nickname TYPE varchar(100);
//	ERROR:  cannot alter type of a column used by a generated column
//
// Dropping the column drops, without a word, every index and every table
// constraint on it, any statistics object that names it, its comment, its
// privileges and its storage options. Measured on PostgreSQL 18, a plain DROP
// COLUMN under RESTRICT takes a partial GIN index outside the declaration with
// it. A view that reads the column never meets the drop: the view rebuild
// drops it earlier in the plan and creates it after the column step.
//
// So the rebuild restates what the database holds on the column, from the
// catalog the comparison read: its indexes from their pg_get_indexdef text,
// its CHECK constraints and the comments on both. The index restated is the
// one the database holds, not the one the declaration asks for, so the steps
// after the rebuild find the table as it was and change it as they would
// have. Before it drops the column, a guard asks the database for anything
// else that depends on it -- an index or a constraint the read did not report,
// a policy, a trigger with a column list, a foreign key from another table, a
// statistics object, a privilege, a storage option, a collation, a security
// label -- and refuses the plan naming each one, rather than drop it silently.
// It refuses a policy or a trigger even where the declaration keeps it
// unchanged, since the plan restates neither.
//
// The column comes back at the end of the table, since ADD COLUMN appends it.
type generatedRebuild struct {
	// table is the table as the column steps name it.
	table string
	// column is the generated column, as the database holds it.
	column catalog.Column
	// declared is the column the declaration asks for, which the rebuild
	// adds back.
	declared schemamodel.Field
	// reads are the columns of the table whose type the plan changes and
	// the expression reads.
	reads []string
	// indexes are the indexes on the column the rebuild restates.
	indexes []catalog.Index
	// checks are the CHECK constraints on the column the rebuild restates.
	checks []catalog.Constraint
}

// planGeneratedRebuilds finds the generated columns a type change of a column
// they read has to rebuild. It refuses a plan that removes such a column, and
// one whose declaration does not hold it, since a rebuild then has nothing to
// add back.
//
// On CockroachDB it refuses the plan instead. CockroachDB refuses the type
// change the same way (SQLSTATE 2BP01, measured on v26.3.5 and v26.4.0), but
// the guard reads pg_depend, which on CockroachDB holds no row for an index on
// a computed column, and pg_describe_object, which it does not have. A
// rebuild there could not see what the drop takes.
func planGeneratedRebuilds(diff *difftypes.SchemaDiff, semantics identifier.Semantics, dialect string) ([]generatedRebuild, error) {
	var rebuilds []generatedRebuild
	for _, tableDiff := range diff.TablesModified {
		retyped := retypedColumnNames(tableDiff)
		if len(retyped) == 0 || !tableDiff.Current.HasTable() {
			continue
		}
		for _, column := range tableDiff.Current.Table.Columns {
			if column.GeneratedExpression == nil {
				continue
			}
			reads := slices.DeleteFunc(slices.Clone(retyped), func(name string) bool {
				return !deporder.ReferencesIdentifier(*column.GeneratedExpression, name)
			})
			if len(reads) == 0 {
				continue
			}
			if dialect == platform.CockroachDB {
				return nil, fmt.Errorf(
					"%w: generated column %s.%s reads column %s, whose type this plan changes; CockroachDB refuses the change "+
						"while the column depends on it, and Ptah rebuilds a generated column only on PostgreSQL and YugabyteDB: "+
						"drop the generated column, change the type and add it back in migrations of their own",
					ptaherr.ErrUnsupportedFeature, tableDiff.TableName, column.Name, strings.Join(reads, ", "))
			}
			rebuild, err := planGeneratedRebuild(tableDiff, column, reads, semantics)
			if err != nil {
				return nil, err
			}
			rebuilds = append(rebuilds, rebuild)
		}
	}
	return rebuilds, nil
}

func planGeneratedRebuild(
	tableDiff difftypes.TableDiff,
	column catalog.Column,
	reads []string,
	semantics identifier.Semantics,
) (generatedRebuild, error) {
	sameColumn := func(name string) bool {
		return semantics.ColumnIdentityKey(name) == semantics.ColumnIdentityKey(column.Name)
	}
	if slices.ContainsFunc(tableDiff.ColumnsRemoved, func(field schemamodel.Field) bool { return sameColumn(field.Name) }) {
		return generatedRebuild{}, fmt.Errorf(
			"%w: generated column %s.%s reads column %s, whose type this plan changes, and the plan also removes it: "+
				"PostgreSQL refuses the type change while the column exists, so remove the generated column in a plan of its own first",
			ptaherr.ErrUnsupportedFeature, tableDiff.TableName, column.Name, strings.Join(reads, ", "))
	}
	if column.Collate != "" {
		return generatedRebuild{}, fmt.Errorf(
			"%w: generated column %s.%s reads column %s, whose type this plan changes, and has the collation %s: "+
				"PostgreSQL refuses the type change while the column exists, and the plan cannot add the column back "+
				"with its collation, since the PostgreSQL renderer writes no COLLATE on a column",
			ptaherr.ErrUnsupportedFeature, tableDiff.TableName, column.Name, strings.Join(reads, ", "), column.Collate)
	}
	index := slices.IndexFunc(tableDiff.Desired.Fields, func(field schemamodel.Field) bool { return sameColumn(field.Name) })
	if index < 0 {
		return generatedRebuild{}, fmt.Errorf(
			"%w: generated column %s.%s reads column %s, whose type this plan changes, and the declaration of the table does not hold it, "+
				"so the plan cannot add it back after the change",
			ptaherr.ErrUnsupportedFeature, tableDiff.TableName, column.Name, strings.Join(reads, ", "))
	}
	rebuild := generatedRebuild{
		table:    tableDiff.TableName,
		column:   column,
		declared: tableDiff.Desired.Fields[index],
		reads:    reads,
	}
	keys := constraintBackedIndexNames(tableDiff.Current.Constraints)
	for _, index := range tableDiff.Current.Indexes {
		if !indexReadsColumn(index, column.Name, semantics) || index.IsPrimary || keys[index.Name] {
			continue
		}
		if definition, ok := restatedIndexDefinition(index.Definition, tableDiff.TableName); ok {
			index.Definition = definition
			rebuild.indexes = append(rebuild.indexes, index)
		}
	}
	for _, constraint := range tableDiff.Current.Constraints {
		if restatableCheck(constraint) && deporder.ReferencesIdentifier(*constraint.CheckClause, column.Name) {
			rebuild.checks = append(rebuild.checks, constraint)
		}
	}
	return rebuild, nil
}

// indexDefinitionTable splits the text pg_get_indexdef returns around the
// table it names: CREATE [UNIQUE] INDEX name ON [ONLY] table USING ...
var indexDefinitionTable = regexp.MustCompile(
	`(?s)^(CREATE (?:UNIQUE )?INDEX (?:"(?:[^"]|"")*"|[^\s"]+) ON (?:ONLY )?)` +
		`((?:(?:"(?:[^"]|"")*"|[^\s".]+)\.)?(?:"(?:[^"]|"")*"|[^\s".]+))` +
		`( USING .*)$`,
)

// restatedIndexDefinition is definition, the index as pg_get_indexdef writes
// it, naming its table the way the rest of the plan does. The server names
// the table with the schema it was read in; a plan written for a schema-bound
// target names it without one, and replaying the definition as read would
// create the index in the schema of the database the plan was made from. A
// definition of another shape is not restated, and the guard refuses the
// rebuild naming the index instead.
func restatedIndexDefinition(definition, table string) (string, bool) {
	parts := indexDefinitionTable.FindStringSubmatch(definition)
	if parts == nil {
		return "", false
	}
	return parts[1] + quotePostgresIdentifierPath(table) + parts[3], true
}

// retypedColumnNames names the columns of the table whose type the plan
// changes.
func retypedColumnNames(tableDiff difftypes.TableDiff) []string {
	var names []string
	for _, column := range tableDiff.ColumnsModified {
		if _, changed := column.Changes["type"]; changed {
			names = append(names, column.ColumnName)
		}
	}
	return names
}

// constraintBackedIndexNames names the indexes a key or an exclusion
// constraint owns. Created again on its own, such an index would come back
// without its constraint, so the rebuild leaves it to the guard.
func constraintBackedIndexNames(constraints []catalog.Constraint) map[string]bool {
	names := make(map[string]bool)
	for _, constraint := range constraints {
		if constraint.Type != "CHECK" && constraint.Type != "FOREIGN KEY" {
			names[constraint.Name] = true
		}
	}
	return names
}

// indexReadsColumn reports whether index names column as a key, in an
// expression key or in its predicate.
func indexReadsColumn(index catalog.Index, column string, semantics identifier.Semantics) bool {
	names := func(name string) bool {
		return semantics.ColumnIdentityKey(name) == semantics.ColumnIdentityKey(column)
	}
	if slices.ContainsFunc(index.Columns, names) {
		return true
	}
	for _, part := range index.Parts {
		if names(part.Name) || part.Expr != "" && deporder.ReferencesIdentifier(part.Expr, column) {
			return true
		}
	}
	return index.Condition != "" && deporder.ReferencesIdentifier(index.Condition, column)
}

// restatableCheck reports whether constraint is a CHECK the rebuild can write
// again as the database holds it. A NOT VALID or NOT ENFORCED one carries a
// state ADD CONSTRAINT would change, so the guard refuses it instead.
func restatableCheck(constraint catalog.Constraint) bool {
	return constraint.Type == "CHECK" && constraint.CheckClause != nil && *constraint.CheckClause != "" &&
		!constraint.NotValid && !constraint.NotEnforced
}

// withoutRebuiltColumnChanges returns diff without the changes of the
// generated columns the plan rebuilds: the rebuild adds each one back as the
// declaration asks for it. The diff's own slices are not changed.
func withoutRebuiltColumnChanges(diff *difftypes.SchemaDiff, rebuilds []generatedRebuild, semantics identifier.Semantics) *difftypes.SchemaDiff {
	if len(rebuilds) == 0 {
		return diff
	}
	result := *diff
	result.TablesModified = slices.Clone(diff.TablesModified)
	for i, tableDiff := range result.TablesModified {
		tableDiff.ColumnsModified = slices.DeleteFunc(slices.Clone(tableDiff.ColumnsModified), func(column difftypes.ColumnDiff) bool {
			return slices.ContainsFunc(rebuilds, func(rebuild generatedRebuild) bool {
				return rebuild.table == tableDiff.TableName &&
					semantics.ColumnIdentityKey(rebuild.column.Name) == semantics.ColumnIdentityKey(column.ColumnName)
			})
		})
		result.TablesModified[i] = tableDiff
	}
	return &result
}

// dropRebuiltGeneratedColumns guards and drops each generated column the plan
// rebuilds, ahead of the column step.
func dropRebuiltGeneratedColumns(result []ast.Node, rebuilds []generatedRebuild) []ast.Node {
	for _, rebuild := range rebuilds {
		result = append(result,
			ast.NewDescriptiveComment(fmt.Sprintf(
				"Rebuild generated column %s.%s around the type change of %s",
				rebuild.table, rebuild.column.Name, strings.Join(rebuild.reads, ", "),
			)),
			ast.NewRawSQL(rebuild.guard()),
			&ast.AlterTableNode{Name: rebuild.table, Operations: []ast.AlterOperation{
				&ast.DropColumnOperation{ColumnName: rebuild.column.Name},
			}},
		)
	}
	return result
}

// restoreRebuiltGeneratedColumns adds each generated column the plan rebuilds
// back, after the column step, and then restates what the drops took with
// them. Every column comes back before any index or CHECK: an index or a
// CHECK on two rebuilt columns reads both, and each drop took it, so it is
// written once, after the last add.
func restoreRebuiltGeneratedColumns(result []ast.Node, rebuilds []generatedRebuild, vocabulary difftypes.UserTypeVocabulary) []ast.Node {
	for _, rebuild := range rebuilds {
		column := modelast.FromFieldWithoutForeignKeys(rebuild.declared, vocabulary.Enums, platform.Postgres)
		result = append(result, &ast.AlterTableNode{Name: rebuild.table, Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: column},
		}})
		if column.Comment != "" {
			result = append(result, &ast.AlterTableNode{Name: rebuild.table, Operations: []ast.AlterOperation{
				&ast.SetCommentOperation{Column: column.Name, Comment: column.Comment},
			}})
		}
	}
	restated := make(map[string]bool)
	for _, rebuild := range rebuilds {
		for _, index := range rebuild.indexes {
			name := qualifiedIndexName(rebuild.table, index)
			if restated["index "+name] {
				continue
			}
			restated["index "+name] = true
			result = append(result, ast.NewRawSQL(index.Definition))
			if index.Comment != "" {
				result = append(result, ast.NewRawSQL(fmt.Sprintf("COMMENT ON INDEX %s IS %s",
					quotePostgresIdentifierPath(name), quotePostgresLiteral(index.Comment))))
			}
		}
		for _, check := range rebuild.checks {
			if restated["check "+rebuild.table+" "+check.Name] {
				continue
			}
			restated["check "+rebuild.table+" "+check.Name] = true
			result = append(result, ast.NewRawSQL(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
				quotePostgresIdentifierPath(rebuild.table), quotePostgresIdentifier(check.Name), *check.CheckClause)))
			if check.Comment != "" {
				result = append(result, ast.NewRawSQL(fmt.Sprintf("COMMENT ON CONSTRAINT %s ON %s IS %s",
					quotePostgresIdentifier(check.Name), quotePostgresIdentifierPath(rebuild.table), quotePostgresLiteral(check.Comment))))
			}
		}
	}
	return result
}

// qualifiedIndexName names index in the schema of its table: an index always
// lives in its table's schema.
func qualifiedIndexName(table string, index catalog.Index) string {
	if schema, _, qualified := strings.Cut(table, "."); qualified {
		return schema + "." + index.Name
	}
	return index.Name
}

// guard is the statement that refuses the rebuild, before it drops anything,
// where something the rebuild does not restate depends on the column. It
// reads the catalog of the database the plan runs on, so it also sees what
// the comparison did not read: an index outside the selected scope, or an
// object Ptah does not model.
//
// It is one SELECT rather than a DO block, so a dev database replays it under
// the default replay policy, which refuses the DO sublanguage. A row it finds
// becomes the text of a cast to integer, and the server's refusal of that
// cast carries the names: invalid input syntax for type integer: "refusing to
// rebuild generated column ...". Where nothing is found it returns no row.
//
// The column's own expression is a pg_attrdef row that depends on the column,
// automatically on PostgreSQL 14 and internally on 18; the add restates it, so
// the guard leaves it out.
//
// PostgreSQL 18 keeps a column's NOT NULL as a pg_constraint row of type n
// that depends on the column. The add restates it from the declaration, so
// the guard leaves it out with the CHECK constraints the plan restates.
//
// An option the server writes itself, such as pg_attver on PostgreSQL 18.7,
// is not one the operator set, so the options it counts leave out the pg_
// prefix.
//
// A collation other than the type's own is refused rather than restated: the
// PostgreSQL renderer writes no COLLATE on a column, so the add would bring
// the column back under the type's collation, and every index on it with it.
func (r generatedRebuild) guard() string {
	table := quotePostgresLiteral(quotePostgresIdentifierPath(r.table)) + "::regclass"
	column := quotePostgresLiteral(r.column.Name)
	indexes := make([]string, 0, len(r.indexes))
	for _, index := range r.indexes {
		indexes = append(indexes, "to_regclass("+quotePostgresLiteral(quotePostgresIdentifierPath(qualifiedIndexName(r.table, index)))+")")
	}
	checks := make([]string, 0, len(r.checks))
	for _, check := range r.checks {
		checks = append(checks, quotePostgresLiteral(check.Name))
	}
	subject := quotePostgresLiteral("refusing to rebuild generated column " + r.table + "." + r.column.Name +
		" around the type change of " + strings.Join(r.reads, ", ") + ": dropping it would take ")
	return fmt.Sprintf(`SELECT (%[1]s || string_agg(dependent, ', ' ORDER BY dependent) || ', which the plan does not restate')::integer
FROM (
    SELECT pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent
    FROM pg_depend d
    JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
    WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = %[2]s AND a.attname = %[3]s
      AND d.deptype IN ('n', 'a') AND d.classid <> 'pg_attrdef'::regclass
      AND NOT (d.classid = 'pg_class'::regclass AND d.objid = ANY (ARRAY[%[4]s]::oid[]))
      AND NOT (d.classid = 'pg_constraint'::regclass AND d.objid IN (
          SELECT oid FROM pg_constraint WHERE conrelid = %[2]s AND (contype = 'n' OR conname = ANY (ARRAY[%[5]s]::text[]))))
    UNION ALL
    SELECT 'the privileges or storage options of the column'
    FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = %[2]s AND a.attname = %[3]s
      AND (a.attacl IS NOT NULL OR EXISTS (SELECT 1 FROM unnest(a.attoptions) o WHERE o NOT LIKE 'pg\_%%')
           OR COALESCE(a.attstattarget::int, -1) <> -1
           OR COALESCE(to_jsonb(a) ->> 'attcompression', '') <> '' OR a.attstorage <> t.typstorage)
    UNION ALL
    SELECT 'the collation ' || quote_ident(c.collname) || ' of the column'
    FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid JOIN pg_collation c ON c.oid = a.attcollation
    WHERE a.attrelid = %[2]s AND a.attname = %[3]s AND a.attcollation <> t.typcollation
    UNION ALL
    SELECT 'the security label of the column'
    FROM pg_seclabel l JOIN pg_attribute a ON a.attrelid = l.objoid AND a.attnum = l.objsubid
    WHERE l.classoid = 'pg_class'::regclass AND l.objoid = %[2]s AND a.attname = %[3]s
) dependents
HAVING count(*) > 0`, subject, table, column, strings.Join(indexes, ", "), strings.Join(checks, ", "))
}
