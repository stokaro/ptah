// Package schemaprecondition builds the CREATE SCHEMA node a migration plan
// emits before the objects that are declared inside that schema, and refuses a
// database change a planner does not plan.
//
// It exists so that the two planners which emit such a node -- the PostgreSQL
// family's and SQL Server's -- construct it the same way. Both derive the
// schema NAME from the qualified names of the objects they are creating, and
// stopping there leaves everything else the declaration says about the schema
// with nowhere to go: the plan creates a schema and drops the comment the
// author wrote for it, on every run, with the next comparison seeing a schema
// whose comment the declaration has and the database does not
// (stokaro/ptah#2618).
package schemaprecondition

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// Node returns the creation for one schema, carrying what the declaration says
// about it.
//
// name is the schema an added object needs, which is why the node is emitted at
// all; declared is every schema the desired document declares, normally
// [ptah.run/migration/schemadiff/difftypes.SchemaDiff.DeclaredSchemas].
// A name no declaration matches yields the bare guarded creation, which is the
// established behavior for a schema reached only through an object's qualifier.
//
// The renderers decide what survives: PostgreSQL writes COMMENT ON SCHEMA, SQL
// Server writes the comment as a leading `--` line, and the MySQL-family
// renderer writes DEFAULT CHARACTER SET and COLLATE. Attaching all three here
// keeps that decision in one place per dialect rather than in the planner.
//
// A name two declarations fold onto yields the bare creation as well. Two
// schemas that compare equal name no one declaration, and attaching one of
// their comments would be a coin toss written into the user's database; the
// schema still gets created, so nothing is lost that was not already ambiguous.
func Node(name string, declared []schemamodel.Schema, semantics identifier.Semantics) *ast.CreateSchemaNode {
	node := &ast.CreateSchemaNode{Name: name, IfNotExists: true}
	match := find(name, declared, semantics)
	if match == nil {
		return node
	}
	node.Comment = match.Comment
	node.Charset = match.Charset
	node.Collate = match.Collate
	return node
}

// find returns the single declaration naming the same schema as name, or nil.
//
// An exact match wins before any folding, so a document that spells the name
// the way the object's qualifier does is never re-interpreted. Otherwise the
// two are compared under the dialect's rule for a schema name, which is what
// joins `App` and `app` on SQL Server and keeps them apart on PostgreSQL.
func find(name string, declared []schemamodel.Schema, semantics identifier.Semantics) *schemamodel.Schema {
	for i := range declared {
		if declared[i].Name == name {
			return &declared[i]
		}
	}
	key := semantics.TableIdentityKey(name)
	var found *schemamodel.Schema
	for i := range declared {
		if semantics.TableIdentityKey(declared[i].Name) != key {
			continue
		}
		if found != nil {
			return nil
		}
		found = &declared[i]
	}
	return found
}

// RefuseServerSchemas refuses a diff that creates, drops or changes a
// database, for a planner of dialect that plans no databases. The comparison
// records those changes only for a whole MySQL or MariaDB server
// (stokaro/ptah#3789), so another planner reaches one only through a diff
// built by hand, and planning nothing would report the two sides equal.
func RefuseServerSchemas(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil || len(diff.SchemasAdded)+len(diff.SchemasRemoved)+len(diff.SchemasModified) == 0 {
		return nil
	}
	return fmt.Errorf("%w: the diff creates, drops or changes a database, which only a MySQL or MariaDB plan of a whole server does; the %s planner plans none",
		ptaherr.ErrUnsupportedFeature, dialect)
}

// RefuseIndexChangesInPlace refuses a diff that renames an index or changes an
// index's partitioning in place, for a planner of dialect that plans neither.
// The comparison records those changes only on a target with
// capability.IndexRename or capability.IndexPartitioning, which only YDB has,
// so another planner reaches one only through a diff built by hand, and
// planning nothing would leave the index as it was and report the two sides
// equal.
func RefuseIndexChangesInPlace(dialect string, diff *difftypes.SchemaDiff) error {
	switch {
	case diff == nil:
		return nil
	case len(diff.IndexesRenamed) > 0:
		rename := diff.IndexesRenamed[0]
		return fmt.Errorf("%w: the diff renames index %q of table %q to %q, and the %s planner plans no index rename",
			ptaherr.ErrUnsupportedFeature, rename.From, rename.TableName, rename.To, dialect)
	case len(diff.IndexPartitioningChanged) > 0:
		change := diff.IndexPartitioningChanged[0]
		return fmt.Errorf("%w: the diff changes the partitioning of index %q of table %q, which only a YDB plan does; "+
			"the %s planner plans none", ptaherr.ErrUnsupportedFeature, change.Name, change.TableName, dialect)
	default:
		return nil
	}
}

// RefuseSerialSequenceChanges refuses a diff that changes the start or the
// increment of a Serial column's sequence, for a planner of dialect that plans
// no such change. The comparison records one only on a target with
// capability.SerialSequenceOptions, which only YDB has, so another planner
// reaches one only through a diff built by hand, and planning nothing would
// leave the sequence as it was and report the two sides equal.
func RefuseSerialSequenceChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			for _, key := range []string{"identity_start", "identity_increment"} {
				if change, changed := column.Changes[key]; changed {
					return fmt.Errorf("%w: the diff changes %s of column %q of table %q (%s), which only a YDB plan "+
						"does; the %s planner plans none", ptaherr.ErrUnsupportedFeature, key, column.ColumnName,
						table.TableName, change, dialect)
				}
			}
		}
	}
	return nil
}

// RefuseYDBColumnFamilyChanges refuses a diff that changes a table's YDB column
// families, for a planner of dialect that plans none. Only a YDB catalog
// reports column families, so another planner reaches such a change through a
// declaration that names them, or a diff built by hand, and planning nothing
// would leave every column where the server put it while the comparison kept
// reporting the difference.
func RefuseYDBColumnFamilyChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	for _, tableDiff := range diff.TablesModified {
		if tableDiff.YDBColumnFamiliesChange != nil {
			return fmt.Errorf("%w: the diff changes the column families of table %q, which only a YDB plan does; "+
				"the %s planner plans none", ptaherr.ErrUnsupportedFeature, tableDiff.TableName, dialect)
		}
	}
	return nil
}

// RefuseChangefeedChanges refuses a diff that changes a table's changefeeds,
// for a planner of dialect that plans none. The comparison records such a
// change only where the two sides hold changefeeds, which only a YDB
// declaration and a YDB database do, so another planner reaches one only
// through a diff built by hand or a declaration meant for YDB; planning
// nothing would leave the changefeeds as they were and report the two sides
// equal.
func RefuseChangefeedChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	for _, tableDiff := range diff.TablesModified {
		if tableDiff.ChangefeedsChange != nil {
			return fmt.Errorf("%w: the diff changes the changefeeds of table %q, which only a YDB plan does; "+
				"the %s planner plans none", ptaherr.ErrUnsupportedFeature, tableDiff.TableName, dialect)
		}
	}
	return nil
}

// RefuseTopics refuses a diff that creates, drops or changes a YDB topic, for
// a planner of dialect that plans none. The comparison records a topic change
// whenever a desired schema declares one, and only the YDB planner plans it,
// so planning nothing here would report the topic applied while the database
// has none.
func RefuseTopics(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	var subject string
	switch {
	case len(diff.TopicsAdded) > 0:
		subject = "creates topic " + diff.TopicsAdded[0].QualifiedName()
	case len(diff.TopicsRemoved) > 0:
		subject = "drops topic " + diff.TopicsRemoved[0].QualifiedName()
	case len(diff.TopicsModified) > 0:
		subject = "changes topic " + diff.TopicsModified[0].Name
	default:
		return nil
	}
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(capability.Topics),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("the diff %s, which requires target capability %s, unavailable on this %s target; "+
			"only a YDB plan creates, drops or changes a topic", subject, capability.Topics, dialect),
	}
}

// RefuseRoleMemberships refuses a diff that adds or removes the membership of
// a role in another, for a planner of dialect that plans none. The comparison
// records memberships only on a target with capability.RoleMembership, which
// only YDB has, so another planner reaches one only through a diff built by
// hand, and planning nothing would leave the member holding what it held and
// report the two sides equal.
func RefuseRoleMemberships(dialect string, diff *difftypes.SchemaDiff) error {
	switch {
	case diff == nil:
		return nil
	case len(diff.RoleMembershipsAdded) > 0:
		membership := diff.RoleMembershipsAdded[0]
		return fmt.Errorf("%w: the diff makes %q a member of %q, and the %s planner plans no role membership",
			ptaherr.ErrUnsupportedFeature, membership.Member, membership.Role, dialect)
	case len(diff.RoleMembershipsRemoved) > 0:
		membership := diff.RoleMembershipsRemoved[0]
		return fmt.Errorf("%w: the diff takes %q out of %q, and the %s planner plans no role membership",
			ptaherr.ErrUnsupportedFeature, membership.Member, membership.Role, dialect)
	default:
		return nil
	}
}

// RefuseCoordinationNodes refuses a diff that creates, changes or drops a YDB
// coordination node, for a planner of dialect, which has none, with a
// [ptaherr.CapabilityError] naming [capability.CoordinationNodes]. The
// comparison records a declared node on any target, so a schema that declares
// one reaches every planner, and planning nothing would report the database
// synced while the node is missing.
func RefuseCoordinationNodes(dialect string, diff *difftypes.SchemaDiff) error {
	var subject string
	switch {
	case diff == nil:
		return nil
	case len(diff.CoordinationNodesAdded) > 0:
		subject = "the diff creates coordination node " + diff.CoordinationNodesAdded[0].QualifiedName()
	case len(diff.CoordinationNodesModified) > 0:
		subject = "the diff changes coordination node " + diff.CoordinationNodesModified[0].QualifiedName()
	case len(diff.CoordinationNodesRemoved) > 0:
		subject = "the diff drops coordination node " + diff.CoordinationNodesRemoved[0].QualifiedName()
	default:
		return nil
	}
	normalized := platform.NormalizeDialect(dialect)
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: string(capability.CoordinationNodes),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target: "+
			"a coordination node is a YDB object", subject, capability.CoordinationNodes, normalized),
	}
}

// RefuseYDBTableSettingChanges refuses a diff that changes a YDB row table's
// own settings -- its column families, or its partitioning, read replicas or
// key bloom filter -- for a planner of dialect that plans none; see
// [RefuseYDBColumnFamilyChanges] and [RefuseYDBTablePartitioningChanges].
// Every planner but YDB's asks it, once.
func RefuseYDBTableSettingChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if err := RefuseYDBColumnFamilyChanges(dialect, diff); err != nil {
		return err
	}
	return RefuseYDBTablePartitioningChanges(dialect, diff)
}

// RefuseYDBTablePartitioningChanges refuses a diff that changes a table's YDB
// settings -- how it splits into partitions, its read replicas or its key
// bloom filter -- for a planner of dialect that plans no such change. Only a
// YDB catalog reports the settings, so another planner reaches such a change
// through a declaration that names them, or a diff built by hand, and
// planning nothing would leave the table at the server's defaults while the
// comparison kept reporting the difference.
func RefuseYDBTablePartitioningChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	for _, table := range diff.TablesModified {
		if table.YDBPartitioningChange != nil {
			return fmt.Errorf("%w: the diff changes the partitioning, read replicas or key bloom filter of table %q, "+
				"which only a YDB plan does; the %s planner plans none", ptaherr.ErrUnsupportedFeature, table.TableName, dialect)
		}
	}
	return nil
}
