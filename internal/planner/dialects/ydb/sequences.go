package ydb

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbsequence"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/schemadiff/difftypes"
)

// serialSequencePlan is the ALTER SEQUENCE statements a plan writes, split by
// where they go: after the CREATE TABLE of a table the plan adds, and among the
// in-place changes of a table that exists.
type serialSequencePlan struct {
	// created holds, per table identity key, the statements that follow the
	// table's CREATE TABLE. Each restarts the sequence at its start when the
	// start is not 1: START alone leaves the next value where it was, even on
	// a sequence that never issued one, and the table has no row a restart
	// could collide with.
	created map[string][]*ast.AlterSerialSequenceNode
	// changed is the statements for tables that exist, in the order the diff
	// lists them. None restarts: a restart onto a value a row holds fails the
	// next insert with `Conflict with existing key`, and the declaration asks
	// for a start, not for a restart.
	changed []ast.Node
}

// planSerialSequences decides every ALTER SEQUENCE the diff needs, or refuses
// the plan before any node is emitted: a Serial column of a table the plan
// creates that declares a start or an increment other than 1, and a Serial
// column of a table that exists whose start or increment the comparison found
// changed. A table the plan rebuilds or drops is skipped; the rebuild refuses
// a table with a Serial column on its own.
func (p *Planner) planSerialSequences(
	diff *difftypes.SchemaDiff,
	rebuilds map[string]*tableRebuild,
	semantics identifier.Semantics,
) (serialSequencePlan, error) {
	plan := serialSequencePlan{created: make(map[string][]*ast.AlterSerialSequenceNode)}
	for _, creation := range diff.TablesAdded {
		for _, field := range creation.Fields {
			node, planned, err := p.serialSequence(diff, creation.Name, field, true)
			if err != nil {
				return serialSequencePlan{}, err
			}
			if planned {
				key := semantics.TableIdentityKey(creation.Name)
				plan.created[key] = append(plan.created[key], node)
			}
		}
	}
	for _, tableDiff := range diff.TablesModified {
		if _, rebuilt := rebuilds[semantics.TableIdentityKey(tableDiff.TableName)]; rebuilt {
			continue
		}
		for _, colDiff := range tableDiff.ColumnsModified {
			if !changesSerialSequence(colDiff) {
				continue
			}
			subject := fmt.Sprintf("the sequence of column %q of table %q", colDiff.ColumnName, tableDiff.TableName)
			if colDiff.Desired.Name == "" {
				return serialSequencePlan{}, refuseFact(subject,
					"the plan carries no declaration of the column to take its start and increment from")
			}
			if colDiff.CurrentSequenceRestart != "" {
				return serialSequencePlan{}, refuseFact(subject, "the sequence was restarted at "+
					colDiff.CurrentSequenceRestart+", and YDB replays that restart on every later ALTER SEQUENCE, "+
					"so the next insert would take that value again and fail on a key a row may hold "+
					"(measured: `Conflict with existing key`); a table created with a start other than 1 "+
					"is restarted at it, so its sequence keeps the start and the increment it was created with")
			}
			node, _, err := p.serialSequence(diff, tableDiff.TableName, colDiff.Desired, false)
			if err != nil {
				return serialSequencePlan{}, err
			}
			plan.changed = append(plan.changed, node)
		}
	}
	return plan, nil
}

// changesSerialSequence reports whether the comparison found the start or the
// increment of a column's sequence changed.
func changesSerialSequence(colDiff difftypes.ColumnDiff) bool {
	_, start := colDiff.Changes["identity_start"]
	_, increment := colDiff.Changes["identity_increment"]
	return start || increment
}

// serialSequence builds the ALTER SEQUENCE that gives the sequence of field,
// a column of table, the start and the increment field declares. For a column
// of a table the plan creates it answers false when the declaration is the
// default a new sequence already has; for one of a table that exists it is
// asked only where the comparison found a difference, and answers the
// statement even for the default, which is what moves an altered sequence
// back.
func (p *Planner) serialSequence(
	diff *difftypes.SchemaDiff,
	table string,
	field schemamodel.Field,
	created bool,
) (*ast.AlterSerialSequenceNode, bool, error) {
	subject := fmt.Sprintf("the sequence of column %q of table %q", field.Name, table)
	serialType, serial := p.serialTypeOf(field)
	if !serial {
		if created {
			return nil, false, nil
		}
		return nil, false, refuseFact(subject, "the column is not a Serial column, so it has no sequence to change")
	}
	settings, err := ydbsequence.Parse(field.IdentityStart, field.IdentityIncrement)
	if err != nil {
		return nil, false, refuseFact(subject, err.Error())
	}
	if created && settings.IsDefault() {
		return nil, false, nil
	}
	if !p.caps.Has(capability.SerialSequenceOptions) {
		return nil, false, refuseKey(capability.SerialSequenceOptions, "giving "+subject+" a start or an increment")
	}
	if reason := ydbsequence.RangeRefusal(serialType, settings, p.caps); reason != "" {
		return nil, false, refuseKey(capability.SerialSequenceKeepsRange, "giving "+subject+" a start or an increment ("+
			reason+")")
	}
	if diff.CurrentDatabasePath == "" {
		return nil, false, refuseFact(subject, fmt.Sprintf("giving it start %d and increment %d takes an ALTER "+
			"SEQUENCE, which names the sequence only by its absolute path, beginning with the database's own, and "+
			"this plan was not made against a read of a database that gives one; plan against the database with "+
			"schema apply or migrations diff", settings.Start, settings.Increment))
	}
	ref, ok := tableref.Parse(table)
	if !ok {
		ref = tableref.Ref{Name: table}
	}
	return &ast.AlterSerialSequenceNode{
		Table:     table,
		Column:    field.Name,
		Path:      ydbsequence.Path(diff.CurrentDatabasePath, ref.Schema, ref.Name, field.Name),
		Start:     settings.Start,
		Increment: settings.Increment,
		Restart:   created && settings.Start != 1,
	}, true, nil
}

// serialTypeOf answers the YDB Serial type the renderer writes field as, and
// false for a column that is not a Serial: one declared as SERIAL, BIGSERIAL
// or SMALLSERIAL, or as an integer YDB has a Serial for with auto_increment or
// an identity.
func (p *Planner) serialTypeOf(field schemamodel.Field) (string, bool) {
	mapping, err := ydbtype.Map(field.Type, p.caps)
	if err != nil {
		return "", false
	}
	if mapping.Serial {
		return mapping.Type, true
	}
	if !field.AutoInc && field.IdentityGeneration == "" {
		return "", false
	}
	return ydbtype.SerialFor(mapping.Type)
}

// withoutPlannedSequenceSettings takes the start and the increment off the
// columns of table whose sequence plan writes an ALTER SEQUENCE for, so the
// renderer writes the CREATE TABLE without reporting them as dropped. A column
// the plan writes nothing for keeps them, and the renderer reports what it
// cannot write rather than losing it.
func withoutPlannedSequenceSettings(table *ast.CreateTableNode, planned []*ast.AlterSerialSequenceNode) {
	for _, node := range planned {
		for _, column := range table.Columns {
			if column.Name == node.Column {
				column.IdentityStart = ""
				column.IdentityIncrement = ""
			}
		}
	}
}
