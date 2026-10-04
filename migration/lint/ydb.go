package lint

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbsequence"
	"ptah.run/internal/yqlddl"
	"ptah.run/internal/yqlquery"
)

// The YD family: YDB statements the server refuses, or applies with an effect
// the statement does not state. Each rule reads YQL through internal/yqlddl,
// which `ptah sql lint` reads it through too, and each trap was measured on
// YDB 26.2.1.14 and 25.1.4.7 with the answers quoted at the rule.
//
// Every rule here describes whether a statement can run or what it does when
// it runs, which the server decides the same way in either direction, so each
// one reads the down half of a migration as well.
//
// YD104, YD105 and YD106 need to know what the table looks like before the
// statement: whether an index uses a column, which column the TTL reads,
// what its minimum partition count is, which views read a table. They read
// that state from the directory itself, whether or not the run has a dev
// database: the up migrations before the analyzed version, in version order,
// then the statements of the file before the one analyzed. A table created
// outside the directory is unknown to it, and an unknown table reports nothing.
//
// A run that names no dialect runs every rule, YD included, and reads the
// text with the hybrid lexer, which does not read YQL, against a target that
// answers no capability. A YD rule says nothing there ([ydbRun]): judging
// another dialect's SQL by what YDB refuses would report hazards that are not
// there, and YD103 would take DD101's place on every NOT NULL column.

// ydbRules is the YD family.
func ydbRules() []Rule {
	return []Rule{
		ydbUniqueIndexOnExistingTableRule(),
		ydbMixedQueryRule(),
		ydbAddColumnRefusedRule(),
		ydbDropUsedColumnRule(),
		ydbPartitionMinimumResetRule(),
		ydbViewOrphanedRule(),
		ydbNarrowSerialSequenceRule(),
		ydbReplayedRestartRule(),
		ydbMovedTableWithChangefeedRule(),
	}
}

// ydbOnly restricts a rule to YDB.
var ydbOnly = []string{platform.YDB}

// ydbRun reports whether the run targets YDB, which a YD rule needs before it
// reads anything; see the family comment above.
func ydbRun(target Target) bool {
	return target.Dialect == platform.YDB
}

// ydbUniqueIndexOnExistingTableRule reports a unique index added to a table
// that exists, which a target without
// [capability.UniqueIndexOnExistingTable] refuses. A table the same migration
// created exists too by the time ALTER TABLE runs, since every scheme
// statement is a query of its own. Measured:
//
//	26.2.1.14, flag off  Failed item check: Adding a unique index to an existing table is disabled
//	25.1.4.7             Unknown index type: syncGlobalUnique
//
// on an empty table and on one holding rows, while the same index declared in
// CREATE TABLE is accepted on both. With EnableAddUniqueIndex on, 26.2.1.14
// builds it over an empty table and fails on a duplicate (`Duplicate key
// found`), which is MF101's and MF102's question rather than this rule's. On a
// target that refuses the index, this rule replaces theirs.
func ydbUniqueIndexOnExistingTableRule() Rule {
	return Rule{
		Code:          "YD101",
		Title:         "unique index added to an existing table",
		Severity:      SeverityError,
		Dialects:      ydbOnly,
		AppliesToDown: true,
		Subsumes:      []string{"MF101", "MF102"},
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !ydbRun(stmt.Target) {
				return false, ""
			}
			read := yqlddl.Read(stmt.SQL)
			action, missing := missingRequirement(read, stmt.Target, capability.UniqueIndexOnExistingTable)
			if !missing {
				return false, ""
			}
			return true, fmt.Sprintf(
				"ADD INDEX %s adds a unique index to %s, a table that exists already, which needs target capability %s, "+
					"unavailable on this %s target; declare the unique index in the CREATE TABLE that creates the table",
				action.Index.Name, read.Name, capability.UniqueIndexOnExistingTable, platform.YDB)
		},
	}
}

// ydbMixedQueryRule reports a block or an action call that runs a scheme
// statement and a statement that reads or writes a table in one query, which
// YDB refuses whole; [yqlquery.Reader.Mixed] has the measurements. The
// migrator runs every other scheme statement as a query of its own, so only
// those two shapes can mix.
func ydbMixedQueryRule() Rule {
	return Rule{
		Code:     "YD102",
		Title:    "scheme and data statements in one query",
		Severity: SeverityError,
		Dialects: ydbOnly,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) {
				return nil
			}
			var reader yqlquery.Reader
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if !reader.Mixed(stmt.SQL) {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "YD102",
					Title:    "scheme and data statements in one query",
					Severity: SeverityError,
					File:     file.Path,
					Line:     stmt.Line,
					Message: "this statement runs a scheme statement and a statement that reads or writes a table in one query, " +
						"which YDB refuses whole (Queries with mixed data and scheme operations are not supported); " +
						"move the scheme statement out of the block or action into a statement of its own",
					Context: statementFindingContext(i),
				})
			}
			return findings
		},
	}
}

// ydbAddColumnRefusedRule reports an ADD COLUMN YDB refuses. Measured on
// 26.2.1.14 and 25.1.4.7, on an empty table and on one holding rows:
//
//	ADD COLUMN c Int64 NOT NULL           Cannot add not null column without default value
//	ADD COLUMN c Int64 [NOT NULL] DEFAULT 7
//	                                      26.2.1.14: accepted, existing rows take the default
//	                                      25.1.4.7: Column addition with default value is not supported now.
//
// The first is refused on every line, the second where the target lacks
// [capability.AddColumnWithDefault]. Neither depends on the rows, so a table
// this migration created is no exception, which is where it differs from
// DD101; it replaces DD101 on the statement.
func ydbAddColumnRefusedRule() Rule {
	return Rule{
		Code:          "YD103",
		Title:         "column added in a form YDB refuses",
		Severity:      SeverityError,
		Dialects:      ydbOnly,
		AppliesToDown: true,
		Subsumes:      []string{"DD101"},
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !ydbRun(stmt.Target) {
				return false, ""
			}
			read := yqlddl.Read(stmt.SQL)
			if action, missing := missingRequirement(read, stmt.Target, capability.AddColumnWithDefault); missing {
				return true, fmt.Sprintf(
					"ADD COLUMN %s gives the column a default, which needs target capability %s, unavailable on this %s target; "+
						"add the column without a default and write its values in a data statement",
					action.Column.Name, capability.AddColumnWithDefault, platform.YDB)
			}
			withDefault := stmt.Target.Capabilities.Has(capability.AddColumnWithDefault)
			for _, action := range read.Actions {
				if action.Kind != yqlddl.AddColumn {
					continue
				}
				column := action.Column
				switch {
				case column.NotNull && !column.Default && withDefault:
					return true, fmt.Sprintf(
						"ADD COLUMN %s is NOT NULL without a default, which YDB refuses even on an empty table "+
							"(Cannot add not null column without default value); give it a DEFAULT, which existing rows take",
						column.Name)
				case column.NotNull && !column.Default:
					return true, fmt.Sprintf(
						"ADD COLUMN %s is NOT NULL without a default, which YDB refuses even on an empty table "+
							"(Cannot add not null column without default value), and a default needs target capability %s, "+
							"unavailable on this %s target; add the column as nullable",
						column.Name, capability.AddColumnWithDefault, platform.YDB)
				}
			}
			return false, ""
		},
	}
}

// ydbDropUsedColumnRule reports a DROP COLUMN of a column an index keys or
// covers, or of the column the table's TTL reads. Measured on 26.2.1.14 and
// 25.1.4.7:
//
//	key column of an index   Impossible drop column because table has an index with that column
//	covered column           Impossible drop column because table index covers that column
//	TTL column               Can't drop TTL column: 'ts', disable TTL first
//
// After ALTER TABLE ... DROP INDEX, and after ALTER TABLE ... RESET (TTL),
// each drop is accepted.
func ydbDropUsedColumnRule() Rule {
	return Rule{
		Code:     "YD104",
		Title:    "column dropped while an index or the TTL uses it",
		Severity: SeverityError,
		Dialects: ydbOnly,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) {
				return nil
			}
			state := file.ydbBefore.clone()
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				read := yqlddl.Read(stmt.SQL)
				if read.Kind != yqlddl.AlterTable {
					state.apply(read)
					continue
				}
				table := state.table(read.Name)
				for _, action := range read.Actions {
					if action.Kind == yqlddl.DropColumn {
						if message, used := table.dropRefusal(action.Column.Name); used {
							findings = append(findings, Finding{
								Rule:     "YD104",
								Title:    "column dropped while an index or the TTL uses it",
								Severity: SeverityError,
								File:     file.Path,
								Line:     stmt.Line,
								Message:  fmt.Sprintf("DROP COLUMN %s: %s", action.Column.Name, message),
								Context: statementFindingContext(i, Subject{
									Kind: SubjectColumn, Name: action.Column.Name, Parent: read.Name,
								}),
							})
						}
					}
					table = table.applyAction(action)
				}
				state.store(read, table)
			}
			return findings
		},
	}
}

// ydbPartitionMinimumResetRule reports an ALTER TABLE that turns
// AUTO_PARTITIONING_BY_SIZE or AUTO_PARTITIONING_BY_LOAD on without setting
// AUTO_PARTITIONING_MIN_PARTITIONS_COUNT in the same statement. Measured on
// 26.2.1.14 and 25.1.4.7 against a table whose minimum was 4:
//
//	SET (AUTO_PARTITIONING_BY_SIZE = ENABLED)        minimum 1, also when it was enabled already
//	SET (AUTO_PARTITIONING_BY_LOAD = ENABLED)        minimum 1
//	SET AUTO_PARTITIONING_BY_LOAD ENABLED            minimum 1
//	SET (..._BY_SIZE = ENABLED, ..._MIN_PARTITIONS_COUNT = 4)       minimum 4
//	SET (..._MIN_PARTITIONS_COUNT = 4), SET (..._BY_LOAD = ENABLED) minimum 4
//	SET (..._BY_LOAD = DISABLED), or a size or maximum setting      minimum 4
//
// With the minimum at 1, YDB may merge the table down to one partition. A
// table whose minimum was 1 already loses nothing, so the rule stays silent
// where the directory's own history says so (see [ydbTable.minPartitions]),
// and warns where it does not know.
func ydbPartitionMinimumResetRule() Rule {
	return Rule{
		Code:     "YD105",
		Title:    "partitioning change resets the minimum partition count",
		Severity: SeverityWarning,
		Dialects: ydbOnly,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) {
				return nil
			}
			state := file.ydbBefore.clone()
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				read := yqlddl.Read(stmt.SQL)
				if message, resets := partitionMinimumReset(read, state.table(read.Name)); resets {
					findings = append(findings, Finding{
						Rule:     "YD105",
						Title:    "partitioning change resets the minimum partition count",
						Severity: SeverityWarning,
						File:     file.Path,
						Line:     stmt.Line,
						Message:  message,
						Context:  statementFindingContext(i, Subject{Kind: SubjectTable, Name: read.Name}),
					})
				}
				state.apply(read)
			}
			return findings
		},
	}
}

// partitionMinimumReset says why an ALTER TABLE of table resets its minimum
// partition count to 1, and reports whether it does. A table already at a
// minimum of 1 has nothing to lose.
func partitionMinimumReset(read yqlddl.Statement, table ydbTable) (string, bool) {
	if read.Kind != yqlddl.AlterTable || (table.minKnown && table.minPartitions == 1) {
		return "", false
	}
	enabled := ""
	for _, setting := range settingsSet(read) {
		switch {
		case setting.Name == ydbMinPartitions:
			return "", false
		case enabled == "" && setting.Value == "ENABLED" &&
			(setting.Name == "AUTO_PARTITIONING_BY_SIZE" || setting.Name == "AUTO_PARTITIONING_BY_LOAD"):
			enabled = setting.Name
		}
	}
	if enabled == "" {
		return "", false
	}
	return fmt.Sprintf(
		"setting %s = ENABLED resets %s of %s to 1, so YDB may merge its partitions down to one; "+
			"set %s in the same ALTER TABLE to keep it",
		enabled, ydbMinPartitions, read.Name, ydbMinPartitions), true
}

// createdMinPartitions is the minimum partition count a CREATE TABLE leaves,
// and whether it is known. Measured on 26.2.1.14 and 25.1.4.7: a table
// created with no partitioning setting, or with AUTO_PARTITIONING_BY_SIZE or
// _BY_LOAD alone, has a minimum of 1; UNIFORM_PARTITIONS = 4 leaves 4, also
// beside AUTO_PARTITIONING_BY_SIZE = ENABLED in either order;
// PARTITION_AT_KEYS with two split points leaves 3, and with three composite
// ones 4; and AUTO_PARTITIONING_MIN_PARTITIONS_COUNT wins over either, so
// UNIFORM_PARTITIONS = 4 with a minimum of 2 leaves 2. A value the reader
// cannot read, or both kinds of split together, leaves it unknown.
func createdMinPartitions(settings []yqlddl.Setting) (int, bool) {
	var explicit, uniform, atKeys int
	for _, setting := range settings {
		switch setting.Name {
		case ydbMinPartitions:
			value, ok := partitionCount(setting)
			if !ok {
				return 0, false
			}
			explicit = value
		case "UNIFORM_PARTITIONS":
			value, ok := partitionCount(setting)
			if !ok {
				return 0, false
			}
			uniform = value
		case "PARTITION_AT_KEYS":
			if setting.Items == 0 {
				return 0, false
			}
			atKeys = setting.Items + 1
		}
	}
	switch {
	case explicit > 0:
		return explicit, true
	case uniform > 0 && atKeys > 0:
		return 0, false
	case uniform > 0:
		return uniform, true
	case atKeys > 0:
		return atKeys, true
	default:
		return 1, true
	}
}

// partitionCount reads a setting whose value is a count of partitions.
func partitionCount(setting yqlddl.Setting) (int, bool) {
	value, err := strconv.Atoi(setting.Value)
	return value, err == nil && value > 0
}

// alteredMinPartitions is the minimum partition count an ALTER TABLE leaves
// on table: the count it sets, wherever in the statement; 1 when it turns
// auto partitioning by size or by load on without one; and the table's own
// otherwise. YDB refuses RESET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT) and
// a change of UNIFORM_PARTITIONS, measured on both lines.
func alteredMinPartitions(read yqlddl.Statement, table ydbTable) (int, bool) {
	settings := settingsSet(read)
	for _, setting := range settings {
		if setting.Name == ydbMinPartitions {
			return partitionCount(setting)
		}
	}
	for _, setting := range settings {
		if setting.Value == "ENABLED" &&
			(setting.Name == "AUTO_PARTITIONING_BY_SIZE" || setting.Name == "AUTO_PARTITIONING_BY_LOAD") {
			return 1, true
		}
	}
	return table.minPartitions, table.minKnown
}

// missingRequirement returns the first action of a statement that needs key,
// and reports whether the target lacks it; see [yqlddl.Statement.Requirements],
// which `ptah sql lint` judges a YQL statement by too.
func missingRequirement(read yqlddl.Statement, target Target, key capability.Capability) (yqlddl.Action, bool) {
	if target.Capabilities.Has(key) {
		return yqlddl.Action{}, false
	}
	for _, requirement := range read.Requirements() {
		if requirement.Capability == key {
			return read.Actions[requirement.Action], true
		}
	}
	return yqlddl.Action{}, false
}

// ydbMinPartitions is the setting the partitioning reset clears.
const ydbMinPartitions = "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT"

// settingsSet returns every setting the SET actions of an ALTER TABLE name.
func settingsSet(read yqlddl.Statement) []yqlddl.Setting {
	var settings []yqlddl.Setting
	for _, action := range read.Actions {
		if action.Kind == yqlddl.SetSettings {
			settings = append(settings, action.Settings...)
		}
	}
	return settings
}

// ydbViewOrphanedRule reports a DROP TABLE of a table a view reads, and an
// ALTER TABLE ... RENAME TO of one. A view reads its table by path when it is
// read, so YDB keeps a view whose table is dropped or moved, and every read of
// the view then fails. Measured on 26.2.1.14 and 25.1.4.7: after `CREATE VIEW
// vv WITH (security_invoker = TRUE) AS SELECT id FROM vbase`, `DROP TABLE
// vbase` succeeds, and `SELECT * FROM vv` answers `Cannot find table
// 'db.[/local/vbase]' because it does not exist`. After `ALTER TABLE vbase
// RENAME TO vmoved` the read answers the same, and renaming the table back
// makes the view read it again.
//
// The renames of a table rebuild are left out (see [fileRebuilds]): the copy
// takes the table's name back in the next statement, and the view reads it.
func ydbViewOrphanedRule() Rule {
	return Rule{
		Code:     "YD106",
		Title:    "table dropped or renamed while a view reads it",
		Severity: SeverityError,
		Dialects: ydbOnly,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) {
				return nil
			}
			rebuilds := fileRebuilds(file)
			state := file.ydbBefore.clone()
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				read := yqlddl.Read(stmt.SQL)
				views := state.viewsReading(read.Name)
				var message string
				switch {
				case len(views) == 0:
				case read.Kind == yqlddl.DropTable:
					message = fmt.Sprintf(
						"DROP TABLE %s leaves %s reading a table that does not exist: YDB keeps a view whose table is dropped, "+
							"and every read of it fails; drop or recreate %s first",
						read.Name, viewList(views), pronounFor(views))
				case read.Kind == yqlddl.AlterTable && !rebuilds.renames[i]:
					if newName, renamed := tableRenamedTo(read); renamed {
						message = fmt.Sprintf(
							"ALTER TABLE %s RENAME TO %s leaves %s reading a table that does not exist: a view reads its table "+
								"by path, so every read of it fails until a table takes the name %s again; recreate %s over %s",
							read.Name, newName, viewList(views), read.Name, pronounFor(views), newName)
					}
				}
				if message != "" {
					findings = append(findings, Finding{
						Rule:     "YD106",
						Title:    "table dropped or renamed while a view reads it",
						Severity: SeverityError,
						File:     file.Path,
						Line:     stmt.Line,
						Message:  message,
						Context:  statementFindingContext(i, Subject{Kind: SubjectTable, Name: read.Name}),
					})
				}
				state.apply(read)
			}
			return findings
		},
	}
}

// ydbMovedTableWithChangefeedRule reports an ALTER TABLE ... RENAME TO of a
// table that carries a changefeed, which YDB refuses. Measured on 25.1.4.7,
// 25.2.1.24, 25.3.1.25, 25.4.1.15, 26.1.1.22 and 26.2.1.14:
//
//	ALTER TABLE t RENAME TO u   Cannot move table with cdc streams
//
// with one changefeed or several, and after DROP CHANGEFEED of each the same
// rename is accepted. The table's changefeeds are read from the directory's
// own migrations, as YD104 reads its indexes.
func ydbMovedTableWithChangefeedRule() Rule {
	return Rule{
		Code:     "YD109",
		Title:    "table renamed while it carries a changefeed",
		Severity: SeverityError,
		Dialects: ydbOnly,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) {
				return nil
			}
			state := file.ydbBefore.clone()
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				read := yqlddl.Read(stmt.SQL)
				if read.Kind != yqlddl.AlterTable {
					state.apply(read)
					continue
				}
				table := state.table(read.Name)
				for _, action := range read.Actions {
					if action.Kind == yqlddl.RenameTable && len(table.changefeeds) > 0 {
						findings = append(findings, Finding{
							Rule:     "YD109",
							Title:    "table renamed while it carries a changefeed",
							Severity: SeverityError,
							File:     file.Path,
							Line:     stmt.Line,
							Message: fmt.Sprintf(
								"RENAME TO moves %s, which carries %s, and YDB moves no table that carries one "+
									"(Cannot move table with cdc streams); drop the changefeeds first and add them again after "+
									"the move, which restarts each stream",
								read.Name, changefeedList(table.changefeeds)),
							Context: statementFindingContext(i, Subject{Kind: SubjectTable, Name: read.Name}),
						})
					}
					table = table.applyAction(action)
				}
				state.store(read, table)
			}
			return findings
		},
	}
}

// changefeedList names a table's changefeeds in a finding.
func changefeedList(changefeeds []string) string {
	if len(changefeeds) == 1 {
		return "changefeed " + changefeeds[0]
	}
	return "changefeeds " + strings.Join(changefeeds, ", ")
}

// tableRenamedTo returns the name an ALTER TABLE moves its table to, and reports
// whether it renames it.
func tableRenamedTo(read yqlddl.Statement) (string, bool) {
	for _, action := range read.Actions {
		if action.Kind == yqlddl.RenameTable && action.NewName != "" {
			return action.NewName, true
		}
	}
	return "", false
}

func viewList(views []string) string {
	if len(views) == 1 {
		return "view " + views[0]
	}
	return "views " + strings.Join(views, ", ")
}

func pronounFor(views []string) string {
	if len(views) == 1 {
		return "it"
	}
	return "them"
}

// ydbSchema is the part of a YDB schema the YD rules read, as the directory's
// own statements leave it.
type ydbSchema struct {
	tables map[string]ydbTable
	// views are the tables each view reads, by view name.
	views map[string][]string
}

// ydbTable is what the rules read about one table.
type ydbTable struct {
	indexes []yqlddl.Index
	// ttl is the column the TTL reads, empty when the table has none.
	ttl string
	// columns are the table's columns, in order, when columnsKnown: a table
	// the directory created has them all, and one it only altered does not.
	columns      []string
	columnsKnown bool
	// minPartitions is the table's AUTO_PARTITIONING_MIN_PARTITIONS_COUNT
	// when minKnown: set by the CREATE TABLE the directory ran, or by an
	// ALTER TABLE that sets the count or resets it.
	minPartitions int
	minKnown      bool
	// serials are the YDB types of the table's Serial columns, by column:
	// Serial, BigSerial or SmallSerial, whichever alias the CREATE TABLE
	// wrote.
	serials map[string]string
	// restarts are, by Serial column, the value the last RESTART of the
	// column's sequence moved it to, written as the statement wrote it.
	restarts map[string]string
	// changefeeds are the table's changefeeds, in the order they were added.
	changefeeds []string
}

func (s *ydbSchema) clone() *ydbSchema {
	cloned := &ydbSchema{tables: make(map[string]ydbTable), views: make(map[string][]string)}
	if s == nil {
		return cloned
	}
	for name, table := range s.tables {
		cloned.tables[name] = ydbTable{
			indexes: slices.Clone(table.indexes), ttl: table.ttl,
			columns: slices.Clone(table.columns), columnsKnown: table.columnsKnown,
			minPartitions: table.minPartitions, minKnown: table.minKnown,
			serials: maps.Clone(table.serials), restarts: maps.Clone(table.restarts),
			changefeeds: slices.Clone(table.changefeeds),
		}
	}
	for name, reads := range s.views {
		cloned.views[name] = slices.Clone(reads)
	}
	return cloned
}

// table returns what the schema knows about a table, which is nothing for one
// the directory never named.
func (s *ydbSchema) table(name string) ydbTable {
	return s.tables[name]
}

// store records a table an ALTER TABLE changed, under its new name when the
// statement renamed it.
func (s *ydbSchema) store(read yqlddl.Statement, table ydbTable) {
	if read.Name == "" {
		return
	}
	name := read.Name
	for _, action := range read.Actions {
		if action.Kind == yqlddl.RenameTable && action.NewName != "" {
			delete(s.tables, name)
			name = action.NewName
		}
	}
	s.tables[name] = table
}

// apply changes the schema the way a statement does.
func (s *ydbSchema) apply(read yqlddl.Statement) {
	if read.Name == "" {
		return
	}
	switch read.Kind {
	case yqlddl.CreateTable:
		if _, exists := s.tables[read.Name]; exists && read.IfExists {
			return
		}
		columns := make([]string, 0, len(read.Columns))
		for _, column := range read.Columns {
			columns = append(columns, column.Name)
		}
		minimum, minKnown := createdMinPartitions(read.Settings)
		s.tables[read.Name] = ydbTable{
			indexes: slices.Clone(read.Indexes), ttl: read.TTLColumn, columns: columns, columnsKnown: true,
			minPartitions: minimum, minKnown: minKnown, serials: serialColumns(read),
		}
	case yqlddl.AlterTable:
		table := s.table(read.Name)
		for _, action := range read.Actions {
			table = table.applyAction(action)
		}
		table.minPartitions, table.minKnown = alteredMinPartitions(read, table)
		s.store(read, table)
	case yqlddl.DropTable:
		delete(s.tables, read.Name)
	case yqlddl.CreateView:
		if _, exists := s.views[read.Name]; exists && read.IfExists {
			return
		}
		s.views[read.Name] = slices.Clone(read.Reads)
	case yqlddl.DropView:
		delete(s.views, read.Name)
	case yqlddl.AlterSequence:
		name, column, owned := s.sequenceOwner(read.Name)
		if !owned || !read.Restart {
			return
		}
		table := s.tables[name]
		table.restarts = maps.Clone(table.restarts)
		if table.restarts == nil {
			table.restarts = make(map[string]string)
		}
		table.restarts[column] = cmp.Or(read.RestartWith, "its start")
		s.tables[name] = table
	}
}

// serialTypes are the spellings YQL takes for a Serial column, by the type
// each makes. Measured on 26.2.1.14 and 25.1.4.7, in any letter case: a
// SmallSerial or Serial2 column is Int16, a Serial or Serial4 column Int32,
// and a BigSerial or Serial8 column Int64.
var serialTypes = map[string]string{
	"smallserial": "SmallSerial", "serial2": "SmallSerial",
	"serial": "Serial", "serial4": "Serial",
	"bigserial": "BigSerial", "serial8": "BigSerial",
}

// serialColumns returns the Serial columns a CREATE TABLE declares, by
// column, with the type each makes.
func serialColumns(read yqlddl.Statement) map[string]string {
	serials := make(map[string]string)
	for _, column := range read.Columns {
		if serial, ok := serialTypes[strings.ToLower(column.Type)]; ok {
			serials[column.Name] = serial
		}
	}
	return serials
}

// sequenceOwner returns the table and the Serial column whose sequence path
// names. The last element of the path is `_serial_column_<column>`, and the
// elements before it end with the table: YDB takes the path from the cluster
// root, so the database's own path comes first, and the directory names its
// tables from below that. Of the tables the directory knows, the longest that
// ends the path is the owner.
func (s *ydbSchema) sequenceOwner(path string) (table, column string, ok bool) {
	slash := strings.LastIndex(path, "/")
	if slash < 0 {
		return "", "", false
	}
	column, ok = strings.CutPrefix(path[slash+1:], ydbsequence.Name(""))
	if !ok || column == "" {
		return "", "", false
	}
	owner := path[:slash]
	for _, name := range slices.Sorted(maps.Keys(s.tables)) {
		if (owner == name || strings.HasSuffix(owner, "/"+name)) && len(name) > len(table) {
			table = name
		}
	}
	return table, column, table != ""
}

// ydbNarrowSerialSequenceRule reports an ALTER SEQUENCE on the sequence of a
// Serial or SmallSerial column the directory created. Measured on 26.2.1.14
// and 25.1.4.7, any ALTER SEQUENCE, `INCREMENT BY 1` included, raises the
// sequence's maximum from the column's to the Int64 maximum, and nothing
// lowers it again:
//
//	before, SmallSerial at 32767   doesn't have any more values available
//	after,  SmallSerial at 32767   the next row is stored with id -32768
//	after,  Serial at 2147483647   the next row is stored with id -2147483648
//
// The statement succeeds, and the column stores the value past its range as a
// negative number without an error. A BigSerial's maximum is the Int64
// maximum already.
func ydbNarrowSerialSequenceRule() Rule {
	return Rule{
		Code:          "YD107",
		Title:         "ALTER SEQUENCE widens a 16-bit or 32-bit Serial",
		Severity:      SeverityError,
		Dialects:      ydbOnly,
		AppliesToDown: true,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) || file.Target.Capabilities.Has(capability.SerialSequenceKeepsRange) {
				return nil
			}
			state := file.ydbBefore.clone()
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				read := yqlddl.Read(stmt.SQL)
				if read.Kind == yqlddl.AlterSequence {
					table, column, owned := state.sequenceOwner(read.Name)
					serial := state.table(table).serials[column]
					if owned && (serial == "Serial" || serial == "SmallSerial") {
						findings = append(findings, Finding{
							Rule:     "YD107",
							Title:    "ALTER SEQUENCE widens a 16-bit or 32-bit Serial",
							Severity: SeverityError,
							File:     file.Path,
							Line:     stmt.Line,
							Message: fmt.Sprintf(
								"ALTER SEQUENCE %s raises the maximum of the sequence of %s column %s.%s to the Int64 maximum, "+
									"and the column then stores the value past its own maximum as a negative number without "+
									"an error; declare the column BigSerial to give its sequence a start or an increment",
								read.Name, serial, table, column),
							Context: statementFindingContext(i, Subject{Kind: SubjectColumn, Name: column, Parent: table}),
						})
					}
				}
				state.apply(read)
			}
			return findings
		},
	}
}

// ydbReplayedRestartRule reports an ALTER SEQUENCE without a RESTART of its
// own on a sequence an earlier statement restarted. Measured on 26.2.1.14 and
// 25.1.4.7: after `RESTART WITH 500` and a row holding 500, `ALTER SEQUENCE
// ... INCREMENT BY 1` succeeds and moves the next value back to 500, and the
// next insert fails with `Conflict with existing key`. A RESTART of the
// statement's own sets a new value instead.
func ydbReplayedRestartRule() Rule {
	return Rule{
		Code:          "YD108",
		Title:         "ALTER SEQUENCE replays an earlier RESTART",
		Severity:      SeverityError,
		Dialects:      ydbOnly,
		AppliesToDown: true,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) {
				return nil
			}
			state := file.ydbBefore.clone()
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				read := yqlddl.Read(stmt.SQL)
				if read.Kind == yqlddl.AlterSequence && !read.Restart {
					table, column, owned := state.sequenceOwner(read.Name)
					if restart, restarted := state.table(table).restarts[column]; owned && restarted {
						findings = append(findings, Finding{
							Rule:     "YD108",
							Title:    "ALTER SEQUENCE replays an earlier RESTART",
							Severity: SeverityError,
							File:     file.Path,
							Line:     stmt.Line,
							Message: fmt.Sprintf(
								"ALTER SEQUENCE %s alters a sequence an earlier statement restarted at %s, and YDB replays that "+
									"restart on every later ALTER SEQUENCE, so the next row of %s takes %s again and fails on a key "+
									"a row already holds (Conflict with existing key); restart it in this statement at a value "+
									"past every row, or leave it as it is",
								read.Name, restart, table, restart),
							Context: statementFindingContext(i, Subject{Kind: SubjectColumn, Name: column, Parent: table}),
						})
					}
				}
				state.apply(read)
			}
			return findings
		},
	}
}

// viewsReading returns the views that read a table, sorted.
func (s *ydbSchema) viewsReading(table string) []string {
	var views []string
	for view, reads := range s.views {
		if slices.Contains(reads, table) {
			views = append(views, view)
		}
	}
	slices.Sort(views)
	return views
}

// applyAction returns the table as one ALTER TABLE action leaves it.
func (t ydbTable) applyAction(action yqlddl.Action) ydbTable {
	indexes := slices.Clone(t.indexes)
	switch action.Kind {
	case yqlddl.AddColumn:
		t.columns = append(slices.Clone(t.columns), action.Column.Name)
	case yqlddl.DropColumn:
		t.columns = slices.DeleteFunc(slices.Clone(t.columns), func(name string) bool { return name == action.Column.Name })
	case yqlddl.AddIndex:
		indexes = slices.DeleteFunc(indexes, func(index yqlddl.Index) bool { return index.Name == action.Index.Name })
		indexes = append(indexes, action.Index)
	case yqlddl.DropIndex:
		indexes = slices.DeleteFunc(indexes, func(index yqlddl.Index) bool { return index.Name == action.Index.Name })
	case yqlddl.RenameIndex:
		for i := range indexes {
			if indexes[i].Name == action.Index.Name {
				indexes[i].Name = action.NewName
			}
		}
	case yqlddl.SetSettings:
		for _, setting := range action.Settings {
			if setting.Name == "TTL" {
				t.ttl = setting.Column
			}
		}
	case yqlddl.ResetSettings:
		for _, setting := range action.Settings {
			if setting.Name == "TTL" {
				t.ttl = ""
			}
		}
	case yqlddl.AddChangefeed:
		t.changefeeds = append(slices.Clone(t.changefeeds), action.Changefeed)
	case yqlddl.DropChangefeed:
		t.changefeeds = slices.DeleteFunc(slices.Clone(t.changefeeds), func(name string) bool {
			return name == action.Changefeed
		})
	}
	t.indexes = indexes
	return t
}

// dropRefusal says why YDB refuses to drop a column of the table, and reports
// whether it does.
func (t ydbTable) dropRefusal(column string) (string, bool) {
	for _, index := range t.indexes {
		switch {
		case slices.Contains(index.Columns, column):
			return fmt.Sprintf("index %s keys the column, and YDB refuses to drop it "+
				"(Impossible drop column because table has an index with that column); drop the index first", index.Name), true
		case slices.Contains(index.Cover, column):
			return fmt.Sprintf("index %s covers the column, and YDB refuses to drop it "+
				"(Impossible drop column because table index covers that column); drop the index first", index.Name), true
		}
	}
	if t.ttl != "" && t.ttl == column {
		return "the table's TTL reads the column, and YDB refuses to drop it (Can't drop TTL column, disable TTL first); " +
			"run ALTER TABLE ... RESET (TTL) first", true
	}
	return "", false
}

// ydbHistory computes, for every migration file of a YDB analysis, the schema
// the directory's own up migrations leave before the file runs: for an up
// file, the up migrations of lower versions; for a down file, those and the up
// migration of its own version, which the down half undoes. Files are read in
// version order, as the migrator applies them.
func ydbHistory(files []File) {
	ups := make([]int, 0, len(files))
	for i := range files {
		if files[i].IsUp && files[i].Version > 0 && !files[i].Repeatable {
			ups = append(ups, i)
		}
	}
	slices.SortStableFunc(ups, func(a, b int) int { return cmp.Compare(files[a].Version, files[b].Version) })
	state := (*ydbSchema)(nil).clone()
	after := make(map[int64]*ydbSchema, len(ups))
	for _, index := range ups {
		file := &files[index]
		file.ydbBefore = state.clone()
		for i := range file.Statements {
			state.apply(yqlddl.Read(file.Statements[i].SQL))
		}
		after[file.Version] = state.clone()
	}
	for i := range files {
		if !files[i].IsDown || files[i].Version <= 0 {
			continue
		}
		files[i].ydbBefore = stateThrough(ups, files, after, files[i].Version)
	}
}

// stateThrough is the schema after every up migration whose version is at
// most version.
func stateThrough(ups []int, files []File, after map[int64]*ydbSchema, version int64) *ydbSchema {
	var latest *ydbSchema
	for _, index := range ups {
		if files[index].Version > version {
			break
		}
		latest = after[files[index].Version]
	}
	return latest
}
