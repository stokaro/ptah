package lint

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
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
// Two of them need to know what the table looks like before the statement:
// whether an index uses a column, which column the TTL reads, which views read
// a table. A YDB database cannot be a dev database yet (stokaro/ptah#4015), so
// that state is read from the directory itself: the up migrations before the
// analyzed version, in version order, then the statements of the file before
// the one analyzed. A table created outside the directory is unknown to it,
// and an unknown table reports nothing.
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
// table whose minimum was 1 loses nothing, which the statement cannot say, so
// the rule warns.
func ydbPartitionMinimumResetRule() Rule {
	return Rule{
		Code:          "YD105",
		Title:         "partitioning change resets the minimum partition count",
		Severity:      SeverityWarning,
		Dialects:      ydbOnly,
		AppliesToDown: true,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !ydbRun(stmt.Target) {
				return false, ""
			}
			read := yqlddl.Read(stmt.SQL)
			if read.Kind != yqlddl.AlterTable {
				return false, ""
			}
			enabled := ""
			for _, setting := range settingsSet(read) {
				switch {
				case setting.Name == ydbMinPartitions:
					return false, ""
				case enabled == "" && setting.Value == "ENABLED" &&
					(setting.Name == "AUTO_PARTITIONING_BY_SIZE" || setting.Name == "AUTO_PARTITIONING_BY_LOAD"):
					enabled = setting.Name
				}
			}
			if enabled == "" {
				return false, ""
			}
			return true, fmt.Sprintf(
				"setting %s = ENABLED resets %s of %s to 1, so YDB may merge its partitions down to one; "+
					"set %s in the same ALTER TABLE to keep it",
				enabled, ydbMinPartitions, read.Name, ydbMinPartitions)
		},
	}
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

// ydbViewOrphanedRule reports a DROP TABLE of a table a view reads. YDB drops
// the table and keeps the view, and every read of the view then fails.
// Measured on 26.2.1.14 and 25.1.4.7: after `CREATE VIEW vv WITH
// (security_invoker = TRUE) AS SELECT id FROM vbase`, `DROP TABLE vbase`
// succeeds, and `SELECT * FROM vv` answers `Cannot find table
// 'db.[/local/vbase]' because it does not exist`.
func ydbViewOrphanedRule() Rule {
	return Rule{
		Code:     "YD106",
		Title:    "table dropped while a view reads it",
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
				if read.Kind == yqlddl.DropTable {
					if views := state.viewsReading(read.Name); len(views) > 0 {
						findings = append(findings, Finding{
							Rule:     "YD106",
							Title:    "table dropped while a view reads it",
							Severity: SeverityError,
							File:     file.Path,
							Line:     stmt.Line,
							Message: fmt.Sprintf(
								"DROP TABLE %s leaves %s reading a table that does not exist: YDB keeps a view whose table is dropped, "+
									"and every read of it fails; drop or recreate %s first",
								read.Name, viewList(views), pronounFor(views)),
							Context: statementFindingContext(i, Subject{Kind: SubjectTable, Name: read.Name}),
						})
					}
				}
				state.apply(read)
			}
			return findings
		},
	}
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
}

func (s *ydbSchema) clone() *ydbSchema {
	cloned := &ydbSchema{tables: make(map[string]ydbTable), views: make(map[string][]string)}
	if s == nil {
		return cloned
	}
	for name, table := range s.tables {
		cloned.tables[name] = ydbTable{indexes: slices.Clone(table.indexes), ttl: table.ttl}
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
		s.tables[read.Name] = ydbTable{indexes: slices.Clone(read.Indexes), ttl: read.TTLColumn}
	case yqlddl.AlterTable:
		table := s.table(read.Name)
		for _, action := range read.Actions {
			table = table.applyAction(action)
		}
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
	slices.SortStableFunc(ups, func(a, b int) int {
		switch {
		case files[a].Version < files[b].Version:
			return -1
		case files[a].Version > files[b].Version:
			return 1
		default:
			return 0
		}
	})
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
