package clickhouse

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/modelast"
	"ptah.run/internal/tableref"
)

// holdingPrefix starts the name of the table a rebuild parks a table's rows in.
// The name is fixed by the table's own, so a run that stopped half way leaves a
// table the next plan finds and the server refuses to overwrite, and the
// operator reads where the rows are.
const holdingPrefix = "ptah_rebuild_"

// holdingMarker starts the comment of a holding table, followed by the name of
// the table whose rows it holds. A planner that finds a table with this comment
// among the tables a plan drops refuses the plan: the table is what an
// unfinished rebuild left, and it may hold the only copy of the rows.
const holdingMarker = "ptah rebuild holding table for "

// rebuildWarning says what an operator reviewing the plan accepts. It is a
// comment of the plan, which every statement style keeps, and the reason of the
// step's effect.
const rebuildWarning = "the steps are not atomic, and rows written to the table while they run are lost"

// WithTableRebuild returns a copy of the planner that creates a table again,
// and copies its rows, where ClickHouse has no ALTER TABLE for the change,
// instead of refusing the change. request is how the operator asks for it, for
// the refusal of a planner not asked to name. The receiver is not modified.
func (p *Planner) WithTableRebuild(allow bool, request string) *Planner {
	cp := *p
	cp.rebuild = tableRebuildPolicy{allow: allow, request: request}
	return &cp
}

type tableRebuildPolicy struct {
	allow   bool
	request string
}

func (r tableRebuildPolicy) words() string {
	if r.request != "" {
		return r.request
	}
	return planext.DefaultTableRebuildRequest
}

// tableRebuild is one table the plan creates again, with the statements that
// do it. It replaces every other statement the plan has for the table.
type tableRebuild struct {
	name    string
	subject objectidentity.ID
	nodes   []ast.Node
	effect  schemaext.Effect
}

// tableRebuilds holds the rebuilds of one plan in the order the diff names the
// tables.
type tableRebuilds []tableRebuild

// has reports whether the plan creates the named table again.
func (r tableRebuilds) has(builder objectidentity.Builder, name string) bool {
	key := builder.Table(name).Key()
	return slices.ContainsFunc(r, func(rebuild tableRebuild) bool { return rebuild.subject.Key() == key })
}

// planRebuilds decides, for every modified table, whether ClickHouse can make
// its change with ALTER TABLE, and returns the rebuild of each table it cannot.
// Without the operator's request the first such table is refused, in words that
// name the request.
func (p *Planner) planRebuilds(diff *difftypes.SchemaDiff) (tableRebuilds, error) {
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(platform.ClickHouse))
	var rebuilds tableRebuilds
	for i := range diff.TablesModified {
		table := &diff.TablesModified[i]
		reasons := recreationReasons(table)
		if len(reasons) == 0 {
			continue
		}
		if engine, replicated := replicatedEngine(table); replicated {
			return nil, &ptaherr.CapabilityError{
				Dialect: platform.ClickHouse,
				Feature: "rebuilding a replicated table",
				Err:     ptaherr.ErrUnsupportedFeature,
				Message: fmt.Sprintf("unsupported feature: %s; Ptah does not create table %s again, because its engine %s is replicated. "+
					"The rebuild is not safe there. The holding table is local. The copy reads one replica, which may lag. "+
					"DROP TABLE ... SYNC removes one replica. The new table meets the metadata the other replicas keep at the same Keeper path. "+
					"Create the table again by hand, on every replica.",
					strings.Join(reasons, "; "), table.TableName, engine),
			}
		}
		if !p.rebuild.allow {
			return nil, &ptaherr.CapabilityError{
				Dialect: platform.ClickHouse,
				Feature: "changing a table's storage in place",
				Err:     ptaherr.ErrUnsupportedFeature,
				Message: fmt.Sprintf("unsupported feature: %s; Ptah creates the table again and keeps its rows when asked with %s: %s",
					strings.Join(reasons, "; "), p.rebuild.words(), rebuildWarning),
			}
		}
		rebuild, err := p.rebuildTable(diff, builder, table, reasons)
		if err != nil {
			return nil, err
		}
		rebuilds = append(rebuilds, rebuild)
	}
	return rebuilds, nil
}

// recreationReasons says why ClickHouse cannot change the table with ALTER
// TABLE, one sentence for each limit the change meets.
//
// A column that moves into or out of the primary key meets the same limit as a
// declared PRIMARY KEY clause that changes, and is named in place of that
// clause: the comparison reports the column change, which the server accepts as
// MODIFY COLUMN and which changes no key, so the plan would apply and the next
// one would report the same change (stokaro/ptah#4104).
func recreationReasons(table *difftypes.TableDiff) []string {
	var reasons []string
	columnKey := false
	for _, column := range table.ColumnsModified {
		if change, moved := column.Changes["primary_key"]; moved {
			reasons = append(reasons, fmt.Sprintf(
				"the primary key of %s changes (column %s: %s): ClickHouse fixes a MergeTree table's primary key when the table is created, and no ALTER changes it",
				table.TableName, column.ColumnName, change))
			columnKey = true
			break
		}
	}
	for _, record := range table.FeatureChanges {
		change, ok := record.Value.(*chdiff.Table)
		if !ok {
			continue
		}
		limit, required := chplan.RecreationReason(change, columnNames(table.Current.Table.Columns), declaredNames(table.Desired.Fields))
		// The column's own sentence names the column, which says more than the
		// clause does.
		if required && (!columnKey || !strings.HasSuffix(limit, "changes a table's PRIMARY KEY")) {
			reasons = append(reasons, limit)
		}
	}
	return reasons
}

// rebuildTable plans the statements that create a table again and keep its
// rows. Through a holding table, in this order:
//
//  1. CREATE TABLE holding, with the declared type of each column the table
//     keeps, and no key, default, comment or constraint. The server refuses
//     the statement when a table of that name exists, so nothing is overwritten.
//  2. INSERT INTO holding SELECT the kept columns from the table. A column
//     whose type or nullability changes goes through accurateCast, which fails
//     on a value the new type cannot hold, where an unqualified INSERT wraps a
//     number around and turns NULL into the type's zero. The table is intact
//     until this statement has run.
//  3. DROP TABLE the table SYNC. SYNC waits until the server has removed the
//     table's data and, on a Replicated table, its path in the coordination
//     service, which a table of the same path needs before it can be created.
//  4. CREATE TABLE the declared table, then the skipping indexes it declares,
//     so rows are written with them.
//  5. INSERT INTO the table SELECT the kept columns from holding. A column the
//     declaration adds takes its default; a column it drops is not copied.
//  6. DROP TABLE holding SYNC.
//
// A row policy belongs to the table by name, and ClickHouse keeps it while the
// table is dropped, so none is written here.
func (p *Planner) rebuildTable(diff *difftypes.SchemaDiff, builder objectidentity.Builder, table *difftypes.TableDiff, reasons []string) (tableRebuild, error) {
	declared := table.Desired
	if !declared.HasTable() {
		return tableRebuild{}, unsupportedRebuild(table.TableName,
			"the diff carries no declaration for it; declare the table, or drop it instead of changing it")
	}
	holding, err := holdingTableName(diff, table.TableName)
	if err != nil {
		return tableRebuild{}, err
	}
	create := modelast.FromTableWithConstraints(declared.Table, declared.Fields, declared.Enums, platform.ClickHouse, declared.Constraints)
	create.Name = table.TableName
	if err := followSortingKey(create, table.Current.Table.Facets); err != nil {
		return tableRebuild{}, err
	}
	present := make(map[string]bool, len(table.Current.Table.Columns))
	for _, column := range table.Current.Table.Columns {
		present[column.Name] = true
	}
	changedType := make(map[string]bool)
	for _, column := range table.ColumnsModified {
		_, typed := column.Changes["type"]
		_, nullable := column.Changes["nullable"]
		if typed || nullable {
			changedType[column.ColumnName] = true
		}
	}
	var kept []*ast.ColumnNode
	var added, computed []string
	for _, column := range create.Columns {
		switch {
		case column.GeneratedKind != "" || column.GeneratedExpression != "":
			computed = append(computed, column.Name)
		case !present[column.Name]:
			added = append(added, column.Name)
		default:
			kept = append(kept, column)
		}
	}
	if len(kept) == 0 {
		return tableRebuild{}, unsupportedRebuild(table.TableName,
			"the declaration keeps none of its columns, so no row could be copied; drop the table and create it again instead, which says the same thing")
	}
	holder := ast.NewCreateTable(holding)
	holder.Comment = holdingMarker + table.TableName
	holder.Options["ENGINE"] = "MergeTree"
	holder.Options["ORDER_BY"] = "tuple()"
	var copies []string
	var read []string
	for _, column := range kept {
		holder.Columns = append(holder.Columns, holdingColumn(column))
		copies = append(copies, quoteIdentifier(column.Name))
		expression := quoteIdentifier(column.Name)
		if changedType[column.Name] {
			mapping, err := chcolumn.Type(column)
			if err != nil {
				return tableRebuild{}, fmt.Errorf("rebuilding table %s: column %s: %w", table.TableName, column.Name, err)
			}
			expression = fmt.Sprintf("accurateCast(%s, %s)", expression, chcolumn.StringLiteral(mapping.Type))
		}
		read = append(read, expression)
	}
	current, holdingTable := quoteTable(table.TableName), quoteTable(holding)
	columnList := strings.Join(copies, ", ")

	nodes := []ast.Node{
		ast.NewComment(fmt.Sprintf("ClickHouse table rebuild of %s: %s", table.TableName, strings.Join(reasons, "; "))),
		ast.NewComment("WARNING: " + rebuildWarning + "."),
		ast.NewComment(fmt.Sprintf("The rows are copied to %s first.", holding)),
		ast.NewComment(fmt.Sprintf("If the copy into %s fails, or the last DROP TABLE fails, %s is whole and %s holds a copy of every row. "+
			"Compare count() of both. Drop %s when %s holds every row. Do not copy back, which doubles the rows.",
			holding, table.TableName, holding, holding, table.TableName)),
		ast.NewComment(fmt.Sprintf("If a statement fails after DROP TABLE %s and %s is not created again, %s holds the only copy. "+
			"Create %s by hand from the declaration. Then run INSERT INTO %s (%s) SELECT %s FROM %s. Then drop %s.",
			table.TableName, table.TableName, holding, table.TableName, table.TableName, columnList, columnList, holding, holding)),
		ast.NewComment(fmt.Sprintf("If %s is created again and filling it fails, compare count() of both and copy the missing rows.", table.TableName)),
		ast.NewComment(fmt.Sprintf("Ptah refuses every plan that would drop %s until this is done.", holding)),
	}
	if len(added) > 0 {
		nodes = append(nodes, ast.NewComment("Columns the table gains take their defaults: "+strings.Join(added, ", ")))
	}
	if dropped := droppedColumns(table, create.Columns); len(dropped) > 0 {
		nodes = append(nodes, ast.NewComment("Data of the columns the table loses is not carried over: "+strings.Join(dropped, ", ")+
			". A rename without a rename hint is a drop and an add, so a column that takes a new name starts with its default"))
	}
	if len(computed) > 0 {
		nodes = append(nodes, ast.NewComment("Computed columns are not copied, the server evaluates them: "+strings.Join(computed, ", ")))
	}
	nodes = append(nodes,
		ast.NewComment("This statement fails, naming them, when a materialized view reads or writes the table, a Buffer table flushes into it, or a dictionary loads from it. "+
			"The copy back would feed every row through a materialized view again, writes through one would fail while the table is gone, "+
			"rows a Buffer holds would be lost when it flushes into the missing table, and the server refuses to drop a table a dictionary loads from."),
		ast.NewComment("A view, a Merge or Distributed table, or a remote() query that reads the table finds no table while these steps run, and reads the new one after."),
		ast.NewRawSQL(dependentsGuard(table.TableName)),
		holder,
		ast.NewRawSQL(fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;", holdingTable, columnList, strings.Join(read, ", "), current)),
		ast.NewRawSQL(fmt.Sprintf("DROP TABLE %s SYNC;", current)),
		create,
	)
	for _, index := range declared.Indexes {
		nodes = append(nodes, indexNode(index, table.TableName))
	}
	nodes = append(nodes,
		// The default limit of 100 partitions in one block would fail this
		// statement after the table is gone, for a table that is partitioned
		// finer than that.
		ast.NewRawSQL(fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s SETTINGS max_partitions_per_insert_block = 0;", current, columnList, columnList, holdingTable)),
		ast.NewRawSQL(fmt.Sprintf("DROP TABLE %s SYNC;", holdingTable)),
	)
	return tableRebuild{
		name:    table.TableName,
		subject: builder.Table(table.TableName),
		nodes:   nodes,
		effect: schemaext.Effect{
			Impact: schemaext.Destructive,
			Reason: fmt.Sprintf("DROP TABLE removes %s and its rows, which are copied back from %s; %s", table.TableName, holding, rebuildWarning),
		},
	}, nil
}

// holdingColumn is the column the holding table keeps a kept column in: its
// declared type, and nothing the new table enforces or computes. The rows are
// checked against the new definition when they are written to the new table,
// which is the later statement; the holding table only has to hold them.
func holdingColumn(column *ast.ColumnNode) *ast.ColumnNode {
	held := *column
	held.Default = nil
	held.Check, held.CheckName, held.CheckNotEnforced = "", "", false
	held.Unique, held.AutoInc, held.ForeignKey = false, false, nil
	held.Comment, held.Collate, held.NotNullConstraintName = "", "", ""
	return &held
}

// droppedColumns names the columns of the table as it is that the declaration
// does not hold.
func droppedColumns(table *difftypes.TableDiff, declared []*ast.ColumnNode) []string {
	var dropped []string
	for _, column := range table.Current.Table.Columns {
		if !slices.ContainsFunc(declared, func(c *ast.ColumnNode) bool { return c.Name == column.Name }) {
			dropped = append(dropped, column.Name)
		}
	}
	return dropped
}

// holdingTableName names the holding table of a rebuild, in the database the
// table is in, and refuses a name another table of the plan has. A table the
// database holds under that name, and the declaration does not, is the one an
// earlier rebuild stopped on, with the rows of the table in it. The plan would
// drop it, so it is refused here, and the server refuses the creation of one
// the plan does not see, such as a table an exclude selector leaves out.
func holdingTableName(diff *difftypes.SchemaDiff, table string) (string, error) {
	name := holdingPrefix + table
	if ref, ok := tableref.Parse(table); ok {
		name = holdingPrefix + ref.Name
		if ref.Qualified {
			name = ref.Schema + "." + name
		}
	}
	if slices.Contains(diff.TablesRemoved.Names(), name) {
		return "", unsupportedRebuild(table, fmt.Sprintf(
			"the database holds a table named %s, where the rebuild keeps the rows; ptah did not leave it there, so rename it or drop it first",
			name))
	}
	declared := slices.ContainsFunc(diff.TablesModified, func(t difftypes.TableDiff) bool { return t.TableName == name }) ||
		slices.ContainsFunc(diff.TablesAdded.Qualified(diff.DeclaredUserTypes, platform.ClickHouse).InDependencyOrder(), func(c difftypes.TableCreation) bool { return c.Name == name })
	if declared {
		return "", unsupportedRebuild(table, fmt.Sprintf("the declaration has a table named %s, where the rebuild keeps the rows", name))
	}
	return name, nil
}

func unsupportedRebuild(table, reason string) error {
	return &ptaherr.CapabilityError{
		Dialect: platform.ClickHouse,
		Feature: "rebuilding a table",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("unsupported feature: cannot create table %s again: %s", table, reason),
	}
}

// quoteTable quotes a table name that may carry its database.
func quoteTable(name string) string {
	ref, ok := tableref.Parse(name)
	switch {
	case !ok:
		return quoteIdentifier(name)
	case ref.Qualified:
		return quoteIdentifier(ref.Schema) + "." + quoteIdentifier(ref.Name)
	default:
		return quoteIdentifier(ref.Name)
	}
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// followSortingKey gives the new table the primary key of its new sorting key
// where the declaration leaves the primary key alone and the table's own
// followed its sorting key.
//
// The comparison resolves a clause the declaration omits to the value the table
// has, and by then the declaration of an omitted PRIMARY KEY and of one that
// repeats the old value read the same. A table whose primary key equals its
// sorting key has the key ClickHouse gives a table that declares none, so the
// omission is the likelier meaning: without this, every change of the sorting
// key that does not keep the old one as a prefix would fail at CREATE TABLE,
// where the new table would otherwise take the new key. A declaration that
// states the primary key and contradicts the new sorting key is still refused
// by the renderer, since this reads only a primary key equal to the observed one.
func followSortingKey(create *ast.CreateTableNode, current schemaext.Facets) error {
	declared, found, err := schemaext.FacetAs[*chschema.DesiredTable](create.Facets, chschema.TableKind)
	if err != nil || !found {
		return err
	}
	observed, found, err := schemaext.FacetAs[*chschema.ObservedTable](current, chschema.TableKind)
	if err != nil || !found {
		return err
	}
	if observed.PrimaryKey != observed.OrderBy || declared.PrimaryKey.Value != observed.PrimaryKey || declared.OrderBy.Value == observed.OrderBy {
		return nil
	}
	followed := *declared
	followed.PrimaryKey = declared.OrderBy
	create.Facets, err = create.Facets.Replace(&followed)
	return err
}

func columnNames(columns []catalog.Column) []string {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	return names
}

func declaredNames(fields []schemamodel.Field) []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}

// replicatedEngine reports the engine of the table, as it is or as declared,
// when it is a replicated one.
func replicatedEngine(table *difftypes.TableDiff) (string, bool) {
	var engines []string
	if observed, found, err := schemaext.FacetAs[*chschema.ObservedTable](table.Current.Table.Facets, chschema.TableKind); err == nil && found {
		engines = append(engines, observed.Engine)
	}
	if desired, found, err := schemaext.FacetAs[*chschema.DesiredTable](table.Desired.Table.Facets, chschema.TableKind); err == nil && found {
		engines = append(engines, desired.Engine.Value)
	}
	for _, engine := range engines {
		name, _, _ := strings.Cut(engine, "(")
		name = strings.ToUpper(strings.TrimSpace(name))
		if strings.HasPrefix(name, "REPLICATED") || strings.HasPrefix(name, "SHARED") {
			return engine, true
		}
	}
	return "", false
}

// dependentsGuard is a statement that fails, naming them, when an object the
// rebuild would harm depends on the table:
//
//   - a materialized view that reads it, which the server lists in the table's
//     dependencies_table. It keeps firing after DROP TABLE and CREATE TABLE, so
//     the copy back would feed every row through it a second time;
//   - a dictionary, or a table whose default calls dictGet over it, which the
//     server lists in loading_dependent_table. The server refuses DROP TABLE
//     while one exists, and a refusal there would stop the rebuild halfway;
//   - a materialized view that writes to it (TO the table). Its source refuses
//     inserts while the table is gone. The server lists it only among the
//     dependencies of its source, so the guard reads its definition, whose TO
//     target the server writes right after the view's name; see
//     [materializedViewTarget];
//   - a Buffer table that flushes into it. The rows it holds in memory are not
//     in the table when the rows are copied, and a flush into the missing table
//     drops them. The guard reads the destination out of engine_full; see
//     [bufferDestination].
//
// A view, a Merge or Distributed table and a remote() query only read the
// table. They find no table while it is gone and read the new one after, which
// the plan's comment says; they do not stop the rebuild. A refreshable
// materialized view owns the rows of the table it writes to: a refresh while
// the table is gone fails and runs again, and the next one rewrites the rows,
// so it does not stop the rebuild either, and the pattern does not match it.
// Each case was measured on 24.10 and 26.10. The message is the error the server gives for a
// string that is not a number, because throwIf takes only a constant message.
func dependentsGuard(table string) string {
	database, name := "currentDatabase()", chcolumn.StringLiteral(table)
	if ref, ok := tableref.Parse(table); ok {
		name = chcolumn.StringLiteral(ref.Name)
		if ref.Qualified {
			database = chcolumn.StringLiteral(ref.Schema)
		}
	}
	target, buffer := chcolumn.StringLiteral(materializedViewTarget), chcolumn.StringLiteral(bufferDestination)
	return fmt.Sprintf("SELECT toUInt8(concat(%s, arrayStringConcat(arraySort(groupUniqArray(dependent)), ', '), %s)) FROM ("+
		"SELECT concat(tupleElement(dependency, 1), '.', tupleElement(dependency, 2)) AS dependent FROM system.tables "+
		"ARRAY JOIN arrayConcat(arrayZip(dependencies_database, dependencies_table), arrayZip(loading_dependent_database, loading_dependent_table)) AS dependency "+
		"WHERE database = %s AND name = %s "+
		"UNION ALL SELECT concat(database, '.', name) AS dependent FROM system.tables "+
		"WHERE engine = 'MaterializedView' AND replaceAll(extractGroups(create_table_query, %s)[1], '`', '') = %s "+
		"AND replaceAll(extractGroups(create_table_query, %s)[2], '`', '') = %s "+
		"UNION ALL SELECT concat(database, '.', name) AS dependent FROM system.tables "+
		"WHERE engine = 'Buffer' AND extractGroups(engine_full, %s)[1] = %s AND extractGroups(engine_full, %s)[2] = %s"+
		") HAVING count() > 0;",
		chcolumn.StringLiteral("cannot create "+table+" again: these depend on it: "),
		chcolumn.StringLiteral(". A materialized view that reads the table would receive every row again, "+
			"one that writes to it would fail while the table is gone, a Buffer would lose the rows it holds, "+
			"and the server refuses to drop a table a dictionary loads from. "+
			"Drop each one and create it again around the rebuild"),
		database, name, target, database, target, name, buffer, database, buffer, name)
}

// materializedViewTarget is the RE2 pattern, as the server's extractGroups()
// reads it, that takes the database and the table a materialized view writes to
// out of its create_table_query. The server writes that text in one shape,
// CREATE MATERIALIZED VIEW db.name TO db.table (columns) AS SELECT ..., with a
// name in backticks only where it needs them, so the target sits right after
// the view's own qualified name. A view with its own engine has no TO and does
// not match, and neither does a refreshable one, whose REFRESH clause comes
// before its TO.
const materializedViewTarget = "^CREATE MATERIALIZED VIEW (?:`(?:[^`\\\\]|\\\\.)*`|[^ `.]+)\\.(?:`(?:[^`\\\\]|\\\\.)*`|[^ `]+) TO " +
	"(`(?:[^`\\\\]|\\\\.)*`|[^ `.]+)\\.(`(?:[^`\\\\]|\\\\.)*`|[^ `(]+)"

// bufferDestination is the RE2 pattern that takes the database and the table a
// Buffer table flushes into out of its engine_full. The server writes both as
// string literals, Buffer('db', 'table', ...), whatever spelling created the
// table, with currentDatabase() already evaluated. A Buffer with no
// destination has an empty table name, which names no table.
const bufferDestination = "^Buffer\\('((?:[^'\\\\]|\\\\.)*)', '((?:[^'\\\\]|\\\\.)*)'"

// refuseUnfinishedRebuilds refuses a plan that drops a holding table an earlier
// rebuild left. The table carries the marker comment.
//
// The marker cannot tell which statement failed, so the refusal says what to do
// by the state the diff shows. A plan that creates the owner, which is missing,
// follows a rebuild that stopped after DROP TABLE: the holding table holds the
// only copy of the rows. Otherwise the owner exists, and the rebuild stopped
// before its DROP TABLE, or at the last statement, or while the owner was
// filled. The owner may already hold every row, and copying back then doubles
// them, so the refusal asks for a comparison first.
func refuseUnfinishedRebuilds(diff *difftypes.SchemaDiff) error {
	for _, removal := range diff.TablesRemoved {
		owner, held := strings.CutPrefix(removal.Current.Table.Comment, holdingMarker)
		if !held {
			continue
		}
		columns := make([]string, len(removal.Current.Table.Columns))
		for i, column := range removal.Current.Table.Columns {
			columns[i] = quoteIdentifier(column.Name)
		}
		list := strings.Join(columns, ", ")
		holding, ownerTable := quoteTable(removal.Name), quoteTable(owner)
		copyBack := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", ownerTable, list, list, holding)
		missing := slices.ContainsFunc(diff.TablesAdded.Qualified(diff.DeclaredUserTypes, platform.ClickHouse).InDependencyOrder(),
			func(creation difftypes.TableCreation) bool { return creation.Name == owner })
		var steps string
		if missing {
			steps = fmt.Sprintf("Table %s is missing, so the rebuild stopped after it dropped the table, and %s may hold the only copy of the rows. "+
				"Create %s by hand from your declaration. Copy the rows back: %s. Check them. Then drop the holding table: DROP TABLE %s",
				owner, removal.Name, owner, copyBack, holding)
		} else {
			steps = fmt.Sprintf("Table %s exists. The rebuild may have stopped before it dropped the table, or at its last statement, and then %s holds a copy of every row. "+
				"Copying it back would double them. Compare SELECT count() FROM %s with SELECT count() FROM %s. "+
				"If %s holds every row, drop the holding table: DROP TABLE %s. "+
				"If it lacks rows, copy them back: %s. Then drop the holding table. "+
				"If the count of %s fails because the table does not exist, the rebuild stopped after it dropped the table: "+
				"create %s from your declaration and copy the rows back, and do not drop the holding table first",
				owner, removal.Name, ownerTable, holding, owner, holding, copyBack, owner, owner)
		}
		return &ptaherr.CapabilityError{
			Dialect: platform.ClickHouse,
			Feature: "dropping the holding table of a rebuild",
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("unsupported feature: table %s is the holding table of a rebuild of table %s that stopped. The plan would drop it, and it may hold rows the table lacks. %s",
				removal.Name, owner, steps),
		}
	}
	return nil
}
