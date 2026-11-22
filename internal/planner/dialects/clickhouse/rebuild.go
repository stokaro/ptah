package clickhouse

import (
	"cmp"
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

// copyPrefix starts the name the new table of a rebuild is created under, and
// the old table takes once EXCHANGE TABLES has swapped the two. The name is
// fixed by the table's own, so a run that stopped part way leaves a table the
// next plan finds, and the server refuses to create a second one over it.
const copyPrefix = "ptah_rebuild_"

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
					"The rebuild is not safe there. The new table is created on this server only. The copy reads one replica, which may lag. "+
					"A new table at a fixed Keeper path meets the metadata the other replicas keep at that path. "+
					"Create the table again by hand, on every replica.",
					strings.Join(reasons, "; "), table.TableName, engine),
			}
		}
		if engine, stored := unstoredEngine(table); !stored {
			return nil, &ptaherr.CapabilityError{
				Dialect: platform.ClickHouse,
				Feature: "rebuilding a table whose rows live elsewhere",
				Err:     ptaherr.ErrUnsupportedFeature,
				Message: fmt.Sprintf("unsupported feature: %s; Ptah does not create table %s again, because its engine %s does not keep the table's rows on this server. "+
					"The copy would read rows from somewhere else, write them somewhere else, or drop them. "+
					"Ptah copies rows only between engines that store them here: a MergeTree engine that is not replicated or shared, Memory, Log, TinyLog and StripeLog. "+
					"Create the table again by hand.",
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
// rows. The table stays as it is until its replacement holds the rows:
//
//  1. A guard that fails unless the database is Atomic, the one engine that
//     swaps two tables, and the user running the plan reads every row the
//     table stores. A row policy or a table filter of the user's settings
//     hides rows from the copy, and the hidden rows would go with the old
//     table. See [rebuildGuard].
//  2. CREATE TABLE, under the copy's name, from the declaration, then the
//     skipping indexes it declares, so rows are written with them. The server
//     refuses the statement when a table of that name exists, so nothing is
//     overwritten.
//  3. INSERT INTO the copy SELECT the kept columns from the table. A column
//     whose type or nullability changes goes through accurateCast, which fails
//     on a value the new type cannot hold, where an unqualified INSERT wraps a
//     number around and turns NULL into the type's zero. A column the
//     declaration adds takes its default; a column it drops is not copied.
//  4. A check that the copy holds as many rows as the table, so a write that
//     reached the table during the copy, or a copy the user's limits cut
//     short, stops the plan before the swap. A declared TTL drops expired rows
//     as they are written, and then the counts differ for that reason alone, so
//     a table with one is not checked.
//  5. EXCHANGE TABLES, which swaps the two names in one step.
//  6. DROP TABLE the copy's name SYNC, which now names the old table.
//
// Everything that finds the table by name follows the swap: measured on 24.10
// and 26.10, a materialized view that reads the table fires for the new one
// and not for the copy, one that writes to it, a Buffer table and a dictionary
// reach the new one, and a row policy and a grant hold for it. So none of them
// stops the rebuild, and none is written here.
func (p *Planner) rebuildTable(diff *difftypes.SchemaDiff, builder objectidentity.Builder, table *difftypes.TableDiff, reasons []string) (tableRebuild, error) {
	declared := table.Desired
	if !declared.HasTable() {
		return tableRebuild{}, unsupportedRebuild(table.TableName,
			"the diff carries no declaration for it; declare the table, or drop it instead of changing it")
	}
	copyName, err := copyTableName(diff, table.TableName)
	if err != nil {
		return tableRebuild{}, err
	}
	create := modelast.FromTableWithConstraints(declared.Table, declared.Fields, declared.Enums, platform.ClickHouse, declared.Constraints)
	create.Name = copyName
	if err := followSortingKey(create, table.Current.Table.Facets); err != nil {
		return tableRebuild{}, err
	}
	expires, err := declaresTTL(create)
	if err != nil {
		return tableRebuild{}, err
	}
	columns, err := copiedColumns(table, create)
	if err != nil {
		return tableRebuild{}, err
	}
	current, copyTable := quoteTable(table.TableName), quoteTable(copyName)

	loss := fmt.Sprintf("A row written to %s while it is copied stops the plan before the swap. "+
		"A row written between that check and the swap is lost with the old table.", table.TableName)
	if expires {
		loss = fmt.Sprintf("The declaration gives the table a TTL, which drops expired rows as they are written, "+
			"so the copy is not counted against %s, and a row written to %s while it is copied is lost with the old table.", table.TableName, table.TableName)
	}
	nodes := rebuildComments(table, copyName, reasons, loss, columns)
	nodes = append(nodes, ast.NewRawSQL(rebuildGuard(table.TableName)), create)
	for _, index := range declared.Indexes {
		nodes = append(nodes, indexNode(index, copyName))
	}
	// The copy lifts the limit of 100 partitions in one block, which a table
	// partitioned finer than that exceeds. It writes the rows as they are:
	// without optimize_on_insert = 0 a ReplacingMergeTree or CollapsingMergeTree
	// copy merges the rows of each block as it writes them, and without
	// insert_deduplicate = 0 a table with a deduplication window drops a block
	// equal to one written before it, either of which makes the counts differ.
	nodes = append(nodes, ast.NewRawSQL(fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s SETTINGS max_partitions_per_insert_block = 0, optimize_on_insert = 0, insert_deduplicate = 0;",
		copyTable, strings.Join(columns.copies, ", "), strings.Join(columns.read, ", "), current)))
	if !expires {
		nodes = append(nodes, ast.NewRawSQL(copyCheck(table.TableName, copyName)))
	}
	nodes = append(nodes,
		ast.NewRawSQL(fmt.Sprintf("EXCHANGE TABLES %s AND %s;", current, copyTable)),
		ast.NewRawSQL(fmt.Sprintf("DROP TABLE %s SYNC;", copyTable)),
	)
	return tableRebuild{
		name:    table.TableName,
		subject: builder.Table(table.TableName),
		nodes:   nodes,
		effect: schemaext.Effect{
			Impact: schemaext.Destructive,
			Reason: fmt.Sprintf("DROP TABLE removes the old table %s and its rows once EXCHANGE TABLES has swapped in the copy %s; %s",
				table.TableName, copyName, rebuildWarning),
		},
	}, nil
}

// rebuildColumns is how the copy of a rebuild fills the new table's columns.
type rebuildColumns struct {
	// copies are the quoted columns the INSERT names, and read is the
	// expression the SELECT reads each from, in the same order.
	copies, read []string
	// added take their defaults, and the server evaluates computed.
	added, computed []string
}

// copiedColumns sorts the new table's columns into those the copy carries, those
// the declaration adds and the computed ones. A carried column whose type or
// nullability changes is read through accurateCast, which fails on a value the
// new type cannot hold, where an unqualified INSERT wraps a number around and
// turns NULL into the type's zero.
func copiedColumns(table *difftypes.TableDiff, create *ast.CreateTableNode) (rebuildColumns, error) {
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
	var columns rebuildColumns
	for _, column := range create.Columns {
		if column.GeneratedKind != "" || column.GeneratedExpression != "" {
			columns.computed = append(columns.computed, column.Name)
			continue
		}
		if !present[column.Name] {
			columns.added = append(columns.added, column.Name)
			continue
		}
		expression := quoteIdentifier(column.Name)
		if changedType[column.Name] {
			mapping, err := chcolumn.Type(column)
			if err != nil {
				return rebuildColumns{}, fmt.Errorf("rebuilding table %s: column %s: %w", table.TableName, column.Name, err)
			}
			expression = fmt.Sprintf("accurateCast(%s, %s)", expression, chcolumn.StringLiteral(mapping.Type))
		}
		columns.copies = append(columns.copies, quoteIdentifier(column.Name))
		columns.read = append(columns.read, expression)
	}
	if len(columns.copies) == 0 {
		return rebuildColumns{}, unsupportedRebuild(table.TableName,
			"the declaration keeps none of its columns, so no row could be copied; drop the table and create it again instead, which says the same thing")
	}
	return columns, nil
}

// rebuildComments open a rebuild: what it does, what a stop leaves behind, which
// writes are lost, and what happens to each column.
func rebuildComments(table *difftypes.TableDiff, copyName string, reasons []string, loss string, columns rebuildColumns) []ast.Node {
	nodes := []ast.Node{
		ast.NewComment(fmt.Sprintf("ClickHouse table rebuild of %s: %s", table.TableName, strings.Join(reasons, "; "))),
		ast.NewComment("WARNING: " + rebuildWarning + "."),
		ast.NewComment(fmt.Sprintf("The new table is created as %s and filled from %s, which stays as it is until EXCHANGE TABLES swaps the two names. "+
			"Then the old table, named %s by then, is dropped.", copyName, table.TableName, copyName)),
		ast.NewComment(fmt.Sprintf("If a statement before EXCHANGE TABLES fails, %s is as it was, and %s holds part of the copy or all of it. "+
			"If the last DROP TABLE fails, %s is the new table and holds every row the copy read, and %s is the old table. "+
			"Either way drop %s; Ptah does not rebuild %s while it exists.",
			table.TableName, copyName, table.TableName, copyName, copyName, table.TableName)),
		ast.NewComment(loss),
	}
	if len(columns.added) > 0 {
		nodes = append(nodes, ast.NewComment("Columns the table gains take their defaults: "+strings.Join(columns.added, ", ")))
	}
	if dropped := droppedColumns(table, columns); len(dropped) > 0 {
		nodes = append(nodes, ast.NewComment("Data of the columns the table loses is not carried over: "+strings.Join(dropped, ", ")+
			". A rename without a rename hint is a drop and an add, so a column that takes a new name starts with its default"))
	}
	if len(columns.computed) > 0 {
		nodes = append(nodes, ast.NewComment("Computed columns are not copied, the server evaluates them: "+strings.Join(columns.computed, ", ")))
	}
	return append(nodes, ast.NewComment(fmt.Sprintf("Materialized views, dictionaries, Buffer tables, row policies and grants find %s by name, "+
		"so they use the new table once the two are swapped.", table.TableName)))
}

// declaresTTL reports whether the new table has a table TTL, which drops the
// rows it has expired as they are written.
func declaresTTL(create *ast.CreateTableNode) (bool, error) {
	declared, found, err := schemaext.FacetAs[*chschema.DesiredTable](create.Facets, chschema.TableKind)
	if err != nil || !found {
		return false, err
	}
	return strings.TrimSpace(declared.TTL.Value) != "", nil
}

// droppedColumns names the columns of the table as it is that the new table
// does not have.
func droppedColumns(table *difftypes.TableDiff, columns rebuildColumns) []string {
	kept := slices.Concat(columns.copies, columns.added, columns.computed)
	var dropped []string
	for _, column := range table.Current.Table.Columns {
		if !slices.Contains(kept, column.Name) && !slices.Contains(kept, quoteIdentifier(column.Name)) {
			dropped = append(dropped, column.Name)
		}
	}
	return dropped
}

// copyTableName names the new table of a rebuild, in the database the table is
// in, and refuses a name another table has. A table the database holds under
// that name, and the declaration does not, is most likely what an earlier
// rebuild of the table left when it stopped. The table holds every row that
// rebuild read, whichever statement it stopped at, so the table of that name
// can go; the plan says so rather than drop it, since Ptah cannot tell it from
// a table someone else named that way. The server refuses the creation of one
// the plan does not see, such as a table an exclude selector leaves out.
func copyTableName(diff *difftypes.SchemaDiff, table string) (string, error) {
	name := copyPrefix + table
	if ref, ok := tableref.Parse(table); ok {
		name = copyPrefix + ref.Name
		if ref.Qualified {
			name = ref.Schema + "." + name
		}
	}
	if slices.Contains(diff.TablesRemoved.Names(), name) {
		return "", unsupportedRebuild(table, fmt.Sprintf(
			"the database holds a table named %s, the name the new table takes until the swap. "+
				"A rebuild of %s that stopped leaves it there, and %s then holds every row that rebuild read, whichever statement it stopped at. "+
				"Drop %s and run the plan again; if no rebuild left it there, rename it first",
			name, table, table, name))
	}
	declared := slices.ContainsFunc(diff.TablesModified, func(t difftypes.TableDiff) bool { return t.TableName == name }) ||
		slices.ContainsFunc(diff.TablesAdded.Qualified(diff.DeclaredUserTypes, platform.ClickHouse).InDependencyOrder(), func(c difftypes.TableCreation) bool { return c.Name == name })
	if declared {
		return "", unsupportedRebuild(table, fmt.Sprintf("the declaration has a table named %s, the name the new table takes until the swap", name))
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

// tableEngines returns the engine of the table as it is, then as declared. A
// declaration that names no engine gets MergeTree.
func tableEngines(table *difftypes.TableDiff) []string {
	var engines []string
	if observed, found, err := schemaext.FacetAs[*chschema.ObservedTable](table.Current.Table.Facets, chschema.TableKind); err == nil && found {
		engines = append(engines, observed.Engine)
	}
	if desired, found, err := schemaext.FacetAs[*chschema.DesiredTable](table.Desired.Table.Facets, chschema.TableKind); err == nil && found {
		engines = append(engines, cmp.Or(strings.TrimSpace(desired.Engine.Value), "MergeTree"))
	}
	return engines
}

// engineName is the name of an engine without its parameters, in upper case.
func engineName(engine string) string {
	name, _, _ := strings.Cut(engine, "(")
	return strings.ToUpper(strings.TrimSpace(name))
}

// replicatedEngine reports the engine of the table, as it is or as declared,
// when it is a replicated one.
func replicatedEngine(table *difftypes.TableDiff) (string, bool) {
	for _, engine := range tableEngines(table) {
		name := engineName(engine)
		if strings.HasPrefix(name, "REPLICATED") || strings.HasPrefix(name, "SHARED") {
			return engine, true
		}
	}
	return "", false
}

// unstoredEngine reports the engine of the table, as it is or as declared, that
// does not keep the table's rows on this server, and false when there is one.
// A copy out of a Kafka or Distributed table reads rows from elsewhere, one into
// a MySQL, URL or Distributed table writes them elsewhere, and one into a Null
// table drops them. A replicated engine is refused before this, for its own
// reason.
func unstoredEngine(table *difftypes.TableDiff) (string, bool) {
	for _, engine := range tableEngines(table) {
		switch name := engineName(engine); {
		case name == "MEMORY", name == "LOG", name == "TINYLOG", name == "STRIPELOG":
		case strings.HasSuffix(name, "MERGETREE"):
		default:
			return engine, false
		}
	}
	return "", true
}

// rebuildGuard is a statement that fails before anything is created:
//
//   - unless the table's database is Atomic. EXCHANGE TABLES swaps two tables
//     only there: an Ordinary or a Memory database answers Code 48, measured
//     on 24.10 and 26.10. A Replicated database, which also swaps tables, is
//     refused as well: it runs each statement on every replica, and the copy
//     of a rebuild planned for one server reads the rows of one;
//   - unless the user running the plan reads every row the table stores. The
//     copy reads the table as that user, so a row policy that applies to the
//     user, a table filter of the user's settings, or a server that hides rows
//     from a user no policy names, would leave rows out of the copy, and they
//     would go with the old table. total_rows in system.tables is what the
//     table stores, which no policy filters, and apply_deleted_mask = 0 counts
//     the rows a lightweight DELETE has hidden, which total_rows still counts.
//     A write that lands between the two counts stops the plan too.
//
// throwIf takes only a constant message, which is why the messages name no
// count.
func rebuildGuard(table string) string {
	database, name := tableCatalogNames(table)
	return fmt.Sprintf("SELECT throwIf((SELECT count() FROM system.databases WHERE name = %s AND engine = 'Atomic') = 0, %s), "+
		"throwIf((SELECT count() FROM %s) != (SELECT ifNull(sum(total_rows), 0) FROM system.tables WHERE database = %s AND name = %s), %s) "+
		"SETTINGS apply_deleted_mask = 0;",
		database,
		chcolumn.StringLiteral("cannot create "+table+" again: EXCHANGE TABLES swaps two tables only in an Atomic database, and the database of "+table+" is not one"),
		quoteTable(table), database, name,
		chcolumn.StringLiteral("cannot create "+table+" again: the user running the plan does not read every row "+table+" stores, "+
			"so the copy would leave rows out and they would go with the old table. "+
			"A row policy that applies to the user, or a table filter in the user's settings, hides them, or a row was written while the plan counted. "+
			"Run the plan as a user that reads every row, with writes to the table stopped"))
}

// copyCheck is a statement that fails, before the swap, when the copy holds
// another number of rows than the table. The table is as it was then.
func copyCheck(table, copyName string) string {
	return fmt.Sprintf("SELECT throwIf((SELECT count() FROM %s) != (SELECT count() FROM %s), %s);",
		quoteTable(copyName), quoteTable(table),
		chcolumn.StringLiteral("cannot create "+table+" again: the copy "+copyName+" holds another number of rows than "+table+
			", so a row was written to "+table+" while it was copied, or a limit in the user's settings cut the copy short. "+
			table+" is as it was. Drop "+copyName+", stop writes to the table, and run the plan again"))
}

// tableCatalogNames returns the database and the name of a table as
// ClickHouse expressions a system table can be filtered by.
func tableCatalogNames(table string) (database, name string) {
	database, name = "currentDatabase()", chcolumn.StringLiteral(table)
	if ref, ok := tableref.Parse(table); ok {
		name = chcolumn.StringLiteral(ref.Name)
		if ref.Qualified {
			database = chcolumn.StringLiteral(ref.Schema)
		}
	}
	return database, name
}
