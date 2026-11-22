package clickhouse

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// carriedStorage is what a rebuild carries from the table onto the new one
// because no declaration states it: the codec of each column the new table
// keeps, and the table's projections. Without it the new table has none of
// them, and the next plan reports nothing, since no comparison reads either.
//
// The codec of a column whose type changes is left behind, and the plan says
// so. A codec fits the type it was chosen for: measured on 26.10, `x UInt64
// CODEC(Delta, ZSTD)` declared as String would come back as `String
// CODEC(Delta(8), ZSTD(1))`, which a CREATE TABLE of that column refuses with
// code 36.
//
// The declaration states the rest of what the table keeps, and the new table
// takes it from there: measured on 24.10 and 26.10, the ALTER path removes a
// table or column comment the declaration leaves out, and leaves a codec and a
// projection as they are, which is the line the carry follows.
type carriedStorage struct {
	// columns are the columns both tables have with the same type, which the
	// check compares.
	columns []string
	codecs  []carriedCodec
	// lost are the codecs of the columns whose type changes.
	lost        []carriedCodec
	projections []catalog.Projection
}

type carriedCodec struct {
	column, clause string
}

// carryStorage collects what the new table takes from the table. It refuses a
// column whose definition holds a storage clause other than CODEC -- a column
// TTL, STATISTICS or SETTINGS, which Ptah does not carry -- and a projection
// that uses a column the declaration drops, which the new table cannot have.
func carryStorage(table *difftypes.TableDiff, create *ast.CreateTableNode, columns rebuildColumns) (carriedStorage, error) {
	declared := make(map[string]bool, len(create.Columns))
	for _, column := range create.Columns {
		declared[column.Name] = true
	}
	var carried carriedStorage
	for _, column := range table.Current.Table.Columns {
		if !declared[column.Name] {
			continue
		}
		retyped := slices.Contains(columns.retyped, column.Name)
		if !retyped {
			carried.columns = append(carried.columns, column.Name)
		}
		for _, clause := range column.StorageClauses {
			if keyword := clauseKeyword(clause); keyword != "CODEC" {
				return carriedStorage{}, unsupportedRebuild(table.TableName, fmt.Sprintf(
					"column %s has %s, which no declaration states and Ptah does not carry into the new table, so the new table would lose it: %s. "+
						"Remove it from the column first, or create the table again by hand",
					column.Name, keyword, clause))
			}
			codec := carriedCodec{column: column.Name, clause: clause}
			if retyped {
				carried.lost = append(carried.lost, codec)
				continue
			}
			carried.codecs = append(carried.codecs, codec)
		}
	}
	dropped := droppedColumns(table, columns)
	for _, projection := range table.Current.Table.Projections {
		if column, uses := projectionUses(projection, dropped); uses {
			return carriedStorage{}, unsupportedRebuild(table.TableName, fmt.Sprintf(
				"its projection %s uses column %s, which the declaration drops, so the new table cannot have the projection. "+
					"Drop the projection first, or keep the column",
				projection.Name, column))
		}
		carried.projections = append(carried.projections, projection)
	}
	return carried, nil
}

// clauseKeyword is the keyword a storage clause starts with.
func clauseKeyword(clause string) string {
	end := strings.IndexAny(clause, "( ")
	if end < 0 {
		return clause
	}
	return clause[:end]
}

// projectionUses reports a column of dropped that the projection's definition
// names. A function or an alias that shares a dropped column's name reads as
// the column, so such a projection is refused rather than carried onto a table
// that would refuse it.
func projectionUses(projection catalog.Projection, dropped []string) (string, bool) {
	lex := lexer.NewLexerWithOptions(projection.Definition, dialectlexer.Options(platform.ClickHouse))
	for {
		token := lex.NextToken()
		if token.Type == lexer.TokenEOF {
			return "", false
		}
		if token.Type != lexer.TokenIdentifier {
			continue
		}
		if name, ok := chcolumn.UnquoteIdentifier(token.Value); ok && slices.Contains(dropped, name) {
			return name, true
		}
	}
}

// statements carry the codecs and the projections onto the empty copy. ALTER
// TABLE takes both as the server prints them in SHOW CREATE TABLE, every
// projection form included, measured on 24.10 and 26.10.
func (c carriedStorage) statements(copyTable string) []ast.Node {
	nodes := make([]ast.Node, 0, len(c.codecs)+len(c.projections))
	for _, codec := range c.codecs {
		nodes = append(nodes, ast.NewRawSQL(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s;", copyTable, quoteIdentifier(codec.column), codec.clause)))
	}
	for _, projection := range c.projections {
		nodes = append(nodes, ast.NewRawSQL(fmt.Sprintf("ALTER TABLE %s ADD PROJECTION %s %s;", copyTable, quoteIdentifier(projection.Name), projection.Definition)))
	}
	return nodes
}

// check is a statement that fails, before a row is copied, unless the copy
// reports the codec the table has for every column the two share, and the
// projections the table has. It reads both from the server, so it holds
// whatever the plan read from the table: a codec or a projection that changed
// after the plan was made, or that the carry missed, stops the plan here.
func (c carriedStorage) check(table, copyName string) string {
	database, name := tableCatalogNames(table)
	_, copyLiteral := tableCatalogNames(copyName)
	shared := make([]string, len(c.columns))
	for i, column := range c.columns {
		shared[i] = chcolumn.StringLiteral(column)
	}
	codecs := func(table string) string {
		return fmt.Sprintf("(SELECT arraySort(groupArray((name, compression_codec))) FROM system.columns WHERE database = %s AND table = %s AND name IN (%s))",
			database, table, strings.Join(shared, ", "))
	}
	projections := func(table string) string {
		return fmt.Sprintf("(SELECT arraySort(groupArray((name, query))) FROM system.projections WHERE database = %s AND table = %s)", database, table)
	}
	return fmt.Sprintf("SELECT throwIf(%s != %s, %s), throwIf(%s != %s, %s);",
		codecs(copyLiteral), codecs(name),
		chcolumn.StringLiteral("cannot create "+table+" again: the copy "+copyName+" does not have the codec "+table+" has for every column the two share. "+
			table+" is as it was. Drop "+copyName+" and run the plan again"),
		projections(copyLiteral), projections(name),
		chcolumn.StringLiteral("cannot create "+table+" again: the copy "+copyName+" does not have the projections "+table+" has. "+
			table+" is as it was. Drop "+copyName+" and run the plan again"))
}

// names says what the carry keeps, for the plan's comments.
func (c carriedStorage) names() []string {
	var names []string
	if len(c.codecs) > 0 {
		columns := make([]string, len(c.codecs))
		for i, codec := range c.codecs {
			columns[i] = codec.column + " " + codec.clause
		}
		names = append(names, "the codecs of columns "+strings.Join(columns, ", "))
	}
	if len(c.projections) > 0 {
		projections := make([]string, len(c.projections))
		for i, projection := range c.projections {
			projections[i] = projection.Name
		}
		names = append(names, "the projections "+strings.Join(projections, ", "))
	}
	return names
}

// lostCodecs names, for the plan's comments, each codec the new table does not
// take because its column's type changes: "column x: codec Delta(8), ZSTD(1)
// -> none, the type changes".
func (c carriedStorage) lostCodecs() []string {
	lost := make([]string, len(c.lost))
	for i, codec := range c.lost {
		codecs := strings.TrimSuffix(strings.TrimPrefix(codec.clause, "CODEC("), ")")
		lost[i] = "column " + codec.column + ": codec " + codecs + " -> none, the type changes"
	}
	return lost
}

// projectionModeGuard is a condition for [rebuildGuard] that fails when the
// table has projections and the new table's engine merges rows, unless
// deduplicate_merge_projection_mode lets the new table keep them. ClickHouse
// refuses a projection on every MergeTree engine that merges rows while the
// setting is throw, its default, with Code 344, measured on 24.10 and 26.10 for
// the Replacing, Collapsing, VersionedCollapsing, Summing and Aggregating
// engines. The guard reads the server's default, since a server may set
// another, and stands where nothing has been created yet; a declaration that
// sets the mode leaves the decision to the server.
func (c carriedStorage) projectionModeGuard(table string, create *ast.CreateTableNode) (string, error) {
	if len(c.projections) == 0 {
		return "", nil
	}
	declared, err := declaredStorage(create)
	if err != nil {
		return "", err
	}
	engine := declared.engine()
	if !mergesRows(engine) || settingNamed(declared.Settings.Value, "deduplicate_merge_projection_mode") {
		return "", nil
	}
	return fmt.Sprintf("throwIf((SELECT value FROM system.merge_tree_settings WHERE name = 'deduplicate_merge_projection_mode') NOT IN ('drop', 'rebuild'), %s)",
		chcolumn.StringLiteral("cannot create "+table+" again: "+table+" has projections, and ClickHouse keeps a projection on a "+engineFamily(engine)+
			" table only when deduplicate_merge_projection_mode is drop or rebuild, which neither the declaration nor the server sets. "+
			"Set it in the table's declared SETTINGS, or drop the projections first")), nil
}

// settingNamed reports whether a SETTINGS body assigns the setting.
func settingNamed(settings, setting string) bool {
	lex := lexer.NewLexerWithOptions(settings, dialectlexer.Options(platform.ClickHouse))
	var previous lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return false
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		if token.MatchOperatorValue("=") && previous.MatchIdentifierValue(setting) {
			return true
		}
		previous = token
	}
}

// rebuildMergePolicy says whose merges can remove rows while the table is
// copied: the new table's, which the rebuild stops, and the table's, which it
// cannot stop without touching the live table.
type rebuildMergePolicy struct {
	// copyRemoval says why the new table's merges remove rows, and is empty
	// when they remove none.
	copyRemoval       string
	sourceRemovesRows bool
}

// copyRemovesRows reports whether the new table's merges remove rows.
func (p rebuildMergePolicy) copyRemovesRows() bool { return p.copyRemoval != "" }

func rebuildMerges(table *difftypes.TableDiff, create *ast.CreateTableNode) (rebuildMergePolicy, error) {
	declared, err := declaredStorage(create)
	if err != nil {
		return rebuildMergePolicy{}, err
	}
	observed, found, err := schemaext.FacetAs[*chschema.ObservedTable](table.Current.Table.Facets, chschema.TableKind)
	if err != nil {
		return rebuildMergePolicy{}, err
	}
	// A table whose storage the read did not report could merge rows away; the
	// check then does not refuse a copy with more rows than it has.
	source := !found || mergeRemoval(observed.Engine, observed.TTL) != ""
	return rebuildMergePolicy{copyRemoval: mergeRemoval(declared.engine(), declared.TTL.Value), sourceRemovesRows: source}, nil
}

// newTable is the storage the new table declares.
type newTable struct{ chschema.DesiredTable }

// engine is the new table's engine, MergeTree where the declaration names none.
func (t newTable) engine() string { return cmp.Or(strings.TrimSpace(t.Engine.Value), "MergeTree") }

// declaredStorage returns the storage the new table declares, empty where the
// declaration states none.
func declaredStorage(create *ast.CreateTableNode) (newTable, error) {
	declared, found, err := schemaext.FacetAs[*chschema.DesiredTable](create.Facets, chschema.TableKind)
	if err != nil || !found {
		return newTable{}, err
	}
	return newTable{*declared}, nil
}

// engineFamily is an engine's name without its parameters, as declared.
func engineFamily(engine string) string {
	name, _, _ := strings.Cut(engine, "(")
	return strings.TrimSpace(name)
}

// mergesRows reports a MergeTree engine whose merges combine rows, which is
// every one of them but MergeTree itself.
func mergesRows(engine string) bool {
	name := engineName(engine)
	return strings.HasSuffix(name, "MERGETREE") && name != "MERGETREE"
}

// mergeRemoval says why a table's background merges remove rows, and is empty
// when they remove none: its engine merges rows, or it is a MergeTree table
// with a TTL. INSERT keeps an expired row, and a merge deletes it, measured on
// 24.10 and 26.10.
func mergeRemoval(engine, ttl string) string {
	switch {
	case mergesRows(engine):
		return "its engine " + engineFamily(engine) + " merges rows"
	case strings.HasSuffix(engineName(engine), "MERGETREE") && strings.TrimSpace(ttl) != "":
		return "its TTL removes expired rows"
	}
	return ""
}
