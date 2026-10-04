package ydb

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/renderdiag"
	"ptah.run/internal/ydbgap"
	"ptah.run/internal/ydbsequence"
	"ptah.run/internal/ydbttl"
	"ptah.run/internal/ydbtype"
)

// mysqlTableOptions are the table options a MySQL-family declaration carries
// that describe MySQL's storage and nothing YDB has. They are dropped with a
// record, as every non-MySQL renderer drops them. An option outside this set
// is a YDB table setting, which is a later phase's work and is refused.
var mysqlTableOptions = map[string]bool{
	"ENGINE":            true,
	"CHARSET":           true,
	"DEFAULT CHARSET":   true,
	"CHARACTER SET":     true,
	"COLLATE":           true,
	"DEFAULT COLLATE":   true,
	"ROW_FORMAT":        true,
	"STATS_PERSISTENT":  true,
	"STATS_AUTO_RECALC": true,
}

// renderCreateTable writes one CREATE TABLE with its key and its indexes.
//
// The key is a table-level PRIMARY KEY clause, because the inline spelling is
// a parse error (`id Int64 PRIMARY KEY` answers `mismatched input 'PRIMARY'`).
// Every key column is written NOT NULL: YDB makes a key column nullable unless
// it says otherwise and then accepts one row whose key is NULL, which no other
// engine allows and no Ptah declaration can ask for.
//
// The indexes the node carries are written inside the statement. That is the
// only way to give a new table a unique index on a target without
// [capability.UniqueIndexOnExistingTable]: measured, ALTER TABLE ... ADD INDEX
// ... GLOBAL UNIQUE SYNC is refused on an existing table, even an empty one,
// and the same index in CREATE TABLE is accepted.
func (r *Renderer) renderCreateTable(node *ast.CreateTableNode) error {
	if err := r.refuseTableDeclarations(node); err != nil {
		return err
	}
	keyColumns, err := r.keyColumns(node)
	if err != nil {
		return err
	}

	lines := make([]string, 0, len(node.Columns)+len(node.Indexes)+1)
	columnTypes := make(map[string]string, len(node.Columns))
	for _, column := range node.Columns {
		definition, ydbType, err := r.columnDefinition(node.Name, column, slices.Contains(keyColumns, column.Name))
		if err != nil {
			return err
		}
		columnTypes[column.Name] = ydbType
		lines = append(lines, definition)
	}
	if err := checkKeyTypes(node.Name, keyColumns, columnTypes); err != nil {
		return err
	}
	settings, err := r.tableSettings(node, columnTypes)
	if err != nil {
		return err
	}
	quotedKey := make([]string, 0, len(keyColumns))
	for _, column := range keyColumns {
		quotedKey = append(quotedKey, quote(column))
	}
	lines = append(lines, "PRIMARY KEY ("+strings.Join(quotedKey, ", ")+")")
	uniques, err := r.uniqueIndexes(node, keyColumns)
	if err != nil {
		return err
	}
	var partitioning []string
	named := make(map[string]bool, len(node.Indexes)+len(uniques))
	for _, index := range append(slices.Clone(node.Indexes), uniques...) {
		clause, err := r.inlineIndex(node.Name, index, keyColumns, columnTypes)
		if err != nil {
			return err
		}
		if named[clause.name] {
			return refuseFact(fmt.Sprintf("table %q", node.Name), fmt.Sprintf("two of its indexes are named %q, "+
				"and YDB names an index once per table", clause.name))
		}
		named[clause.name] = true
		lines = append(lines, clause.String())
		if statement := clause.partitioningStatement(node.Name); statement != "" {
			partitioning = append(partitioning, statement)
		}
	}

	guard, err := r.createGuard(node)
	if err != nil {
		return err
	}
	r.w.WriteLinef("CREATE TABLE%s %s (", guard, tablePath(node.Name))
	r.w.WriteLine("    " + strings.Join(lines, ",\n    "))
	closing := ")"
	if len(settings) > 0 {
		closing += " WITH (" + strings.Join(settings, ", ") + ")"
	}
	if custom := strings.TrimSpace(node.CustomSQL); custom != "" {
		closing += " " + custom
	}
	r.w.WriteLine(closing + ";")
	for _, statement := range partitioning {
		r.w.WriteLine(statement)
	}
	return nil
}

// createGuard writes IF NOT EXISTS where the declaration asked for one.
func (r *Renderer) createGuard(node *ast.CreateTableNode) (string, error) {
	if !node.IfNotExists {
		return "", nil
	}
	if !r.caps.Has(capability.ObjectExistenceGuards) {
		return "", refuseKey(capability.ObjectExistenceGuards, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", node.Name))
	}
	return " IF NOT EXISTS", nil
}

// refuseTableDeclarations refuses the table-level declarations YDB cannot
// hold, and records the MySQL storage options it drops.
func (r *Renderer) refuseTableDeclarations(node *ast.CreateTableNode) error {
	subject := fmt.Sprintf("table %q", node.Name)
	switch {
	case len(node.Columns) == 0:
		return refuseFact(subject, "a YDB table needs at least its key column")
	case strings.TrimSpace(node.SelectBody) != "":
		return refuseFact(subject, "the YDB renderer writes no CREATE TABLE ... AS SELECT")
	case node.Comment != "":
		return refuseGap(ydbgap.Comments, "the comment on "+subject)
	case node.Unlogged:
		return refuseFact(subject, "UNLOGGED is PostgreSQL's; YDB has no unlogged table")
	case node.Partition != nil:
		return refuseFact(subject, "PARTITION BY is PostgreSQL's; a YDB row table splits itself by key range, "+
			"which is a table setting rather than a clause")
	case node.RowTTL != nil:
		return r.keyed(capability.RowLevelTTL, "row-level TTL", "the row-level TTL on "+subject)
	}
	var dropped map[string]string
	// Sorted, so that of two options YDB cannot carry the refusal names the
	// same one on every run.
	for _, key := range slices.Sorted(maps.Keys(node.Options)) {
		value := node.Options[key]
		upper := strings.ToUpper(strings.TrimSpace(key))
		if !mysqlTableOptions[upper] {
			return refuseGap(ydbgap.TableSettings, fmt.Sprintf("the table option %s=%s on %s", key, value, subject))
		}
		if dropped == nil {
			dropped = make(map[string]string)
		}
		dropped[key] = value
	}
	r.sink.RecordDroppedTableOptions(node.Name, dropped)
	return r.refuseTableConstraints(node)
}

// tableSettings writes the settings the table's WITH clause carries: its TTL,
// from the row deletion policy. The policy's column has to be one the table
// declares, of a type YDB reads a TTL from; see [ydbttl.ColumnRefusal].
func (r *Renderer) tableSettings(node *ast.CreateTableNode, columnTypes map[string]string) ([]string, error) {
	policy := node.RowDeletionPolicy
	if policy.IsZero() {
		return nil, nil
	}
	subject := fmt.Sprintf("the row deletion policy of table %q", node.Name)
	setting, err := r.ttlSetting(subject, policy)
	if err != nil {
		return nil, err
	}
	ydbType, declared := columnTypes[policy.Column]
	if !declared {
		return nil, refuseFact(subject, fmt.Sprintf("it reads column %q, which the table does not declare "+
			"(`Cannot enable TTL on unknown column`)", policy.Column))
	}
	unit, err := ydbttl.Unit(policy.Unit)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	if reason := ydbttl.ColumnRefusal(policy.Column, ydbType, unit); reason != "" {
		return nil, refuseFact(subject, reason)
	}
	return []string{"TTL = " + setting}, nil
}

// ttlSetting writes a row deletion policy as the value of YDB's TTL setting,
// refusing an integer column's unit on a target without
// [capability.RowDeletionPolicyEpochColumn], and an interval or unit YDB
// would refuse or keep as something else.
func (r *Renderer) ttlSetting(subject string, policy *ast.RowDeletionPolicySpec) (string, error) {
	if !r.caps.Has(capability.RowDeletionPolicy) {
		return "", refuseKey(capability.RowDeletionPolicy, subject)
	}
	if strings.TrimSpace(policy.Unit) != "" && !r.caps.Has(capability.RowDeletionPolicyEpochColumn) {
		return "", refuseKey(capability.RowDeletionPolicyEpochColumn, subject+" reads an integer column counting "+policy.Unit)
	}
	setting, err := ydbttl.Setting(policy, quote)
	if err != nil {
		return "", refuseFact(subject, err.Error())
	}
	return setting, nil
}

// refuseTableConstraints refuses every table constraint but the key. YDB has
// no CHECK, FOREIGN KEY or UNIQUE constraint and no CONSTRAINT clause: each
// is a parse error.
func (r *Renderer) refuseTableConstraints(node *ast.CreateTableNode) error {
	for _, constraint := range node.Constraints {
		if err := r.refuseConstraint(node.Name, constraint); err != nil {
			return err
		}
	}
	return nil
}

// refuseConstraint answers a table-level constraint. The key is the one kind
// that renders, and it renders as the PRIMARY KEY clause keyColumns builds.
func (r *Renderer) refuseConstraint(table string, constraint *ast.ConstraintNode) error {
	if constraint == nil {
		return refuseFact(fmt.Sprintf("table %q", table), "a constraint is nil")
	}
	subject := fmt.Sprintf("constraint %q on table %q", constraint.Name, table)
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		return r.refuseKeyAttributes(table, constraint)
	case ast.UniqueConstraint:
		if r.caps.Has(capability.UniqueConstraints) {
			return r.keyed(capability.UniqueConstraints, "UNIQUE constraint", subject+" is UNIQUE")
		}
		_, err := uniqueConstraintIndex(table, constraint)
		return err
	case ast.CheckConstraint:
		return r.keyed(capability.CheckConstraints, "CHECK constraint", subject+" is a CHECK")
	case ast.ForeignKeyConstraint:
		return r.keyed(capability.ForeignKeys, "foreign key", subject+" is a FOREIGN KEY")
	default:
		return refuseFact(subject, fmt.Sprintf("YDB has no %s constraint", constraint.Type))
	}
}

// refuseKeyAttributes refuses what a key may say beyond its columns. A YDB
// key has no name (`CONSTRAINT pk PRIMARY KEY (id)` is a parse error), so a
// name is dropped with a record; a key part with an expression, an order or a
// prefix has no spelling and is refused.
func (r *Renderer) refuseKeyAttributes(table string, constraint *ast.ConstraintNode) error {
	if constraint.Name != "" {
		r.sink.Record(renderdiag.PropertyOmission(renderdiag.TableKind, table, "primary key name", constraint.Name))
	}
	subject := fmt.Sprintf("the primary key of table %q", table)
	if constraint.Comment != "" {
		return refuseGap(ydbgap.Comments, "the comment on "+subject)
	}
	for _, part := range constraint.ColumnParts {
		if part.Expr != "" || part.Desc || part.Prefix != "" {
			return refuseFact(subject, "a YDB key is a list of columns, with no expression, order or prefix")
		}
	}
	return nil
}

// keyColumns returns the table's key in order: the PRIMARY KEY constraint's
// columns, or the columns marked primary in declaration order.
func (r *Renderer) keyColumns(node *ast.CreateTableNode) ([]string, error) {
	var fromConstraint []string
	for _, constraint := range node.Constraints {
		if constraint != nil && constraint.Type == ast.PrimaryKeyConstraint {
			fromConstraint = append(fromConstraint, constraintColumns(constraint)...)
		}
	}
	var fromColumns []string
	for _, column := range node.Columns {
		if column.Primary {
			fromColumns = append(fromColumns, column.Name)
		}
	}
	switch {
	case len(fromConstraint) > 0 && len(fromColumns) > 0 && !slices.Equal(fromConstraint, fromColumns):
		return nil, refuseFact(fmt.Sprintf("table %q", node.Name),
			"the table declares a PRIMARY KEY constraint and a different set of primary columns")
	case len(fromConstraint) > 0:
		return fromConstraint, nil
	case len(fromColumns) > 0:
		return fromColumns, nil
	default:
		return nil, refuseKey(capability.PrimaryKeyRequired, fmt.Sprintf("table %q declares no primary key", node.Name))
	}
}

// constraintColumns are a constraint's column names, from its parts where it
// carries them.
func constraintColumns(constraint *ast.ConstraintNode) []string {
	if len(constraint.ColumnParts) == 0 {
		return constraint.Columns
	}
	names := make([]string, 0, len(constraint.ColumnParts))
	for _, part := range constraint.ColumnParts {
		names = append(names, part.Name)
	}
	return names
}

// checkKeyTypes refuses a key over a type YDB cannot order.
func checkKeyTypes(table string, keyColumns []string, columnTypes map[string]string) error {
	for _, column := range keyColumns {
		ydbType, declared := columnTypes[column]
		if !declared {
			return refuseFact(fmt.Sprintf("the primary key of table %q", table),
				fmt.Sprintf("it names column %q, which the table does not declare", column))
		}
		if !ydbtype.KeyComparable(ydbType) {
			return refuseFact(fmt.Sprintf("the primary key of table %q", table),
				fmt.Sprintf("column %q is %s, which YDB refuses in a key (`wrong key type %s`)", column, ydbType, ydbType))
		}
	}
	return nil
}

// renderColumnNode writes a column definition on its own, which is how a
// caller asks for one outside a statement. A UNIQUE column is refused: its
// UNIQUE is an index of the table, which a definition alone cannot carry.
func (r *Renderer) renderColumnNode(column *ast.ColumnNode) error {
	if column.Unique && !r.caps.Has(capability.UniqueConstraints) {
		return refuseFact(fmt.Sprintf("column %q", column.Name),
			"its UNIQUE is a unique index of its table on YDB, which a column definition alone cannot carry")
	}
	definition, _, err := r.columnDefinition("", column, column.Primary)
	if err != nil {
		return err
	}
	r.w.Write(definition)
	return nil
}

// renderConstraintNode writes a key clause on its own, or the unique index
// clause a UNIQUE constraint renders as, and refuses every other constraint, as
// CREATE TABLE does.
func (r *Renderer) renderConstraintNode(constraint *ast.ConstraintNode) error {
	if err := r.refuseConstraint("", constraint); err != nil {
		return err
	}
	if constraint.Type == ast.UniqueConstraint {
		index, err := uniqueConstraintIndex("", constraint)
		if err != nil {
			return err
		}
		clause, err := r.indexClauseOf(index)
		if err != nil {
			return err
		}
		r.w.Write(clause.String())
		return nil
	}
	quoted := make([]string, 0, len(constraint.Columns))
	for _, column := range constraintColumns(constraint) {
		quoted = append(quoted, quote(column))
	}
	r.w.Write("PRIMARY KEY (" + strings.Join(quoted, ", ") + ")")
	return nil
}

// columnDefinition writes one column, and returns the YDB type it chose so the
// caller can hold a key or an index to it.
func (r *Renderer) columnDefinition(table string, column *ast.ColumnNode, key bool) (definition, ydbType string, err error) {
	subject := fmt.Sprintf("column %q", column.Name)
	if table != "" {
		subject = fmt.Sprintf("column %q of table %q", column.Name, table)
	}
	if err := r.refuseColumnDeclarations(subject, column); err != nil {
		return "", "", err
	}
	mapping, err := r.columnType(subject, column)
	if err != nil {
		return "", "", err
	}
	if mapping.Serial {
		if err := r.serialSequence(subject, table, column); err != nil {
			return "", "", err
		}
	}
	if mapping.Dropped != "" {
		r.sink.Record(renderdiag.PropertyOmission(renderdiag.ColumnKind,
			renderdiag.ColumnName(table, column.Name), renderdiag.TypeModifierProperty,
			column.Type+": "+mapping.Dropped))
	}

	parts := []string{quote(column.Name), mapping.Type}
	notNull := key || mapping.Serial || !column.Nullable
	if notNull {
		parts = append(parts, "NOT NULL")
	}
	if notNull && nullDefault(column.Default) {
		return "", "", refuseFact(subject, "the column is NOT NULL and its default is NULL")
	}
	defaultClause, err := r.defaultClause(subject, column.Default, mapping.Type)
	if err != nil {
		return "", "", err
	}
	if defaultClause != "" {
		parts = append(parts, defaultClause)
	}
	return strings.Join(parts, " "), mapping.Type, nil
}

// nullDefault reports a default that is the literal NULL.
func nullDefault(value *ast.DefaultValue) bool {
	if value == nil || strings.TrimSpace(value.Expression) != "" || !value.HasLiteral() {
		return false
	}
	_, isNull := ydbtype.DeclaredValue(value.Value)
	return isNull
}

// refuseColumnDeclarations refuses what a column may say that YDB has no
// clause for. Each is a parse error on every measured line.
func (r *Renderer) refuseColumnDeclarations(subject string, column *ast.ColumnNode) error {
	switch {
	case column.EnumType:
		return r.keyed(capability.EnumInlineColumn, "enum column", subject+" is of an enum type "+column.Type)
	case column.Check != "":
		return r.keyed(capability.CheckConstraints, "CHECK constraint", subject+" declares CHECK ("+column.Check+")")
	case column.ForeignKey != nil:
		return r.keyed(capability.ForeignKeys, "foreign key", subject+" references "+column.ForeignKey.Table)
	case column.Unique && r.caps.Has(capability.UniqueConstraints):
		return r.keyed(capability.UniqueConstraints, "UNIQUE constraint", subject+" is UNIQUE")
	case column.GeneratedExpression != "":
		return r.keyed(capability.GeneratedColumns, "generated column", subject+" is generated")
	case column.NotNullConstraintName != "":
		return r.keyed(capability.NamedNotNullConstraints, "named NOT NULL constraint",
			subject+" names its NOT NULL constraint "+column.NotNullConstraintName)
	case column.UpdateExpression != "":
		return refuseFact(subject, "ON UPDATE is MySQL's; YDB has no such clause")
	case column.Collate != "":
		return refuseFact(subject, "YDB has no column collation (`COLLATE` is a parse error); Utf8 compares bytes")
	case column.Charset != "" && !isUTF8Charset(column.Charset):
		return refuseFact(subject, "YDB stores text as UTF-8 only, and the column declares character set "+column.Charset)
	case column.Comment != "":
		return refuseGap(ydbgap.Comments, "the comment on "+subject)
	}
	return nil
}

// isUTF8Charset reports a character set that names UTF-8, which is what a
// YDB Utf8 column holds, so declaring it drops nothing.
func isUTF8Charset(charset string) bool {
	switch strings.ToLower(strings.TrimSpace(charset)) {
	case "utf8", "utf8mb4", "utf8mb3", "utf-8":
		return true
	default:
		return false
	}
}

// columnType maps the column's declared type, turning an auto-increment or
// identity declaration into the Serial type of the same width.
func (r *Renderer) columnType(subject string, column *ast.ColumnNode) (ydbtype.Mapping, error) {
	mapping, err := ydbtype.Map(column.Type, r.caps)
	if err != nil {
		return ydbtype.Mapping{}, typeRefusal(subject, err)
	}
	if !column.AutoInc && column.IdentityGeneration == "" {
		return mapping, nil
	}
	serial, ok := ydbtype.SerialFor(mapping.Type)
	switch {
	case mapping.Serial:
		return mapping, nil
	case !ok:
		return ydbtype.Mapping{}, refuseFact(subject,
			"an auto-increment column is a Serial, which YDB has for Int16, Int32 and Int64 only, and the column is "+mapping.Type)
	case !r.caps.Has(capability.SerialColumns):
		return ydbtype.Mapping{}, refuseKey(capability.SerialColumns, subject+" increments itself")
	}
	return ydbtype.Mapping{Type: serial, Dropped: mapping.Dropped, Serial: true}, nil
}

// serialSequence checks what a Serial column declares of its sequence.
//
// A Serial fills the column only when the insert names no value, which is
// PostgreSQL's BY DEFAULT, so GENERATED ALWAYS is refused. Its sequence takes a
// start and an increment and nothing else (measured: MINVALUE, MAXVALUE, CACHE
// and CYCLE in ALTER SEQUENCE are parse errors), so raw identity options are
// refused, and a start or an increment YDB would refuse is refused here with
// [ydbsequence.Parse]'s reason.
//
// A valid start or increment other than 1 is not written here: CREATE TABLE
// has no clause for it, and the ALTER SEQUENCE that sets it names the sequence
// by an absolute path that includes the database, which a render without one
// cannot know. A plan writes that statement after the table and takes the
// settings off the column it hands this renderer; one that reaches this
// renderer anyway is reported as dropped.
func (r *Renderer) serialSequence(subject, table string, column *ast.ColumnNode) error {
	if strings.EqualFold(column.IdentityGeneration, "ALWAYS") {
		return refuseFact(subject, "GENERATED ALWAYS refuses an explicit value, and a YDB Serial takes one")
	}
	if column.IdentityOptions != "" {
		return refuseFact(subject, "a YDB Serial's sequence takes a start and an increment and nothing else, "+
			"so declare those with identity_start and identity_increment rather than the options "+column.IdentityOptions)
	}
	settings, err := ydbsequence.Parse(column.IdentityStart, column.IdentityIncrement)
	if err != nil {
		return refuseFact(subject, err.Error())
	}
	if settings.IsDefault() {
		return nil
	}
	if !r.caps.Has(capability.SerialSequenceOptions) {
		return refuseKey(capability.SerialSequenceOptions, subject+" gives its sequence a start or an increment")
	}
	r.sink.RecordLostIdentity(renderdiag.ColumnName(table, column.Name), renderdiag.Identity{
		Start:     column.IdentityStart,
		Increment: column.IdentityIncrement,
	})
	return nil
}

// defaultClause writes a column's DEFAULT, or nothing.
//
// A literal is written in the column's own YQL type; see [ydbtype.Literal].
// An expression is refused on a target without
// [capability.ExpressionDefaults]. A NULL default on a nullable column writes
// nothing, because YDB refuses `DEFAULT NULL` (`default expr with null is not
// supported`) and a nullable column without a default is already NULL where
// an insert names no value; on a NOT NULL column it is a contradiction and is
// refused by the caller, which knows the column; see [nullDefault].
func (r *Renderer) defaultClause(subject string, value *ast.DefaultValue, ydbType string) (string, error) {
	switch {
	case value == nil:
		return "", nil
	case strings.TrimSpace(value.Expression) != "":
		return "", r.keyed(capability.ExpressionDefaults, "expression default",
			subject+" defaults to the expression "+value.Expression)
	case !value.HasLiteral():
		return "", nil
	}
	raw, isNull := ydbtype.DeclaredValue(value.Value)
	if isNull {
		return "", nil
	}
	literal, err := ydbtype.Literal(ydbType, raw, r.caps)
	if err != nil {
		return "", typeRefusal(subject, err)
	}
	return "DEFAULT " + literal, nil
}

// typeRefusal turns a type map refusal into the renderer's error, keyed where
// the refusal names a capability.
func typeRefusal(subject string, err error) error {
	refusal, ok := errors.AsType[*ydbtype.Refusal](err)
	switch {
	case !ok:
		return refuseFact(subject, err.Error())
	case refusal.Key != "":
		return refuseKey(refusal.Key, fmt.Sprintf("%s: %s (%s)", subject, refusal.Declared, refusal.Reason))
	default:
		return refuseFact(subject, refusal.Error())
	}
}

// terminated ends a statement with exactly one semicolon.
func terminated(statement string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(statement), ";")
	return strings.TrimSpace(trimmed) + ";"
}
