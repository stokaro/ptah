// Package ydbexternal owns the rules of YDB's external data sources and
// external tables: what a declaration of one may say, the statements that
// create, replace and drop one, and the form in which a declaration and the
// server's description of one are compared.
//
// An external data source names another system -- an object storage bucket, a
// PostgreSQL, ClickHouse, MySQL or YDB database -- and how YDB authenticates to
// it. An external table is a set of columns over files in a data source of type
// ObjectStorage. Neither holds data in YDB: a read of an external table, or a
// query naming a data source, fetches rows from the other system, and that is
// why the migrator refuses a check that reads one (see
// [ptah.run/internal/dbschema/ydb.ProveLocalReads]). Measured on local-ydb
// 25.1.4.7 and 26.2.1.14 with EnableExternalDataSources on:
//
//   - CREATE EXTERNAL DATA SOURCE takes SOURCE_TYPE, LOCATION, AUTH_METHOD and
//     options of each source type's own; the server keeps every value as
//     written and every option name in upper case, and refuses an option the
//     source type does not take (`ObjectStorage source doesn't support any
//     properties`), an unknown source type (`External source with type
//     postgresql was not found`) and an unknown auth method.
//   - A credential is never an option's value. An option ending in _SECRET_NAME
//     names a deprecated secret object, and one ending in _SECRET_PATH names a
//     YDB secret by its path, which 25.4.1.15 and later take and the server
//     stores as an absolute path; 25.1.4.7 reads it as a missing name
//     (`PASSWORD_SECRET_NAME requires key`).
//   - DescribeExternalDataSource returns the source type, the location and
//     the options, with REFERENCES added: the absolute paths of the external
//     tables over the source, which the server keeps and Ptah leaves out.
//   - CREATE EXTERNAL TABLE takes columns, DATA_SOURCE, LOCATION and options
//     such as FORMAT and COMPRESSION. A column keeps its type and NOT NULL;
//     DEFAULT is accepted and dropped, PRIMARY KEY and FAMILY are refused, and
//     TzTimestamp is `not supported by storage`. DescribeExternalTable returns
//     the data source as an absolute path and each option's value as a JSON
//     array of one string, except PARTITIONED_BY, which is a JSON array of
//     column names as written.
//   - No ALTER changes either object (`Alter operation for
//     EXTERNAL_DATA_SOURCE objects is not implemented`), and CREATE OR
//     REPLACE replaces either object in one statement behind
//     EnableReplaceIfExistsForExternalEntities, off by default on every line.
//   - 25.4.1.15 and later refuse to drop a data source an external table
//     reads (`Other entities depend on this data source`). 25.1.4.7 to
//     25.3.1.25 drop it, and the table over it then cannot be dropped (`path
//     hasn't been resolved` for the source's path), so a drop of a table has
//     to come first.
//   - A relative path in a name, in DATA_SOURCE or in a _SECRET_PATH option
//     resolves under PRAGMA TablePathPrefix, as a table's name does.
//
// The types here are YDB's alone; no other engine has either object.
package ydbexternal

import (
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbtype"
)

// The attributes that declare an external data source or an external table.
// The annotation parser, the YAML reader and the annotation registry read
// these names.
const (
	// AttributeName is the object's name, the last segment of its path.
	AttributeName = "name"
	// AttributeSchema is the directory that holds the object, relative to the
	// database root.
	AttributeSchema = "schema"
	// AttributeSourceType is a data source's SOURCE_TYPE.
	AttributeSourceType = "source_type"
	// AttributeLocation is a data source's or a table's LOCATION.
	AttributeLocation = "location"
	// AttributeAuthMethod is a data source's AUTH_METHOD.
	AttributeAuthMethod = "auth_method"
	// AttributeOptions are every other option, `NAME=value` separated by `;`.
	AttributeOptions = "options"
	// AttributeDataSource is a table's DATA_SOURCE: the data source's path.
	AttributeDataSource = "data_source"
	// AttributeColumns are a table's columns, `name Type [NOT NULL]`
	// separated by commas.
	AttributeColumns = "columns"
)

// The options a statement writes from attributes of their own, and the one the
// server adds to a description.
const (
	optionSourceType = "SOURCE_TYPE"
	optionLocation   = "LOCATION"
	optionAuthMethod = "AUTH_METHOD"
	optionDataSource = "DATA_SOURCE"
	// optionReferences lists the external tables over a data source. The
	// server maintains it, and it is not a declaration's to write.
	optionReferences = "REFERENCES"
	// optionPartitionedBy is the one table option whose value is a JSON
	// array of column names rather than a string.
	optionPartitionedBy = "PARTITIONED_BY"
)

// secretPathSuffix ends the name of every option that names a YDB secret by
// its path.
const secretPathSuffix = "_SECRET_PATH"

// DeclarationError is an attribute whose value a declaration cannot carry.
type DeclarationError struct {
	// Attribute is the attribute's name.
	Attribute string
	// Reason says what the attribute takes.
	Reason string
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Attribute, e.Reason)
}

// DataSource is what declares an external data source, apart from its path.
type DataSource struct {
	// SourceType is SOURCE_TYPE, such as ObjectStorage or PostgreSQL.
	SourceType string
	// Location is LOCATION, or empty where the source type names its server
	// another way, as MDB_CLUSTER_ID does.
	Location string
	// AuthMethod is AUTH_METHOD, such as NONE or BASIC.
	AuthMethod string
	// Options are every other option, keyed by upper-case name.
	Options map[string]string
}

// Column is one column of an external table.
type Column struct {
	// Name is the column's name.
	Name string
	// Type is its YQL type, such as Utf8 or Decimal(22,9).
	Type string
	// NotNull says the column is declared NOT NULL.
	NotNull bool
}

// Table is what declares an external table, apart from its path.
type Table struct {
	// DataSource is the data source's path, relative to the database root or
	// absolute.
	DataSource string
	// Location is LOCATION, the files' path under the data source.
	Location string
	// Columns are the table's columns, in order.
	Columns []Column
	// Options are every other option, such as FORMAT, keyed by upper-case
	// name.
	Options map[string]string
}

// ParseOptions reads an options attribute, `NAME=value` pairs separated by
// `;`, into a map keyed by upper-case name. Each value is kept as written,
// without the spaces around it; `\;` writes a semicolon into a value, as
// `CSV_DELIMITER=\;` does, and `\\` a backslash. reserved are the names the
// declaration writes through attributes of their own, which the attribute may
// not repeat; see [CheckOptions] for the rest of what is refused.
func ParseOptions(text string, reserved ...string) (map[string]string, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	var names, values []string
	for _, pair := range splitEscaped(text) {
		if strings.TrimSpace(pair) == "" {
			continue
		}
		name, value, found := strings.Cut(pair, "=")
		if !found || strings.TrimSpace(name) == "" {
			return nil, &DeclarationError{Attribute: AttributeOptions,
				Reason: fmt.Sprintf("%q is not NAME=value", strings.TrimSpace(pair))}
		}
		names = append(names, name)
		values = append(values, value)
	}
	return checkOptions(names, values, reserved)
}

// CheckOptions checks options declared as a map, as a YAML document writes
// them, and returns them keyed by upper-case name with each value trimmed. A
// name is letters, digits and underscores; one of reserved, REFERENCES, which
// the server keeps, and a name written twice in two letter cases are refused.
func CheckOptions(options map[string]string, reserved ...string) (map[string]string, error) {
	names := slices.Sorted(maps.Keys(options))
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, options[name])
	}
	return checkOptions(names, values, reserved)
}

func checkOptions(names, values []string, reserved []string) (map[string]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	checked := make(map[string]string, len(names))
	for i, written := range names {
		name := strings.ToUpper(strings.TrimSpace(written))
		switch {
		case name == "" || !optionName(name):
			return nil, &DeclarationError{Attribute: AttributeOptions,
				Reason: fmt.Sprintf("%q is not an option name: letters, digits and underscores", name)}
		case slices.Contains(reserved, name):
			return nil, &DeclarationError{Attribute: AttributeOptions,
				Reason: fmt.Sprintf("%s is written with its own attribute, %s", name, strings.ToLower(name))}
		case name == optionReferences:
			return nil, &DeclarationError{Attribute: AttributeOptions,
				Reason: "REFERENCES lists the external tables over a data source, and the server keeps it"}
		}
		if _, repeated := checked[name]; repeated {
			return nil, &DeclarationError{Attribute: AttributeOptions, Reason: name + " is written twice"}
		}
		checked[name] = strings.TrimSpace(values[i])
	}
	return checked, nil
}

// FormatOptions writes options as an options attribute, `NAME=value` pairs
// separated by `;` in name order, with a semicolon or a backslash in a value
// escaped, which [ParseOptions] reads back.
func FormatOptions(options map[string]string) string {
	escape := strings.NewReplacer(`\`, `\\`, ";", `\;`)
	pairs := make([]string, 0, len(options))
	for _, name := range slices.Sorted(maps.Keys(options)) {
		pairs = append(pairs, name+"="+escape.Replace(options[name]))
	}
	return strings.Join(pairs, ";")
}

// splitEscaped splits text at every `;` a backslash does not escape, and
// reads `\;` as a semicolon and `\\` as a backslash.
func splitEscaped(text string) []string {
	var (
		parts   []string
		current strings.Builder
	)
	for i := 0; i < len(text); i++ {
		switch {
		case text[i] == '\\' && i+1 < len(text) && (text[i+1] == ';' || text[i+1] == '\\'):
			i++
			current.WriteByte(text[i])
		case text[i] == ';':
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(text[i])
		}
	}
	return append(parts, current.String())
}

// FormatColumns writes columns as a columns attribute, which [ParseColumns]
// reads back. A name that is not a plain identifier is written in backticks.
func FormatColumns(columns []Column) string {
	entries := make([]string, 0, len(columns))
	for _, column := range columns {
		name := column.Name
		if !plainName(name) {
			name = "`" + name + "`"
		}
		entry := name + " " + column.Type
		if column.NotNull {
			entry += " NOT NULL"
		}
		entries = append(entries, entry)
	}
	return strings.Join(entries, ", ")
}

// plainName reports a name of ASCII letters, digits and underscores that does
// not start with a digit.
func plainName(name string) bool {
	for i, r := range name {
		if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r == '_' || r >= '0' && r <= '9' && i > 0) {
			return false
		}
	}
	return name != ""
}

// DataSourceReserved and TableReserved are the option names the declaration
// of each object writes with an attribute of its own.
var (
	DataSourceReserved = []string{optionSourceType, optionLocation, optionAuthMethod}
	TableReserved      = []string{optionDataSource, optionLocation}
)

// optionName reports a name of ASCII letters, digits and underscores.
func optionName(name string) bool {
	for _, r := range name {
		if !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
			return false
		}
	}
	return true
}

// ParseColumns reads a columns attribute: `name Type [NOT NULL]` entries
// separated by commas outside parentheses, so `amount Decimal(22,9)` is one
// column. A name may be quoted with backticks. DEFAULT, PRIMARY KEY and FAMILY
// are refused: the server drops a default without a word, and refuses the
// other two.
func ParseColumns(text string) ([]Column, error) {
	var columns []Column
	for _, entry := range splitTopLevel(text) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, &DeclarationError{Attribute: AttributeColumns, Reason: "an entry is empty"}
		}
		column, err := parseColumn(entry)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	if err := CheckColumns(columns); err != nil {
		return nil, err
	}
	return columns, nil
}

// CheckColumns refuses an external table's columns when there are none, when
// one has no name or no type, or when a name is written twice.
func CheckColumns(columns []Column) error {
	if len(columns) == 0 {
		return &DeclarationError{Attribute: AttributeColumns, Reason: "an external table needs a column"}
	}
	seen := make(map[string]bool, len(columns))
	for _, column := range columns {
		switch {
		case strings.TrimSpace(column.Name) == "":
			return &DeclarationError{Attribute: AttributeColumns, Reason: "a column has no name"}
		case strings.TrimSpace(column.Type) == "":
			return &DeclarationError{Attribute: AttributeColumns, Reason: "column " + column.Name + " has no type"}
		case seen[column.Name]:
			return &DeclarationError{Attribute: AttributeColumns, Reason: column.Name + " is written twice"}
		}
		seen[column.Name] = true
	}
	return nil
}

// splitTopLevel splits text at the commas outside parentheses.
func splitTopLevel(text string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range text {
		switch r {
		case '(', '<':
			depth++
		case ')', '>':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, text[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, text[start:])
}

// parseColumn reads one `name Type [NOT NULL]` entry.
func parseColumn(entry string) (Column, error) {
	name, rest, err := columnName(entry)
	if err != nil {
		return Column{}, err
	}
	words := strings.Fields(rest)
	upper := strings.ToUpper(strings.Join(words, " "))
	for _, refused := range []string{"DEFAULT", "PRIMARY KEY", "FAMILY"} {
		if strings.Contains(" "+upper+" ", " "+refused+" ") {
			return Column{}, &DeclarationError{Attribute: AttributeColumns,
				Reason: fmt.Sprintf("column %s carries %s, which an external table does not take", name, refused)}
		}
	}
	column := Column{Name: name}
	switch {
	case strings.HasSuffix(" "+upper, " NOT NULL"):
		column.NotNull = true
		words = words[:len(words)-2]
	case strings.HasSuffix(" "+upper, " NULL"):
		words = words[:len(words)-1]
	}
	column.Type = strings.Join(words, " ")
	return column, nil
}

// columnName reads the name at the start of entry, bare or backticked, and
// returns the rest.
func columnName(entry string) (string, string, error) {
	if inner, ok := strings.CutPrefix(entry, "`"); ok {
		end := strings.IndexByte(inner, '`')
		if end <= 0 {
			return "", "", &DeclarationError{Attribute: AttributeColumns,
				Reason: fmt.Sprintf("%q opens a quoted name it does not close", entry)}
		}
		return inner[:end], inner[end+1:], nil
	}
	name, rest, _ := strings.Cut(entry, " ")
	return name, rest, nil
}

// Refusal says why an external object cannot be written on a target: Key is
// the capability it needs and the target lacks, or empty when the declaration
// is wrong whatever the target, and Reason then says why.
type Refusal struct {
	// Subject names what is refused.
	Subject string
	// Key is the capability the target lacks.
	Key capability.Capability
	// Reason is why the declaration is refused, for a refusal without a key.
	Reason string
}

// CheckDataSource reports why the data source name, a canonical reference,
// cannot be created as source says on a target holding caps, or nil.
func CheckDataSource(name string, source DataSource, caps capability.Capabilities) *Refusal {
	subject := "external data source " + name
	switch {
	case !caps.Has(capability.ExternalDataSources):
		return &Refusal{Subject: subject, Key: capability.ExternalDataSources}
	case strings.TrimSpace(source.SourceType) == "":
		return &Refusal{Subject: subject, Reason: "it needs a source_type, such as ObjectStorage or PostgreSQL"}
	case strings.TrimSpace(source.AuthMethod) == "":
		return &Refusal{Subject: subject, Reason: "it needs an auth_method, such as NONE or BASIC (`AUTH_METHOD requires key`)"}
	}
	for _, option := range slices.Sorted(maps.Keys(source.Options)) {
		if strings.HasSuffix(option, secretPathSuffix) && !caps.Has(capability.ExternalDataSourceSecretPaths) {
			return &Refusal{Subject: subject + " option " + option, Key: capability.ExternalDataSourceSecretPaths}
		}
	}
	return nil
}

// CheckTable reports why the external table name, a canonical reference,
// cannot be created as table says on a target holding caps, or nil.
func CheckTable(name string, table Table, caps capability.Capabilities) *Refusal {
	subject := "external table " + name
	switch {
	case !caps.Has(capability.ExternalDataSources):
		return &Refusal{Subject: subject, Key: capability.ExternalDataSources}
	case strings.TrimSpace(table.DataSource) == "":
		return &Refusal{Subject: subject, Reason: "it needs a data_source, the path of the data source it reads"}
	case strings.TrimSpace(table.Location) == "":
		return &Refusal{Subject: subject, Reason: "it needs a location (`LOCATION requires key`)"}
	case len(table.Columns) == 0:
		return &Refusal{Subject: subject, Reason: "it needs a column"}
	}
	return nil
}

// CheckDrop reports why subject, a statement that drops an external object,
// cannot be written on a target holding caps, or nil.
func CheckDrop(subject string, caps capability.Capabilities) *Refusal {
	if !caps.Has(capability.ExternalDataSources) {
		return &Refusal{Subject: subject, Key: capability.ExternalDataSources}
	}
	return nil
}

// CheckReplace reports why subject, an external object a statement replaces
// with CREATE OR REPLACE, cannot be written on a target holding caps, or nil.
func CheckReplace(subject string, caps capability.Capabilities) *Refusal {
	if !caps.Has(capability.ExternalObjectReplace) {
		return &Refusal{Subject: subject + " replaced in place", Key: capability.ExternalObjectReplace}
	}
	return nil
}

// Path writes name, a canonical reference, as one quoted YDB path:
// `<directory>/<name>`, or the name alone at the database root.
func Path(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return sqlident.Quote(platform.YDB, name)
	}
	return sqlident.Qualified(platform.YDB, ref.Schema, ref.Name)
}

// CreateDataSourceStatement writes what creates the data source name, or
// replaces it when replace is set.
func CreateDataSourceStatement(name string, source DataSource, replace bool) string {
	settings := []string{optionSetting(optionSourceType, source.SourceType)}
	if source.Location != "" {
		settings = append(settings, optionSetting(optionLocation, source.Location))
	}
	settings = append(settings, optionSetting(optionAuthMethod, source.AuthMethod))
	settings = append(settings, sortedSettings(source.Options)...)
	return fmt.Sprintf("CREATE %sEXTERNAL DATA SOURCE %s WITH (\n    %s\n);", replacing(replace), Path(name),
		strings.Join(settings, ",\n    "))
}

// CreateTableStatement writes what creates the external table name, or
// replaces it when replace is set.
func CreateTableStatement(name string, table Table, replace bool) string {
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		definition := sqlident.Quote(platform.YDB, column.Name) + " " + column.Type
		if column.NotNull {
			definition += " NOT NULL"
		}
		columns = append(columns, definition)
	}
	settings := []string{optionSetting(optionDataSource, table.DataSource), optionSetting(optionLocation, table.Location)}
	settings = append(settings, sortedSettings(table.Options)...)
	return fmt.Sprintf("CREATE %sEXTERNAL TABLE %s (\n    %s\n) WITH (\n    %s\n);", replacing(replace), Path(name),
		strings.Join(columns, ",\n    "), strings.Join(settings, ",\n    "))
}

// DropDataSourceStatement writes what drops the data source name.
func DropDataSourceStatement(name string) string {
	return "DROP EXTERNAL DATA SOURCE " + Path(name) + ";"
}

// DropTableStatement writes what drops the external table name.
func DropTableStatement(name string) string {
	return "DROP EXTERNAL TABLE " + Path(name) + ";"
}

func replacing(replace bool) string {
	if replace {
		return "OR REPLACE "
	}
	return ""
}

func optionSetting(name, value string) string {
	return name + " = " + ydbtype.StringLiteral(value)
}

func sortedSettings(options map[string]string) []string {
	settings := make([]string, 0, len(options))
	for _, name := range slices.Sorted(maps.Keys(options)) {
		settings = append(settings, optionSetting(name, options[name]))
	}
	return settings
}

// RelativePath is a path an option or a description holds, relative to root,
// the directory a read describes: an absolute path under root loses root, and
// any other path is kept as it is.
func RelativePath(value, root string) string {
	root = "/" + strings.Trim(root, "/")
	if rest, under := strings.CutPrefix(value, root+"/"); under && strings.HasPrefix(value, "/") {
		return path.Clean(rest)
	}
	return value
}

// SameDataSource reports whether declared and current describe the same data
// source on a database whose root is root: the source type, location and auth
// method as written, and the options by upper-case name, with a secret's path
// compared relative to root, as the server stores it absolute.
func SameDataSource(declared, current DataSource, root string) bool {
	return declared.SourceType == current.SourceType &&
		declared.Location == current.Location &&
		declared.AuthMethod == current.AuthMethod &&
		equalOptions(canonicalSourceOptions(declared.Options, root), canonicalSourceOptions(current.Options, root))
}

// SameTable reports whether declared and current describe the same external
// table on a database whose root is root: the data source path, the location,
// the columns in order, and the options, with PARTITIONED_BY compared as the
// list of names it holds.
func SameTable(declared, current Table, root string) bool {
	return RelativePath(declared.DataSource, root) == RelativePath(current.DataSource, root) &&
		declared.Location == current.Location &&
		slices.EqualFunc(declared.Columns, current.Columns, sameColumn) &&
		equalOptions(canonicalTableOptions(declared.Options), canonicalTableOptions(current.Options))
}

// sameColumn compares a column's type without letter case or spaces: YQL
// reads `utf8` and `Decimal(22, 9)` as `Utf8` and `Decimal(22,9)`.
func sameColumn(a, b Column) bool {
	return a.Name == b.Name && a.NotNull == b.NotNull && canonicalType(a.Type) == canonicalType(b.Type)
}

func canonicalType(name string) string {
	return strings.ToUpper(strings.Join(strings.Fields(name), ""))
}

func canonicalSourceOptions(options map[string]string, root string) map[string]string {
	canonical := make(map[string]string, len(options))
	for name, value := range options {
		name = strings.ToUpper(name)
		if strings.HasSuffix(name, secretPathSuffix) {
			value = RelativePath(value, root)
		}
		canonical[name] = value
	}
	return canonical
}

func canonicalTableOptions(options map[string]string) map[string]string {
	canonical := make(map[string]string, len(options))
	for name, value := range options {
		name = strings.ToUpper(name)
		if name == optionPartitionedBy {
			value = compactList(value)
		}
		canonical[name] = value
	}
	return canonical
}

// compactList writes value, a JSON array of strings, without spaces, or
// returns it unchanged when it is not one.
func compactList(value string) string {
	var list []string
	if err := json.Unmarshal([]byte(value), &list); err != nil {
		return value
	}
	compact, err := json.Marshal(list)
	if err != nil {
		return value
	}
	return string(compact)
}

func equalOptions(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for name, value := range a {
		if other, ok := b[name]; !ok || other != value {
			return false
		}
	}
	return true
}

// DescribedTableOption is the value a declaration writes for name, given the
// value DescribeExternalTable returned for it: the one string of its JSON
// array, or for PARTITIONED_BY the array itself.
func DescribedTableOption(name, described string) (string, error) {
	if strings.ToUpper(name) == optionPartitionedBy {
		return compactList(described), nil
	}
	var list []string
	if err := json.Unmarshal([]byte(described), &list); err != nil || len(list) != 1 {
		return "", fmt.Errorf("external table option %s is %s, where the server writes a JSON array of one string",
			name, described)
	}
	return list[0], nil
}

// DescribedSourceOptions are the options DescribeExternalDataSource returned,
// without REFERENCES, which the server keeps, and with each secret path
// relative to root where it lies under it.
func DescribedSourceOptions(described map[string]string, root string) map[string]string {
	options := make(map[string]string, len(described))
	for name, value := range described {
		name = strings.ToUpper(name)
		switch {
		case name == optionReferences:
			continue
		case strings.HasSuffix(name, secretPathSuffix):
			value = RelativePath(value, root)
		}
		options[name] = value
	}
	if len(options) == 0 {
		return nil
	}
	return options
}

// DeprecatedSecretNames reads statement, one YQL statement, and when it
// creates an external data source, returns the source's path as written and
// every option of it that names a credential by a deprecated secret object:
// an option whose name ends in _SECRET_NAME. Measured on 25.1.4.7 and
// 26.2.1.14, the value of such an object sits in `.metadata/secrets/values`,
// and every value it held in `values_history`, readable in clear by the
// database administrator, where a YDB secret named by _SECRET_PATH is a scheme
// object whose value nothing returns.
func DeprecatedSecretNames(statement string) (string, []string) {
	tokens := significantTokens(statement)
	i := 0
	next := func(words ...string) bool {
		for _, word := range words {
			if i >= len(tokens) || !keyword(tokens[i], word) {
				return false
			}
			i++
		}
		return true
	}
	if !next("CREATE") {
		return "", nil
	}
	next("OR", "REPLACE")
	if !next("EXTERNAL", "DATA", "SOURCE") {
		return "", nil
	}
	next("IF", "NOT", "EXISTS")
	if i >= len(tokens) {
		return "", nil
	}
	path := identifierText(tokens[i])
	var options []string
	depth := 0
	for j := i + 1; j < len(tokens); j++ {
		token := tokens[j]
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case depth == 1 && token.Type == lexer.TokenIdentifier && j+1 < len(tokens) &&
			tokens[j+1].MatchOperatorValue("=") && strings.HasSuffix(strings.ToUpper(token.Value), "_SECRET_NAME"):
			options = append(options, strings.ToUpper(token.Value))
		}
	}
	return path, options
}

// significantTokens reads statement as YQL, leaving out whitespace and
// comments.
func significantTokens(statement string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		tokens = append(tokens, token)
	}
}

func keyword(token lexer.Token, word string) bool {
	return token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, word)
}

// identifierText is the name a token writes, without backticks.
func identifierText(token lexer.Token) string {
	if name, ok := lexer.YQLIdentifierValue(token.Value); ok {
		return name
	}
	return token.Value
}
