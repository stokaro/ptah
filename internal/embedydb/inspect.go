package embedydb

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
	"ptah.run/internal/ydbindex"
)

// TargetColumnExists reports whether the generation's vector column is there.
func (b *Backend) TargetColumnExists(ctx context.Context, spec embedgen.Spec) (bool, error) {
	described, exists, err := b.describe(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil || !exists {
		return false, err
	}
	_, found := described.Column(spec.Target.Column)
	return found, nil
}

// ReadStructure reads back the column and index a generation's verification
// is measured against.
//
// Two answers differ from PostgreSQL's in kind. A YDB String column declares
// no width, so the dimension is the index's vector_dimension when the index is
// there and the generation's own otherwise; each stored vector's width is
// measured row by row in the corpus walk either way. And the vector functions
// are part of YDB rather than an extension, so the extension is reported
// present.
func (b *Backend) ReadStructure(
	ctx context.Context, spec embedgen.Spec, activePointer string,
) (embedverify.Structure, error) {
	structure := embedverify.Structure{ActivePointer: activePointer, ExtensionPresent: true}
	described, exists, err := b.describe(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil || !exists {
		return structure, err
	}
	column, found := described.Column(spec.Target.Column)
	if !found {
		return structure, nil
	}
	structure.ColumnExists = true
	structure.ColumnType = column.Type
	structure.Dimension = spec.Model.ReportedDimension
	index, found := described.Index(embedgen.IndexName(spec.Target.Table, spec.Target.Column, spec.Identity().Digest))
	if !found {
		return structure, nil
	}
	structure.IndexExists = true
	structure.IndexValid = index.Ready
	structure.IndexMethod = "GLOBAL"
	if index.Vector != nil {
		structure.IndexMethod = ydbindex.VectorMethod
		structure.OperatorClass = MetricSetting(*index.Vector)
		structure.Dimension = int(index.Vector.Dimension) // #nosec G115 -- YDB caps it at 16384
	}
	return structure, nil
}

// CountGenerationRows counts the vectors a registered generation holds.
func (b *Backend) CountGenerationRows(ctx context.Context, generation embedstore.Generation) (int, error) {
	// #nosec G201 -- names from the registry, through quote.
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $p1",
		quoteTable(generation.TargetSchema, generation.TargetTable),
		quote(generation.TargetColumn+generationSuffix))
	var count uint64
	if err := b.db.QueryRowContext(ctx, query, generation.Identity).Scan(&count); err != nil {
		return 0, fmt.Errorf("count the rows of generation %s: %w", generation.Identity, err)
	}
	return int(count), nil // #nosec G115 -- a row count
}

// GenerationIndexExists reports whether a registered generation's index is
// there. The name comes from the registry row, never from a specification,
// because a retirement holds no specification for the generation it destroys.
func (b *Backend) GenerationIndexExists(ctx context.Context, generation embedstore.Generation) (bool, error) {
	described, exists, err := b.describe(ctx, generation.TargetSchema, generation.TargetTable)
	if err != nil || !exists {
		return false, err
	}
	_, found := described.Index(embedgen.IndexName(generation.TargetTable, generation.TargetColumn, generation.Identity))
	return found, nil
}

// Measure answers what a plan reports about the database.
//
// The vector type is YDB's own, so it is always there. The vector index is
// the vector_indexes capability of the connected line, which YDB 25.1 has
// only with its feature flag on. The index this specification declares is
// buildable when its settings are ones YDB takes and the line has the
// capability. A consistency mode the line cannot provide is a refusal.
func (b *Backend) Measure(ctx context.Context, spec embedgen.Spec) (embedbackend.Measurement, error) {
	measured := embedbackend.Measurement{
		Rows: -1,
		Capabilities: map[string]bool{
			"vector_type":  true,
			"vector_index": b.caps.Has(capability.VectorIndexes),
		},
	}
	if _, err := TargetObjects(spec); err != nil {
		return embedbackend.Measurement{}, err
	}
	source, sourceExists, err := b.describe(ctx, spec.Source.Schema, spec.Source.Table)
	if err != nil {
		return embedbackend.Measurement{}, err
	}
	measured.SourceExists = sourceExists
	target, targetExists, err := b.describe(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil {
		return embedbackend.Measurement{}, err
	}
	// A table of the generation's own does not exist before prepare creates
	// it, and that is not a plan that cannot run.
	measured.TargetTableExists = targetExists || spec.Target.Layout.OwnsTable()
	if targetExists {
		_, measured.TargetColumnExists = target.Column(spec.Target.Column)
	}
	if sourceExists {
		if _, err := keyOf(source, spec.Source.KeyFields, tablePath(spec.Source.Schema, spec.Source.Table)); err != nil {
			measured.Refusals = append(measured.Refusals, err.Error())
		}
		if measured.Rows, err = b.countSource(ctx, spec); err != nil {
			return embedbackend.Measurement{}, err
		}
	}
	measured.IndexBuildable = true
	if embedbackend.HasIndex(spec) {
		_, settingsErr := VectorSettings(spec)
		measured.IndexBuildable = settingsErr == nil && b.caps.Has(capability.VectorIndexes)
	}
	return measured, nil
}

// countSource counts the source rows in scope.
func (b *Backend) countSource(ctx context.Context, spec embedgen.Spec) (int64, error) {
	// #nosec G201 -- the path through quoteTable; the filter is the
	// specification's own YQL, which is what the field is for.
	query := "SELECT COUNT(*) FROM " + quoteTable(spec.Source.Schema, spec.Source.Table)
	if filter := strings.TrimSpace(spec.Source.Filter); filter != "" {
		query += " WHERE (" + filter + ")"
	}
	var count uint64
	if err := b.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return -1, fmt.Errorf("count %s: %w", spec.Source.Table, err)
	}
	return int64(count), nil // #nosec G115 -- a row count
}

// VerificationCorpus walks both sides of the comparison in one read.
//
// One read, so the two sides agree about which key a row is: two reads would
// let a row written between them appear on one side only. The walk is handed
// back as a sequence that owns the result set and closes it when the range
// loop over it ends, so nothing between here and the report holds the corpus.
//
// Equal keys arrive adjacent, which is what [embedverify.Corpus] requires:
// the read is ordered by the key, and by the chunk ordinal where the layout
// holds a set. The width of a stored vector is computed by the server from the
// length of its bytes, so the vectors themselves are not read back; the writer
// refuses a non-finite element, which is what keeps one out.
func (b *Backend) VerificationCorpus(ctx context.Context, spec embedgen.Spec) (embedverify.Corpus, error) {
	if err := validateSource(spec); err != nil {
		return nil, err
	}
	query := verificationQuery(spec)
	//nolint:rowserrcheck // Rows.Err is checked in the iterator returned below.
	rows, err := b.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read %s for verification: %w", verificationSubject(spec), err)
	}
	scanner := newVerificationScanner(spec)
	return func(yield func(embedverify.Pair, error) bool) {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			pair, err := scanner.pair(rows)
			if err != nil {
				yield(embedverify.Pair{}, err)
				return
			}
			if !yield(pair, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(embedverify.Pair{}, fmt.Errorf("read %s for verification: %w", verificationSubject(spec), err))
		}
	}, nil
}

// oneRelation reports whether the specification's two sides name one table.
func oneRelation(spec embedgen.Spec) bool {
	return tablePath(spec.Source.Schema, spec.Source.Table) == tablePath(spec.Target.Schema, spec.Target.Table)
}

// verificationSubject names what the walk reads, for a diagnostic.
func verificationSubject(spec embedgen.Spec) string {
	source := tablePath(spec.Source.Schema, spec.Source.Table)
	if oneRelation(spec) {
		return source
	}
	return source + " and " + tablePath(spec.Target.Schema, spec.Target.Table)
}

// storedWidth is the element count of the float vector in column: four bytes
// an element and the tag. NULL stays NULL, which says the row holds no vector.
func storedWidth(alias, column string) string {
	return "CAST((Length(" + qualified(alias, column) + ") - 1) / 4 AS Int64)"
}

// verificationQuery renders the walk: one table read once when both sides are
// one table, and a full join of the two otherwise, so a stored row whose key
// the source no longer has is seen.
func verificationQuery(spec embedgen.Spec) string {
	if oneRelation(spec) {
		return oneRelationVerificationQuery(spec)
	}
	return joinedVerificationQuery(spec)
}

// oneRelationVerificationQuery reads both sides off one table. The filter
// decides whether a row is in scope, and the walk reads every row the filter
// asks for and every row a generation wrote, so a vector stored for a row the
// filter excludes is reported rather than missed.
func oneRelationVerificationQuery(spec embedgen.Spec) string {
	columns := make([]string, 0, len(spec.Source.KeyFields)+len(spec.Source.InputFields)+9)
	for _, field := range spec.Source.KeyFields {
		columns = append(columns, asText("", field))
	}
	for _, field := range spec.Source.InputFields {
		columns = append(columns, asText("", field))
	}
	for _, field := range versionColumns(spec) {
		columns = append(columns, asText("", field))
	}
	column := spec.Target.Column
	for _, suffix := range metadataSuffixes() {
		columns = append(columns, quote(column+suffix))
	}
	inScope := "true"
	filter := strings.TrimSpace(spec.Source.Filter)
	if filter != "" {
		inScope = "(" + filter + ")"
	}
	columns = append(columns, storedWidth("", column), "0", inScope, "true")
	order := make([]string, len(spec.Source.KeyFields))
	for index, field := range spec.Source.KeyFields {
		order[index] = quote(field)
	}
	// #nosec G201 -- names from the specification, through quote; the filter
	// is the specification's own YQL.
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "),
		quoteTable(spec.Source.Schema, spec.Source.Table))
	if filter != "" {
		query += " WHERE (" + filter + ") OR " + quote(column+generationSuffix) + " IS NOT NULL"
	}
	return query + " ORDER BY " + strings.Join(order, ", ")
}

// The names the joined walk gives what each side selects, so neither side
// publishes a name the specification chose and could collide with.
const (
	scopeAlias          = "ptah_in_scope"
	presenceAlias       = "ptah_target_present"
	generationAlias     = "ptah_generation"
	inputHashAlias      = "ptah_input_hash"
	targetVersionAlias  = "ptah_target_version"
	stateAlias          = "ptah_state"
	storedWidthAlias    = "ptah_stored_width"
	storedOrdinalAlias  = "ptah_stored_ordinal"
	keyAliasPrefix      = "ptah_key_"
	inputFieldPrefix    = "ptah_input_"
	sourceVersionPrefix = "ptah_source_version_"
)

// aliasAt names one member of a list the join carries positionally.
func aliasAt(prefix string, index int) string {
	return prefix + strconv.Itoa(index)
}

// joinedVerificationQuery reads the two tables in one full join on the key as
// text, which is how the write path addresses a row. A position only the
// target has is a stored row the source no longer asks for; one only the
// source has is a row with nothing stored yet.
func joinedVerificationQuery(spec embedgen.Spec) string {
	keyCount := len(spec.Source.KeyFields)
	columns := make([]string, 0, keyCount+len(spec.Source.InputFields)+9)
	on := make([]string, 0, keyCount)
	order := make([]string, 0, keyCount+1)
	for index := range keyCount {
		alias := aliasAt(keyAliasPrefix, index)
		columns = append(columns, fmt.Sprintf("COALESCE(s.%s, t.%s) AS %s", alias, alias, alias))
		on = append(on, fmt.Sprintf("s.%s = t.%s", alias, alias))
		order = append(order, alias)
	}
	for index := range spec.Source.InputFields {
		columns = append(columns, "s."+aliasAt(inputFieldPrefix, index))
	}
	for index := range versionColumns(spec) {
		columns = append(columns, "s."+aliasAt(sourceVersionPrefix, index))
	}
	columns = append(columns,
		"t."+generationAlias, "t."+inputHashAlias, "t."+targetVersionAlias, "t."+stateAlias,
		"t."+storedWidthAlias,
		fmt.Sprintf("COALESCE(t.%s, 0) AS %s", storedOrdinalAlias, storedOrdinalAlias),
		fmt.Sprintf("COALESCE(s.%s, false) AS %s", scopeAlias, scopeAlias),
		fmt.Sprintf("COALESCE(t.%s, false) AS %s", presenceAlias, presenceAlias))
	if spec.Target.Layout.OwnsTable() {
		order = append(order, storedOrdinalAlias)
	}
	// #nosec G201 -- names from the specification through quote, and this
	// package's own aliases.
	return fmt.Sprintf("SELECT %s FROM %s AS s FULL JOIN %s AS t ON %s WHERE COALESCE(s.%s, false) OR "+
		"t.%s IS NOT NULL ORDER BY %s",
		strings.Join(columns, ", "), verificationSourceSide(spec), verificationTargetSide(spec),
		strings.Join(on, " AND "), scopeAlias, generationAlias, strings.Join(order, ", "))
}

// verificationSourceSide is the source half of the join: the key as text, the
// input fields, the version, and whether the filter asks for the row.
func verificationSourceSide(spec embedgen.Spec) string {
	selected := make([]string, 0, len(spec.Source.KeyFields)+len(spec.Source.InputFields)+2)
	for index, field := range spec.Source.KeyFields {
		selected = append(selected, asText("", field)+" AS "+aliasAt(keyAliasPrefix, index))
	}
	for index, field := range spec.Source.InputFields {
		selected = append(selected, asText("", field)+" AS "+aliasAt(inputFieldPrefix, index))
	}
	for index, field := range versionColumns(spec) {
		selected = append(selected, asText("", field)+" AS "+aliasAt(sourceVersionPrefix, index))
	}
	scope := "true"
	if filter := strings.TrimSpace(spec.Source.Filter); filter != "" {
		scope = "(" + filter + ")"
	}
	selected = append(selected, scope+" AS "+scopeAlias)
	return fmt.Sprintf("(SELECT %s FROM %s)", strings.Join(selected, ", "),
		quoteTable(spec.Source.Schema, spec.Source.Table))
}

// verificationTargetSide is the target half: the key as text, the four state
// columns, the width of the vector at the key, its chunk ordinal, and a
// literal saying a row stands there at all.
func verificationTargetSide(spec embedgen.Spec) string {
	column := spec.Target.Column
	selected := make([]string, 0, len(spec.Source.KeyFields)+7)
	for index, field := range spec.Source.KeyFields {
		selected = append(selected, asText("", field)+" AS "+aliasAt(keyAliasPrefix, index))
	}
	ordinal := "0"
	if spec.Target.Layout.OwnsTable() {
		ordinal = quote(column + ordinalSuffix)
	}
	selected = append(selected,
		quote(column+generationSuffix)+" AS "+generationAlias,
		quote(column+inputHashSuffix)+" AS "+inputHashAlias,
		quote(column+versionSuffix)+" AS "+targetVersionAlias,
		quote(column+stateSuffix)+" AS "+stateAlias,
		storedWidth("", column)+" AS "+storedWidthAlias,
		ordinal+" AS "+storedOrdinalAlias,
		"true AS "+presenceAlias)
	return fmt.Sprintf("(SELECT %s FROM %s)", strings.Join(selected, ", "),
		quoteTable(spec.Target.Schema, spec.Target.Table))
}

// verificationScanner reads result rows into storage it reuses, so what a
// row costs does not depend on how many there are.
type verificationScanner struct {
	spec       embedgen.Spec
	keyCount   int
	inputCount int

	values    []sql.NullString
	dimension sql.NullInt64
	ordinal   sql.NullInt64
	inScope   sql.NullBool
	present   sql.NullBool
	into      []any

	source embedverify.SourceRow
	target embedverify.TargetRow
}

// newVerificationScanner sizes the buffers one result row needs.
func newVerificationScanner(spec embedgen.Spec) *verificationScanner {
	scanner := &verificationScanner{
		spec:       spec,
		keyCount:   len(spec.Source.KeyFields),
		inputCount: len(spec.Source.InputFields),
	}
	width := scanner.keyCount + scanner.inputCount + len(versionColumns(spec)) + len(metadataSuffixes())
	scanner.values = make([]sql.NullString, width)
	scanner.into = make([]any, 0, width+4)
	for index := range scanner.values {
		scanner.into = append(scanner.into, &scanner.values[index])
	}
	scanner.into = append(scanner.into, &scanner.dimension, &scanner.ordinal, &scanner.inScope, &scanner.present)
	return scanner
}

// pair reads the current row into the scanner's own rows and points at them.
func (s *verificationScanner) pair(rows *sql.Rows) (embedverify.Pair, error) {
	if err := rows.Scan(s.into...); err != nil {
		return embedverify.Pair{}, fmt.Errorf("read a verification row: %w", err)
	}
	inScope := s.inScope.Valid && s.inScope.Bool
	present := s.present.Valid && s.present.Bool
	key := make([]string, s.keyCount)
	for index := range s.keyCount {
		key[index] = s.values[index].String
	}
	offset := s.keyCount + s.inputCount + len(versionColumns(s.spec))
	state := s.values[offset+3].String
	s.target = embedverify.TargetRow{
		Key:        embedverify.KeyIdentity(key...),
		Generation: s.values[offset].String,
		InputHash:  s.values[offset+1].String,
		Version:    s.values[offset+2].String,
		Tombstone:  state == "tombstone",
		Skipped:    state == "skip",
		Ordinal:    int(s.ordinal.Int64),
	}
	if s.dimension.Valid && s.dimension.Int64 > 0 {
		s.target.Dimension = int(s.dimension.Int64)
	}
	pair := embedverify.Pair{}
	if present {
		pair.Target = &s.target
	}
	if !inScope {
		return pair, nil
	}
	fields := make([]*string, s.inputCount)
	for index := range s.inputCount {
		if value := s.values[s.keyCount+index]; value.Valid {
			fields[index] = &value.String
		}
	}
	version := ""
	if len(versionColumns(s.spec)) > 0 {
		version = s.values[s.keyCount+s.inputCount].String
	}
	set, err := s.spec.CanonicalInputs(embedgen.Row{Key: key, Fields: fields})
	if err != nil {
		return embedverify.Pair{}, fmt.Errorf("canonicalize %v for verification: %w", key, err)
	}
	s.source = embedverify.SourceRow{
		Key:       embedverify.KeyIdentity(key...),
		Version:   version,
		InputHash: s.spec.SourceInputHash(set.Whole),
		Skipped:   set.Whole.Skipped,
	}
	pair.Source = &s.source
	return pair, nil
}
