package embedpg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedstore"
	"go.5x5.cz/ptah/internal/embedverify"
)

// ReadStructure reads back the schema a generation's verification is measured
// against.
//
// Read from the catalog rather than from what Ptah rendered. The two agree
// right up until they do not, and the case that matters -- an index that exists
// and is not VALID -- is invisible from the rendering side.
func ReadStructure(
	ctx context.Context, db *sql.DB, spec embedgen.Spec, activePointer string,
) (embedverify.Structure, error) {
	structure := embedverify.Structure{ActivePointer: activePointer}
	extension, err := extensionInstalled(ctx, db, "vector")
	if err != nil {
		return embedverify.Structure{}, err
	}
	structure.ExtensionPresent = extension

	const columnQuery = `SELECT format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname = $1 AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped`
	var columnType string
	err = db.QueryRowContext(ctx, columnQuery, spec.Target.Table, spec.Target.Column).Scan(&columnType)
	if err == nil {
		structure.ColumnExists = true
		structure.ColumnType = columnType
		structure.Dimension = dimensionOf(columnType)
	} else if err != sql.ErrNoRows {
		return embedverify.Structure{}, fmt.Errorf("read %s.%s: %w",
			spec.Target.Table, spec.Target.Column, err)
	}

	return readIndexInto(ctx, db, spec, structure)
}

// readIndexInto adds what the catalog says about the generation's index.
func readIndexInto(
	ctx context.Context, db *sql.DB, spec embedgen.Spec, structure embedverify.Structure,
) (embedverify.Structure, error) {
	objects, err := spec.TargetObjects()
	if err != nil {
		return structure, err
	}
	if !objects.HasIndex {
		return structure, nil
	}
	// indisvalid is the column this whole function exists to reach: an index
	// that exists and is not valid is one PostgreSQL will not use, and a
	// generation whose index is invalid answers queries by sequential scan --
	// correctly, and far too slowly to cut over to.
	const query = `SELECT am.amname, i.indisvalid,
			COALESCE((SELECT opc.opcname FROM pg_opclass opc WHERE opc.oid = i.indclass[0]), '')
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_am am ON am.oid = ic.relam
		WHERE ic.relname = $1`
	var method, operatorClass string
	var valid bool
	err = db.QueryRowContext(ctx, query, objects.Index.Name).Scan(&method, &valid, &operatorClass)
	if err == sql.ErrNoRows {
		return structure, nil
	}
	if err != nil {
		return structure, fmt.Errorf("read index %s: %w", objects.Index.Name, err)
	}
	structure.IndexExists = true
	structure.IndexMethod = method
	structure.IndexValid = valid
	structure.OperatorClass = operatorClass
	return structure, nil
}

// dimensionOf reads the declared dimension out of a rendered vector type.
//
// Zero when the type carries none, which is a real answer: `vector` without a
// dimension is a column pgvector accepts and refuses to index.
func dimensionOf(columnType string) int {
	open := strings.IndexByte(columnType, '(')
	closing := strings.IndexByte(columnType, ')')
	if open < 0 || closing < open {
		return 0
	}
	var dimension int
	if _, err := fmt.Sscanf(columnType[open+1:closing], "%d", &dimension); err != nil {
		return 0
	}
	return dimension
}

// RollbackState reads what is true about a previous generation now.
//
// Every field is measured. The epic's rule is that rollback must not be
// reported as available merely because the old tables still exist, and the only
// way to honour that is for none of this to come from a status column somebody
// set.
func RollbackState(
	ctx context.Context, db *sql.DB, spec embedgen.Spec, generation string, pointer embedstore.Pointer,
) (embedcutover.RollbackState, error) {
	store := NewStore(db)
	registered, err := store.Generation(ctx, generation)
	if err != nil {
		return embedcutover.RollbackState{}, err
	}

	previous := spec
	previous.Target.Column = registered.TargetColumn
	structure, err := ReadStructure(ctx, db, previous, pointer.Active)
	if err != nil {
		return embedcutover.RollbackState{}, err
	}

	stale, missing, err := generationFreshness(ctx, db, previous, registered)
	if err != nil {
		return embedcutover.RollbackState{}, err
	}
	return embedcutover.RollbackState{
		Present:    structure.ColumnExists,
		IndexReady: structure.IndexExists && structure.IndexValid,
		Retired:    registered.Retired(),
		CutOverAt:  pointer.CutOverAt,
		// Both of these are recorded rather than inferred. A generation nobody
		// verified has a zero VerifiedAt and is reported as unmeasured; one
		// nobody is feeding is not maintained, and from the moment the feeding
		// stops it drifts from the source with every write.
		VerifiedAt:  registered.VerifiedAt,
		Maintained:  registered.Maintained(time.Now().UTC()),
		StaleRows:   stale,
		MissingRows: missing,
	}, nil
}

// generationFreshness counts what is wrong with a generation right now.
//
// Measured against the source rather than read from a status column, which is
// the epic's rule: rollback must not be reported as available merely because
// the old tables still exist. A generation last verified an hour ago and
// unmaintained since has a count these two queries can answer and a
// verification timestamp that cannot.
func generationFreshness(
	ctx context.Context, db *sql.DB, spec embedgen.Spec, registered embedstore.Generation,
) (stale, missing int, err error) {
	sources, targets, err := ReadVerificationRows(ctx, db, spec)
	if err != nil {
		return 0, 0, err
	}
	byKey := make(map[string]embedverify.TargetRow, len(targets))
	for _, row := range targets {
		byKey[row.Key] = row
	}
	for _, source := range sources {
		if source.Skipped {
			continue
		}
		target, found := byKey[source.Key]
		switch {
		case !found, target.Generation != registered.Identity, target.Tombstone:
			missing++
		case target.InputHash != source.InputHash,
			source.Version != "" && target.Version != source.Version:
			stale++
		}
	}
	return stale, missing, nil
}

// ReadVerificationRows reads both sides of the comparison out of the source
// table.
//
// One walk, because the two sides have to agree about which key a row is. Two
// queries would let a row inserted between them appear on one side and not the
// other, and the report would name it as a coverage gap that nothing created.
func ReadVerificationRows(
	ctx context.Context, db *sql.DB, spec embedgen.Spec,
) ([]embedverify.SourceRow, []embedverify.TargetRow, error) {
	source, err := NewSource(db, spec)
	if err != nil {
		return nil, nil, err
	}
	query, err := verificationQuery(spec, source)
	if err != nil {
		return nil, nil, err
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s for verification: %w", spec.Source.Table, err)
	}
	defer rows.Close()

	sourceRows, targetRows, err := scanVerificationRows(rows, spec)
	if err != nil {
		return nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read %s for verification: %w", spec.Source.Table, err)
	}
	return sourceRows, targetRows, nil
}

// verificationQuery renders the one walk over both sides.
func verificationQuery(spec embedgen.Spec, source *Source) (string, error) {
	keys := quoteAll(spec.Source.KeyFields)
	columns := append([]string(nil), castToText(keys)...)
	columns = append(columns, quoteAll(spec.Source.InputFields)...)
	columns = append(columns, quoteAll(source.versionColumns())...)
	column := spec.Target.Column
	columns = append(columns,
		quoteIdentifier(column+GenerationSuffix),
		quoteIdentifier(column+InputHashSuffix),
		quoteIdentifier(column+VersionSuffix),
		quoteIdentifier(column+StateSuffix),
		"vector_dims("+quoteIdentifier(column)+")")

	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation or
	// column name; the identifiers come from the specification and go through
	// quoteIdentifier.
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), source.qualifiedTable())
	if filter := strings.TrimSpace(spec.Source.Filter); filter != "" {
		query += " WHERE (" + filter + ")"
	}
	return query + " ORDER BY " + strings.Join(keys, ", "), nil
}

// scanVerificationRows turns the result set into the two sides.
func scanVerificationRows(
	rows *sql.Rows, spec embedgen.Spec,
) ([]embedverify.SourceRow, []embedverify.TargetRow, error) {
	keyCount := len(spec.Source.KeyFields)
	inputCount := len(spec.Source.InputFields)
	versionCount := len(versionColumnsOf(spec))

	var sources []embedverify.SourceRow
	var targets []embedverify.TargetRow
	for rows.Next() {
		values := make([]sql.NullString, keyCount+inputCount+versionCount+4)
		var dimension sql.NullInt64
		targetsScan := make([]any, 0, len(values)+1)
		for index := range values {
			targetsScan = append(targetsScan, &values[index])
		}
		targetsScan = append(targetsScan, &dimension)
		if err := rows.Scan(targetsScan...); err != nil {
			return nil, nil, fmt.Errorf("read a verification row: %w", err)
		}
		sourceRow, targetRow, err := splitVerificationRow(values, dimension, spec, keyCount, inputCount)
		if err != nil {
			return nil, nil, err
		}
		sources = append(sources, sourceRow)
		targets = append(targets, targetRow)
	}
	return sources, targets, nil
}

// splitVerificationRow turns one scanned row into what each side says about it.
func splitVerificationRow(
	values []sql.NullString, dimension sql.NullInt64, spec embedgen.Spec, keyCount, inputCount int,
) (embedverify.SourceRow, embedverify.TargetRow, error) {
	key := make([]string, keyCount)
	for index := range keyCount {
		key[index] = values[index].String
	}
	fields := make([]*string, inputCount)
	for index := range inputCount {
		if value := values[keyCount+index]; value.Valid {
			fields[index] = &value.String
		}
	}
	offset := keyCount + inputCount
	version := ""
	if len(versionColumnsOf(spec)) > 0 {
		version = values[offset].String
		offset++
	}

	input, err := spec.Canonicalize(embedgen.Row{Key: key, Fields: fields})
	if err != nil {
		return embedverify.SourceRow{}, embedverify.TargetRow{},
			fmt.Errorf("canonicalize %v for verification: %w", key, err)
	}
	identity := strings.Join(key, "\x1f")
	state := values[offset+3].String
	return embedverify.SourceRow{
			Key: identity, Version: version,
			InputHash: spec.SourceInputHash(input), Skipped: input.Skipped,
		},
		embedverify.TargetRow{
			Key: identity, Generation: values[offset].String,
			InputHash: values[offset+1].String, Version: values[offset+2].String,
			Tombstone: state == "tombstone", Skipped: state == "skip",
			Vector: vectorPlaceholder(dimension),
		}, nil
}

// vectorPlaceholder stands in for the stored vector's shape.
//
// The verification layer that reads it asks about length and finiteness.
// `vector_dims` is the server's own answer to the first, and a NaN cannot be
// stored in a pgvector column at all -- it is refused on write, which is why
// reading every vector back to check would be measuring the write path twice.
func vectorPlaceholder(dimension sql.NullInt64) []float32 {
	if !dimension.Valid || dimension.Int64 <= 0 {
		return nil
	}
	return make([]float32, dimension.Int64)
}

// versionColumnsOf names the source version column, and is empty under a
// strategy that establishes none.
//
// A list of nought or one rather than a boolean, because what the caller needs
// is how many columns the query returns and a flag is a second answer to that.
func versionColumnsOf(spec embedgen.Spec) []string {
	if field := strings.TrimSpace(spec.Source.VersionField); field != "" {
		return []string{field}
	}
	return nil
}

// CountGenerationRows counts the vectors a generation holds.
func CountGenerationRows(
	ctx context.Context, db *sql.DB, generation embedstore.Generation,
) (int, error) {
	// #nosec G201 -- relation and column names from the registry, through
	// quoteIdentifier.
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $1",
		quoteIdentifier(generation.TargetTable),
		quoteIdentifier(generation.TargetColumn+GenerationSuffix))
	var count int
	if err := db.QueryRowContext(ctx, query, generation.Identity).Scan(&count); err != nil {
		return 0, fmt.Errorf("count the rows of generation %s: %w", generation.Identity, err)
	}
	return count, nil
}

// RetireIndex drops a generation's index and leaves its vectors.
//
// The index goes first and the columns second, because dropping a column takes
// its index with it and a failure between the two would otherwise leave the
// index gone and the column there -- a generation that is neither retired nor
// usable.
func RetireIndex(
	ctx context.Context, db *sql.DB, spec embedgen.Spec, generation embedstore.Generation,
) error {
	retired := spec
	retired.Target.Column = generation.TargetColumn
	objects, err := retired.TargetObjects()
	if err != nil {
		return err
	}
	if !objects.HasIndex {
		return nil
	}
	// #nosec G201 -- a generated index name, through quoteIdentifier.
	drop := fmt.Sprintf("DROP INDEX IF EXISTS %s", quoteIdentifier(objects.Index.Name))
	if _, err := db.ExecContext(ctx, drop); err != nil {
		return fmt.Errorf("drop index %s: %w", objects.Index.Name, err)
	}
	return nil
}

// RetireColumns drops a generation's vector column and its metadata.
func RetireColumns(
	ctx context.Context, db *sql.DB, generation embedstore.Generation,
) error {
	for _, suffix := range []string{
		"", GenerationSuffix, InputHashSuffix, VersionSuffix, StateSuffix,
	} {
		// #nosec G201 -- relation and column names from the registry, through
		// quoteIdentifier.
		drop := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s",
			quoteIdentifier(generation.TargetTable),
			quoteIdentifier(generation.TargetColumn+suffix))
		if _, err := db.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("drop column %s%s: %w", generation.TargetColumn, suffix, err)
		}
	}
	return nil
}
