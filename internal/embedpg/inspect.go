package embedpg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedspec"
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

	// to_regclass resolves the name the way every other query in this session
	// would: a qualified name names its schema, and an unqualified one takes
	// search_path, which is what a specification naming no schema asked for. It
	// answers NULL for a relation that is not there, so the row simply does not
	// match and the caller's ErrNoRows branch runs.
	//
	// Matching pg_class.relname alone is stokaro/ptah#2629: a same-named table
	// in ANOTHER schema answered for this generation, so verification measured
	// a structure belonging to somebody else's table.
	const columnQuery = `SELECT format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = to_regclass($1) AND a.attname = $2
			AND a.attnum > 0 AND NOT a.attisdropped`
	var columnType string
	target := qualify(spec.Target.Schema, spec.Target.Table)
	err = db.QueryRowContext(ctx, columnQuery, target, spec.Target.Column).Scan(&columnType)
	if err == nil {
		structure.ColumnExists = true
		structure.ColumnType = columnType
		structure.Dimension = dimensionOf(columnType)
	} else if err != sql.ErrNoRows {
		return embedverify.Structure{}, fmt.Errorf("read %s.%s: %w",
			target, spec.Target.Column, err)
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
	// Resolved through to_regclass for the same reason the column read is: an
	// index lives in its table's schema, and matching relname alone let another
	// schema's like-named index answer for this generation's.
	const query = `SELECT am.amname, i.indisvalid,
			COALESCE((SELECT opc.opcname FROM pg_opclass opc WHERE opc.oid = i.indclass[0]), '')
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_am am ON am.oid = ic.relam
		WHERE i.indexrelid = to_regclass($1)`
	var method, operatorClass string
	var valid bool
	err = db.QueryRowContext(ctx, query,
		qualify(spec.Target.Schema, objects.Index.Name)).Scan(&method, &valid, &operatorClass)
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
//
// It takes no specification, deliberately. The question is about ONE generation
// -- the one the pointer names as its way back -- and the only specification
// that can answer it is that generation's own, which the registry records.
// Taking the caller's is what produced stokaro/ptah#2630: the parameter looked
// like context and was in fact the measurement's identity, so the answer
// changed with whichever file the operator happened to pass.
func RollbackState(
	ctx context.Context, db *sql.DB, generation string, pointer embedstore.Pointer,
) (embedcutover.RollbackState, error) {
	store := NewStore(db)
	registered, err := store.Generation(ctx, generation)
	if err != nil {
		return embedcutover.RollbackState{}, err
	}

	previous, err := recordedSpec(registered)
	if err != nil {
		return embedcutover.RollbackState{}, err
	}
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

// recordedSpec is the specification the generation was actually built from.
//
// Not the caller's specification with the old column swapped in. That hybrid
// belongs to no generation -- measured, its identity digest matched neither the
// retired generation's nor the current one's -- and every expected input hash
// computed under it mismatched, so the documented rollback was refused with
// "N rows are stale" while `verify` on the same generation passed at the same
// instant (stokaro/ptah#2630).
//
// A generation with no recorded document is refused rather than measured
// against a substitute. There is no substitute: the answer this feeds is
// whether a way back is safe to take, and a wrong yes destroys a corpus.
func recordedSpec(registered embedstore.Generation) (embedgen.Spec, error) {
	if strings.TrimSpace(registered.SpecDocument) == "" {
		return embedgen.Spec{}, fmt.Errorf(
			"generation %s records no specification, so nothing can measure whether it is "+
				"still fresh enough to roll back to", registered.Identity)
	}
	loaded, err := embedspec.ParsePublished(
		[]byte(registered.SpecDocument), "the recorded specification", registered.SpecDigest)
	if err != nil {
		return embedgen.Spec{}, err
	}
	return loaded.Spec, nil
}

// generationFreshness counts what is wrong with a generation right now.
//
// Measured against the source rather than read from a status column, which is
// the epic's rule: rollback must not be reported as available merely because
// the old tables still exist. A generation last verified an hour ago and
// unmaintained since has counts this can answer and a verification timestamp
// that cannot.
//
// It runs the verification layers rather than walking the rows itself. Coverage
// and freshness are exactly what those layers decide, and a second walk here
// would be a second answer to them -- one that agreed on the fixtures it was
// written against and diverged on a skipped row, a tombstone, or a row
// belonging to another generation, which are the three cases the layers get
// right and a rewrite gets wrong.
//
// The structure is passed as satisfied because this is not asking whether the
// generation is well-formed -- the caller has already read that and reports it
// separately. What it wants is the two counts.
func generationFreshness(
	ctx context.Context, db *sql.DB, spec embedgen.Spec, registered embedstore.Generation,
) (stale, missing int, err error) {
	sources, targets, err := ReadVerificationRows(ctx, db, spec)
	if err != nil {
		return 0, 0, err
	}
	report := embedverify.Verify(
		embedverify.Expectation{Generation: registered.Identity, Dimension: registered.Dimension},
		embedverify.Structure{
			ColumnExists: true, ExtensionPresent: true, Dimension: registered.Dimension,
		},
		sources, targets,
		embedverify.RunState{SnapshotComplete: true, CatchUpReached: true})

	for _, finding := range report.Blocking() {
		switch finding.Layer {
		case embedverify.LayerCoverage:
			missing += finding.Count
		case embedverify.LayerFreshness:
			stale += finding.Count
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

// storedWidthExpression asks the server how wide the vector in a row is.
//
// It depends on the representation because pgvector does. `vector_dims` is
// defined for `vector` and `halfvec` and NOT for `sparsevec` -- measured
// against 0.8.1's pg_proc, where the only sparsevec conversions are the casts
// to the two dense types. Appending it unconditionally is stokaro/ptah#2633: a
// sparsevec generation prepared, backfilled, caught up and indexed, and then
// `verify`, `status` and `cutover` all died with
// `function vector_dims(sparsevec) does not exist`, which stranded the
// generation permanently -- the plan digest an approval binds to is only
// published by a refused cutover, and the cutover could not get that far.
//
// A sparsevec's width is the suffix of its own text form, `{i:v,...}/N`. That
// is read rather than cast, because `col::vector` densifies a sparse value --
// the representation exists not to do that -- and because the two answer
// differently for an UNCONSTRAINED column, where each row carries its own
// width and there is no typmod to read instead. NULL survives the expression as
// NULL, which is what says the row holds no vector.
func storedWidthExpression(representation, column string) string {
	quoted := quoteIdentifier(column)
	if strings.EqualFold(strings.TrimSpace(representation), "sparsevec") {
		return "NULLIF(split_part(" + quoted + "::text, '/', 2), '')::int"
	}
	return "vector_dims(" + quoted + ")"
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
		storedWidthExpression(spec.Target.Representation, column))

	// Whether the row is one the specification asks for, computed by the server
	// from the same predicate that used to be the only thing in the WHERE.
	//
	// The walk reached one row set and split it into both sides, so every
	// target row it produced was in scope by construction and
	// `embedverify.reportOutOfScope` could not fire through the shipped reader
	// (stokaro/ptah#2649 finding 2). A generation carrying vectors for rows the
	// specification excludes -- which catch-up creates on its own -- passed
	// every layer, and the reported target-row count was not the number of
	// vectors in the column.
	//
	// One walk rather than two, and a widened WHERE rather than none: a
	// verification already holds the corpus in memory (stokaro/ptah#2621), so a
	// second full read is not a repair, and dropping the filter entirely would
	// read every row of a table the specification deliberately narrows.
	inScope := "TRUE"
	generationColumn := quoteIdentifier(column + GenerationSuffix)
	filter := strings.TrimSpace(spec.Source.Filter)
	if filter != "" {
		inScope = "(" + filter + ")"
	}
	columns = append(columns, inScope)

	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation or
	// column name; the identifiers come from the specification and go through
	// quoteIdentifier.
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), source.qualifiedTable())
	if filter != "" {
		query += " WHERE (" + filter + ") OR " + generationColumn + " IS NOT NULL"
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
		var inScope sql.NullBool
		targetsScan := make([]any, 0, len(values)+2)
		for index := range values {
			targetsScan = append(targetsScan, &values[index])
		}
		targetsScan = append(targetsScan, &dimension, &inScope)
		if err := rows.Scan(targetsScan...); err != nil {
			return nil, nil, fmt.Errorf("read a verification row: %w", err)
		}
		sourceRow, targetRow, err := splitVerificationRow(values, dimension, spec, keyCount, inputCount)
		if err != nil {
			return nil, nil, err
		}
		// A row the filter excludes is not a source row: the specification does
		// not ask for it, so a verification reporting it missing would report
		// the filter as a defect. It is still a TARGET row, because the vector
		// is there and something has to say so.
		//
		// NULL is not in scope. A three-valued filter answers NULL for a row it
		// can say nothing about, and treating that as "asked for" would make
		// the verification demand a vector the backfill never wrote.
		if inScope.Valid && inScope.Bool {
			sources = append(sources, sourceRow)
		}
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
			Dimension: storedDimension(dimension),
		}, nil
}

// storedDimension is the width the server reported, or zero for no vector.
//
// The width and not the vector. The verification layer asks about length and
// finiteness; `vector_dims` is the server's own answer to the first, and a NaN
// cannot be stored in a pgvector column at all -- it is refused on write, so
// reading every vector back to check would measure the write path twice.
//
// This used to answer with `make([]float32, dimension)`: a zero-filled slice
// per row, carrying nothing the integer does not, and 6 GB of it over a
// million-row corpus at 1536 dimensions.
func storedDimension(dimension sql.NullInt64) int {
	if !dimension.Valid || dimension.Int64 <= 0 {
		return 0
	}
	return int(dimension.Int64)
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
	// qualify and quoteIdentifier.
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $1",
		qualify(generation.TargetSchema, generation.TargetTable),
		quoteIdentifier(generation.TargetColumn+GenerationSuffix))
	var count int
	if err := db.QueryRowContext(ctx, query, generation.Identity).Scan(&count); err != nil {
		return 0, fmt.Errorf("count the rows of generation %s: %w", generation.Identity, err)
	}
	return count, nil
}

// GenerationIndexName is the name of the index the registry row's generation
// built, if it built one.
//
// Derived from the REGISTRY rather than from a specification, because a
// retirement holds no specification for the generation it is destroying. The
// caller's `--spec` describes a different generation -- usually the one
// replacing this one -- and its identity is in the index name.
func GenerationIndexName(generation embedstore.Generation) string {
	return embedgen.IndexName(
		generation.TargetTable, generation.TargetColumn, generation.Identity)
}

// GenerationIndexExists reports whether that index is in the catalog.
//
// A retirement asks before it acts, so the plan an operator approves says what
// is actually there. `DropsIndex` was a literal `true`, so a plan promised to
// drop an index whether or not one existed and the record afterwards claimed
// one had been dropped (stokaro/ptah#2642).
func GenerationIndexExists(
	ctx context.Context, db *sql.DB, generation embedstore.Generation,
) (bool, error) {
	name := GenerationIndexName(generation)
	// The schema is part of the question for the same reason it is part of the
	// DROP: two schemas can hold a like-named index, and answering about the
	// wrong one would put a claim in the plan about somebody else's object
	// (stokaro/ptah#2629). An empty schema means the connection's own
	// search_path, which is what a single-schema installation has.
	const query = `SELECT EXISTS (
		SELECT 1 FROM pg_class ic
		JOIN pg_index i ON i.indexrelid = ic.oid
		JOIN pg_namespace n ON n.oid = ic.relnamespace
		WHERE ic.relname = $1
		  AND n.nspname = COALESCE(NULLIF($2, ''), current_schema()))`
	var exists bool
	err := db.QueryRowContext(ctx, query, name, generation.TargetSchema).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("read index %s: %w", name, err)
	}
	return exists, nil
}

// RetireIndex drops a generation's index and leaves its vectors.
//
// The index goes first and the columns second, because dropping a column takes
// its index with it and a failure between the two would otherwise leave the
// index gone and the column there -- a generation that is neither retired nor
// usable.
//
// It takes the registry row and no specification. It used to take the caller's
// spec with only Target.Column swapped in, and Target.Column is an identity
// field, so the digest baked into the generated name belonged to a hybrid that
// was no generation at all: the DROP matched nothing, the index survived, and
// with --drop-column=false -- the only mode in which dropping the index IS the
// operation -- the verb reported the generation gone at exit 0
// (stokaro/ptah#2642).
//
// There is no HasIndex gate any more, and its absence is deliberate. Whether
// the retired generation built an index is a fact about THAT generation, and
// the registry does not record it; the gate consulted the current
// specification's index method, which answers about a different generation.
// `IF EXISTS` is the honest form of the same question, asked of the server.
func RetireIndex(
	ctx context.Context, db *sql.DB, generation embedstore.Generation,
) error {
	// An index lives in its table's schema, so the DROP names it there. Left
	// bare it resolved through search_path, which dropped a like-named index
	// belonging to another schema's generation (stokaro/ptah#2629).
	name := qualify(generation.TargetSchema, GenerationIndexName(generation))
	// #nosec G201 -- a generated index name, through qualify and quoteIdentifier.
	drop := fmt.Sprintf("DROP INDEX IF EXISTS %s", name)
	if _, err := db.ExecContext(ctx, drop); err != nil {
		return fmt.Errorf("drop index %s: %w", name, err)
	}
	return nil
}

// RetireColumns drops a generation's vector column and its metadata.
func RetireColumns(
	ctx context.Context, db *sql.DB, generation embedstore.Generation,
) error {
	for _, suffix := range append([]string{""}, MetadataSuffixes()...) {
		// #nosec G201 -- relation and column names from the registry, through
		// qualify and quoteIdentifier.
		drop := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s",
			qualify(generation.TargetSchema, generation.TargetTable),
			quoteIdentifier(generation.TargetColumn+suffix))
		if _, err := db.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("drop column %s%s: %w", generation.TargetColumn, suffix, err)
		}
	}
	return nil
}
