package embedpg

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ptah.run/internal/embedcutover"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedspec"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
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

	// Freshness only where the decision can still use it, and each of the three
	// exclusions is a different reason.
	//
	// A retired generation has no columns left and an absent one never had any,
	// so the walk that reads `<column>_generation` fails with a raw `column
	// does not exist` before the decision layer -- which HAS a designed refusal
	// for both, and never got to give it (stokaro/ptah#2647).
	//
	// The generation queries already read is excluded for cost rather than
	// correctness: the refusal for it is unconditional, so measuring every row
	// of the corpus first is work whose answer nothing reads.
	alreadyActive := pointer.Active == generation
	var stale, missing int
	if structure.ColumnExists && !registered.Retired() && !alreadyActive {
		stale, missing, err = generationFreshness(ctx, db, previous, registered)
		if err != nil {
			return embedcutover.RollbackState{}, err
		}
	}
	return embedcutover.RollbackState{
		Present:       structure.ColumnExists,
		AlreadyActive: alreadyActive,
		IndexReady:    structure.IndexExists && structure.IndexValid,
		Retired:       registered.Retired(),
		CutOverAt:     pointer.CutOverAt,
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
	loaded, err := RecordedSpec(registered,
		"still fresh enough to roll back to")
	if err != nil {
		return embedgen.Spec{}, err
	}
	return loaded.Spec, nil
}

// RecordedSpec parses the document a generation was registered with.
//
// It returns the whole [embedspec.Loaded] rather than the specification alone
// because the consistency mode is on it, and the mode a generation was built
// with is a property of THAT generation. Read off the invocation's file
// instead, retiring an outbox-built generation while passing an immutable
// specification skipped the outbox removal entirely and said nothing
// (stokaro/ptah#2649).
//
// The what argument completes the sentence "so nothing can measure whether it
// is ...", so a refusal names the question its caller was answering rather than
// a generic absence.
func RecordedSpec(
	registered embedstore.Generation, what string,
) (embedspec.Loaded, error) {
	if strings.TrimSpace(registered.SpecDocument) == "" {
		return embedspec.Loaded{}, fmt.Errorf(
			"generation %s records no specification, so nothing can measure whether it is %s",
			registered.Identity, what)
	}
	return embedspec.ParsePublished(
		[]byte(registered.SpecDocument), "the recorded specification", registered.SpecDigest)
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
	corpus, err := VerificationCorpus(ctx, db, spec)
	if err != nil {
		return 0, 0, err
	}
	report, err := embedverify.Verify(
		embedverify.Expectation{Generation: registered.Identity, Dimension: registered.Dimension},
		embedverify.Structure{
			ColumnExists: true, ExtensionPresent: true, Dimension: registered.Dimension,
		},
		corpus,
		embedverify.RunState{SnapshotComplete: true, CatchUpReached: true})
	if err != nil {
		return 0, 0, err
	}

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

// VerificationCorpus walks both sides of the comparison in one statement.
//
// One walk, because the two sides have to agree about which key a row is. Two
// queries would let a row inserted between them appear on one side and not the
// other, and the report would name it as a coverage gap that nothing created.
//
// The walk is handed back as a sequence rather than as two slices, and it owns
// the result set: the statement closes when the range loop over it ends, by
// break or by exhaustion. Nothing between here and the report holds the corpus,
// which is the whole point -- materializing it made a verification's memory
// proportional to the number of rows rather than to the number of findings
// (stokaro/ptah#2621).
//
// The scan buffers and the two rows a position points at are built once and
// written through on every row, so what a row costs does not depend on how many
// there are. `embedverify.Pair` is what allows that: neither pointer outlives
// the yield that produced it.
//
// A read that fails partway yields the error as the sequence's second value and
// stops. It is not reported as a finding: an unread row is indistinguishable
// from an in-scope row with no vector, so a report built over a truncated walk
// blames the data for the read.
//
// Equal keys arrive adjacent, which is what `embedverify.Corpus` requires. The
// ORDER BY is over the key COLUMNS, so equal keys are neighbors whatever the
// encoded key strings would sort like.
//
// It is over the chunk ordinal too, and that is not a tidiness: the walk folds
// a source key's stored rows into a set and requires their ordinals to be
// 0, 1, 2 ... in order. PostgreSQL returns a key's rows in whatever order the
// scan produced them, so without this a perfectly formed set arrives shuffled
// and every chunked key is reported as holding rows its set does not declare.
// Measured: 3 source keys, 30 well-formed stored rows, 3 blocking findings.
func VerificationCorpus(
	ctx context.Context, db *sql.DB, spec embedgen.Spec,
) (embedverify.Corpus, error) {
	source, err := NewSource(db, spec)
	if err != nil {
		return nil, err
	}
	query, err := verificationQuery(spec, source)
	if err != nil {
		return nil, err
	}
	//nolint:rowserrcheck // Rows.Err is checked, in the iterator returned below:
	// this function hands back an iter.Seq2 rather than iterating here, and
	// rowserrcheck's analysis does not follow the rows into the closure. See the
	// `if err := rows.Err()` after the loop.
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read %s for verification: %w", verificationSubject(spec), err)
	}
	scanner := newVerificationScanner(spec)
	return func(yield func(embedverify.Pair, error) bool) {
		defer rows.Close()
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

// verificationOrder is what the corpus is sorted by.
//
// The chunk ordinal joins the key columns only where the layout has one. Under
// the other layout [storedOrdinalExpression] answers with the literal zero,
// and a bare integer in an ORDER BY is a COLUMN POSITION to PostgreSQL: the
// query was refused with `ORDER BY position 0 is not in select list`, on every
// verification of every unchunked generation. There is also nothing to order:
// that layout stores one row per source key.
func verificationOrder(spec embedgen.Spec, keys []string) []string {
	order := append([]string(nil), keys...)
	if spec.Target.Layout.OwnsTable() {
		order = append(order, storedOrdinalExpression(spec))
	}
	return order
}

// storedOrdinalExpression is a stored row's position in its source key's set.
//
// A literal zero where the layout stores one vector per source row, because
// that relation has no ordinal column and every one of its rows is the whole of
// its key's set. Selecting it anyway is what lets both query builders hand the
// scanner one shape, the same reason the presence literal is selected there.
func storedOrdinalExpression(spec embedgen.Spec) string {
	if !spec.Target.Layout.OwnsTable() {
		return "0"
	}
	return quoteIdentifier(spec.Target.Column + OrdinalSuffix)
}

// verificationQuery renders the one walk over both sides.
//
// Which shape it renders depends on whether the specification's two sides name
// one relation or two, and that is the whole of stokaro/ptah#2736: the
// single-relation query reads the target's four state columns off the source
// table, which is correct exactly when they are the same table and a missing
// column when they are not. `verify`, `status` and the cutover they gate all
// died with `column "embedding_generation" does not exist` on a specification
// that kept its vectors out of the hot table -- a shape `prepare`, `backfill`,
// `catchup` and `index` all accept, and one an outbox belongs to the source
// half of.
func verificationQuery(spec embedgen.Spec, source *Source) (string, error) {
	if oneRelation(spec) {
		return oneRelationVerificationQuery(spec, source), nil
	}
	return joinedVerificationQuery(spec, source), nil
}

// oneRelation reports whether the specification's two sides name one relation.
//
// Compared as it is written rather than as the server would resolve it: an
// unqualified name means the connection's own search_path, and this has no
// connection to ask. Two spellings of one relation therefore take the joined
// shape, which reads that relation twice and answers identically -- a full
// outer join of a table to itself on its own key is the identity. The mistake
// in the other direction is the one with no recovery, because it reads one
// table's columns off another.
func oneRelation(spec embedgen.Spec) bool {
	return strings.TrimSpace(spec.Source.Schema) == strings.TrimSpace(spec.Target.Schema) &&
		spec.Source.Table == spec.Target.Table
}

// verificationSubject names what the walk reads, for a diagnostic.
//
// Both relations when the specification names two. The error a failed read
// carries is usually about a column, and a message naming one table for a
// column that lives on the other sends the reader to the wrong `\d`: that is
// exactly how stokaro/ptah#2736 arrived, as `read articles for verification:
// column "embedding_generation" does not exist` about a column on
// `article_vectors`.
//
// Schema-qualified, through the same helper the statements use, because the two
// relations can differ by schema alone -- `alpha.articles` and `beta.articles`
// are two tables, and naming them by their bare names would produce
// `articles and articles`, which reads as a bug in the message rather than as
// the two relations it is about. `ReadStructure` already names its subject this
// way.
func verificationSubject(spec embedgen.Spec) string {
	source := qualify(spec.Source.Schema, spec.Source.Table)
	if oneRelation(spec) {
		return source
	}
	return source + " and " + qualify(spec.Target.Schema, spec.Target.Table)
}

// oneRelationVerificationQuery renders the walk when both sides are one table.
//
// Kept beside the joined shape rather than folded into it because a
// verification is the one read proportional to the corpus (stokaro/ptah#2621),
// and this is the overwhelmingly common specification: making it join a
// million-row table to itself to reach an identical answer would be a cost paid
// on every run for a generality this case does not have.
func oneRelationVerificationQuery(spec embedgen.Spec, source *Source) string {
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
		storedWidthExpression(spec.Target.Representation, column),
		storedOrdinalExpression(spec))

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
	// And whether anything stands at the key on the target side, which here is
	// the row itself: one relation cannot have a position the target half is
	// missing from. It is selected anyway so that both builders hand the
	// scanner the same shape, and so that the scanner has one answer to read
	// rather than a rule about which query produced the row.
	columns = append(columns, inScope, "TRUE")

	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation or
	// column name; the identifiers come from the specification and go through
	// quoteIdentifier.
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), source.qualifiedTable())
	if filter != "" {
		query += " WHERE (" + filter + ") OR " + generationColumn + " IS NOT NULL"
	}
	return query + " ORDER BY " + strings.Join(verificationOrder(spec, keys), ", ")
}

// The column names the joined walk gives itself.
//
// Every expression either side selects is aliased to one of these, so the two
// derived tables publish no name the specification chose. A source column
// called `in_scope`, or one whose name collides with a target state column,
// would otherwise make `s.in_scope` ambiguous and refuse the whole read -- and
// the specification is where those names come from, so there is no name here
// that a table could not already have.
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
	sourceSide          = "s"
	targetSide          = "t"
)

// aliasAt names one member of a list the join carries positionally.
func aliasAt(prefix string, index int) string {
	return prefix + strconv.Itoa(index)
}

// coalescedKeys is the key each output row is identified by.
//
// A full outer join produces rows the source did not have and rows the target
// did not have, so neither side's key column is present on every row. The pair
// the scanner builds carries ONE key for both halves, which the join condition
// makes sound: where both sides are present their keys are equal.
func coalescedKeys(keys []string) []string {
	coalesced := make([]string, len(keys))
	for index := range keys {
		alias := aliasAt(keyAliasPrefix, index)
		coalesced[index] = fmt.Sprintf("COALESCE(%s.%s, %s.%s)", sourceSide, alias, targetSide, alias)
	}
	return coalesced
}

// joinedVerificationQuery renders the walk when the two sides are two tables.
//
// A full outer join and not a left one. Verification has to see a target row
// whose key the source no longer has -- catch-up writes tombstones, a delete
// leaves a vector behind, and `embedverify` reports both -- so dropping the
// unmatched target side would answer that a generation is clean by not looking
// at the rows that are not.
//
// The join is on the keys AS TEXT, which is exactly how the write path
// addresses a target row: `Target.upsertStatement` renders
// `<key>::text = $n` against values the scan produced the same way. A join that
// matched rows the writer cannot address would report coverage for vectors that
// could never have been written there.
func joinedVerificationQuery(spec embedgen.Spec, source *Source) string {
	keys := coalescedKeys(spec.Source.KeyFields)
	columns := append([]string(nil), keys...)
	for index := range spec.Source.InputFields {
		columns = append(columns, sourceSide+"."+aliasAt(inputFieldPrefix, index))
	}
	for index := range source.versionColumns() {
		columns = append(columns, sourceSide+"."+aliasAt(sourceVersionPrefix, index))
	}
	scope := fmt.Sprintf("COALESCE(%s.%s, FALSE)", sourceSide, scopeAlias)
	presence := fmt.Sprintf("COALESCE(%s.%s, FALSE)", targetSide, presenceAlias)
	columns = append(columns,
		targetSide+"."+generationAlias, targetSide+"."+inputHashAlias,
		targetSide+"."+targetVersionAlias, targetSide+"."+stateAlias,
		targetSide+"."+storedWidthAlias,
		// COALESCE, because a source row with no stored row leaves the whole
		// target side NULL and the scanner reads the ordinal as an integer. A
		// position with no row is the start of an empty set, which is ordinal
		// zero.
		fmt.Sprintf("COALESCE(%s.%s, 0)", targetSide, storedOrdinalAlias),
		// A row only the target has is not one the specification asked for: its
		// source columns are absent, so a literal TRUE here would hand the
		// scanner a source row made of nulls and the verification would demand
		// a vector for a key the source does not hold.
		scope,
		// And the mirror of it. A row only the source has is not a target row:
		// nothing stands at the key, which is the state `embedverify.Pair`
		// spells as a nil Target. A literal here counted an absent row in
		// `TargetRows` and reported it as a stored row carrying no vector
		// (stokaro/ptah#2781).
		//
		// Neither answer can be a bare literal in the SELECT list, because one
		// there is evaluated per OUTPUT row and is true on both sides of an
		// outer join. Each side carries its own inside its derived table, where
		// the join is what makes it NULL.
		presence)

	on := make([]string, 0, len(spec.Source.KeyFields))
	for index := range spec.Source.KeyFields {
		alias := aliasAt(keyAliasPrefix, index)
		on = append(on, fmt.Sprintf("%s.%s = %s.%s", sourceSide, alias, targetSide, alias))
	}

	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation or
	// column name; the identifiers come from the specification and go through
	// quoteIdentifier, and every alias here is this package's own literal.
	query := fmt.Sprintf("SELECT %s FROM %s %s FULL OUTER JOIN %s %s ON %s",
		strings.Join(columns, ", "),
		verificationSourceSide(spec, source), sourceSide,
		verificationTargetSide(spec), targetSide,
		strings.Join(on, " AND "))
	// Unconditionally, unlike the single-relation shape, and the difference is
	// not an oversight in either direction. There a row can only be out of
	// scope because a filter excluded it, so with no filter the predicate is
	// `TRUE OR ...` and emitting it says nothing. Here "out of scope" also
	// covers "absent from the source", which happens with no filter at all --
	// so guarding the WHERE on a filter made the verdict for one database
	// depend on whether the specification carried a filter that had nothing to
	// do with the row. Measured on identical state, a no-op `filter: 'id > 0'`
	// turned a blocking finding into `every deterministic layer passed`
	// (stokaro/ptah#2781).
	//
	// What it drops is a target row standing at a key the source does not have
	// and carrying no generation: a sidecar row nothing ever wrote. It belongs
	// to no generation, so no generation's verification is the place it is
	// reported.
	query += fmt.Sprintf(" WHERE %s OR %s.%s IS NOT NULL", scope, targetSide, generationAlias)
	order := append([]string(nil), keys...)
	if spec.Target.Layout.OwnsTable() {
		// COALESCE, because a source row with no stored row leaves the whole
		// target side NULL, and a set that does not exist yet begins at zero.
		order = append(order,
			fmt.Sprintf("COALESCE(%s.%s, 0)", targetSide, storedOrdinalAlias))
	}
	return query + " ORDER BY " + strings.Join(order, ", ")
}

// verificationSourceSide is the source half of the join, as a derived table.
//
// The filter is evaluated here rather than in the outer WHERE because that is
// where the source's own columns are in scope; what leaves this subquery is one
// boolean saying whether the specification asks for the row.
func verificationSourceSide(spec embedgen.Spec, source *Source) string {
	selected := make([]string, 0, len(spec.Source.KeyFields)+len(spec.Source.InputFields)+2)
	for index, key := range spec.Source.KeyFields {
		selected = append(selected,
			quoteIdentifier(key)+"::text AS "+aliasAt(keyAliasPrefix, index))
	}
	for index, field := range spec.Source.InputFields {
		selected = append(selected,
			quoteIdentifier(field)+" AS "+aliasAt(inputFieldPrefix, index))
	}
	for index, field := range source.versionColumns() {
		selected = append(selected,
			quoteIdentifier(field)+" AS "+aliasAt(sourceVersionPrefix, index))
	}
	scope := "TRUE"
	if filter := strings.TrimSpace(spec.Source.Filter); filter != "" {
		scope = "(" + filter + ")"
	}
	selected = append(selected, scope+" AS "+scopeAlias)
	// #nosec G201 -- as verificationQuery: identifiers from the specification
	// through quoteIdentifier, aliases this package's own literals.
	return fmt.Sprintf("(SELECT %s FROM %s)",
		strings.Join(selected, ", "), source.qualifiedTable())
}

// verificationTargetSide is the target half, carrying the four state columns
// and the width of the vector standing at the key.
//
// It selects the key by the SOURCE's key field names, because that is what the
// write path addresses a target row by: a target table is a relation carrying
// the same key, which is what makes an `UPDATE ... WHERE key = $n` reach the
// row a source row's vector belongs to.
func verificationTargetSide(spec embedgen.Spec) string {
	column := spec.Target.Column
	selected := make([]string, 0, len(spec.Source.KeyFields)+5)
	for index, key := range spec.Source.KeyFields {
		selected = append(selected,
			quoteIdentifier(key)+"::text AS "+aliasAt(keyAliasPrefix, index))
	}
	selected = append(selected,
		quoteIdentifier(column+GenerationSuffix)+" AS "+generationAlias,
		quoteIdentifier(column+InputHashSuffix)+" AS "+inputHashAlias,
		quoteIdentifier(column+VersionSuffix)+" AS "+targetVersionAlias,
		quoteIdentifier(column+StateSuffix)+" AS "+stateAlias,
		storedWidthExpression(spec.Target.Representation, column)+" AS "+storedWidthAlias,
		storedOrdinalExpression(spec)+" AS "+storedOrdinalAlias,
		// What says a row stands here at all. Every other column this side
		// selects can be NULL in a row that exists -- a target row is created
		// before anything writes a vector into it -- so none of them can answer
		// it. A literal inside the derived table can, because the join is what
		// turns it into NULL.
		"TRUE AS "+presenceAlias)
	// #nosec G201 -- as verificationQuery: identifiers from the specification
	// through quoteIdentifier, aliases this package's own literals.
	return fmt.Sprintf("(SELECT %s FROM %s)",
		strings.Join(selected, ", "), qualify(spec.Target.Schema, spec.Target.Table))
}

// verificationScanner reads result rows into storage it reuses.
//
// Reused rather than allocated per row because `embedverify.Pair` permits it:
// Verify copies what it needs before asking for the next position, and a
// finding holds key strings, which are immutable. A reader that allocated a
// pair per row would be paying for a guarantee nothing asks for, and this walk
// exists not to cost anything proportional to the corpus (stokaro/ptah#2621).
//
// The scan destinations are built once too. They are the same addresses on
// every row, so `database/sql` writes through them into the fields below.
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
	width := scanner.keyCount + scanner.inputCount + len(versionColumnsOf(spec)) + 4
	scanner.values = make([]sql.NullString, width)
	scanner.into = make([]any, 0, width+4)
	for index := range scanner.values {
		scanner.into = append(scanner.into, &scanner.values[index])
	}
	scanner.into = append(scanner.into,
		&scanner.dimension, &scanner.ordinal, &scanner.inScope, &scanner.present)
	return scanner
}

// pair reads the current row into the scanner's own rows and points at them.
func (s *verificationScanner) pair(rows *sql.Rows) (embedverify.Pair, error) {
	if err := rows.Scan(s.into...); err != nil {
		return embedverify.Pair{}, fmt.Errorf("read a verification row: %w", err)
	}
	// Which halves this position actually has, decided before either is built.
	//
	// A row the filter excludes is not a source row: the specification does
	// not ask for it, so a verification reporting it missing would report
	// the filter as a defect. NULL is not in scope either -- a three-valued
	// filter answers NULL for a row it can say nothing about, and treating that
	// as "asked for" would make the verification demand a vector the backfill
	// never wrote.
	//
	// And a row the target relation has no row for is not a target row.
	// `embedverify.Pair` spells that as a nil Target, and under the join it is
	// a position that really occurs.
	inScope := s.inScope.Valid && s.inScope.Bool
	present := s.present.Valid && s.present.Bool

	s.target = targetVerificationRow(
		s.values, s.dimension, s.ordinal, s.spec, s.keyCount, s.inputCount)
	pair := embedverify.Pair{}
	if present {
		pair.Target = &s.target
	}
	if !inScope {
		return pair, nil
	}
	// Canonicalization happens here and not above, because it is the source
	// half's work and it can REFUSE. A target-only position carries every input
	// field as NULL by construction, so a specification whose null or empty
	// policy is `refuse` turned every tombstone into a fatal error for the whole
	// walk -- `verify`, `status` and the cutover they gate (stokaro/ptah#2781).
	// A row the specification does not ask for is not a row its input policy
	// governs.
	source, err := sourceVerificationRow(s.values, s.spec, s.keyCount, s.inputCount)
	if err != nil {
		return embedverify.Pair{}, err
	}
	s.source = source
	pair.Source = &s.source
	return pair, nil
}

// verificationKey is the key both halves of a position are identified by.
func verificationKey(values []sql.NullString, keyCount int) []string {
	key := make([]string, keyCount)
	for index := range keyCount {
		key[index] = values[index].String
	}
	return key
}

// targetStateOffset is where the four state columns start in a scanned row.
func targetStateOffset(spec embedgen.Spec, keyCount, inputCount int) int {
	return keyCount + inputCount + len(versionColumnsOf(spec))
}

// targetVerificationRow is what the stored side of one position says.
//
// Built for every row, including a position the target relation has no row for:
// the caller decides whether to hand it over, and a value nobody points at
// costs nothing because the scanner reuses one.
func targetVerificationRow(
	values []sql.NullString, dimension, ordinal sql.NullInt64,
	spec embedgen.Spec, keyCount, inputCount int,
) embedverify.TargetRow {
	offset := targetStateOffset(spec, keyCount, inputCount)
	state := values[offset+3].String
	return embedverify.TargetRow{
		Key:        embedverify.KeyIdentity(verificationKey(values, keyCount)...),
		Generation: values[offset].String,
		InputHash:  values[offset+1].String,
		Version:    values[offset+2].String,
		Tombstone:  state == "tombstone",
		Skipped:    state == "skip",
		Dimension:  storedDimension(dimension),
		Ordinal:    int(ordinal.Int64),
	}
}

// sourceVerificationRow is what the source side of one position says.
//
// It canonicalizes, so it is the half that can refuse, and it is called only
// for a position the specification asks for.
func sourceVerificationRow(
	values []sql.NullString, spec embedgen.Spec, keyCount, inputCount int,
) (embedverify.SourceRow, error) {
	key := verificationKey(values, keyCount)
	fields := make([]*string, inputCount)
	for index := range inputCount {
		if value := values[keyCount+index]; value.Valid {
			fields[index] = &value.String
		}
	}
	version := ""
	if len(versionColumnsOf(spec)) > 0 {
		version = values[keyCount+inputCount].String
	}

	// The WHOLE canonical input, not a chunk: a stored row's hash is taken over
	// the source row it came from, so freshness is recomputed the same way for
	// a chunked corpus as for an unchunked one.
	set, err := spec.CanonicalInputs(embedgen.Row{Key: key, Fields: fields})
	input := set.Whole
	if err != nil {
		return embedverify.SourceRow{},
			fmt.Errorf("canonicalize %v for verification: %w", key, err)
	}
	return embedverify.SourceRow{
		Key:       embedverify.KeyIdentity(key...),
		Version:   version,
		InputHash: spec.SourceInputHash(input),
		Skipped:   input.Skipped,
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
	ctx context.Context, db queryRower, generation embedstore.Generation,
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
	ctx context.Context, db queryRower, generation embedstore.Generation,
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
type contextExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func RetireIndex(
	ctx context.Context, db contextExecer, generation embedstore.Generation,
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
// relationInspector is the SQL surface dropping an owned relation needs: the
// DROP, and the question that has to be answered before it.
type relationInspector interface {
	contextExecer
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// RetireTable drops the relation a generation stored its vectors in, and
// refuses unless Ptah created that relation for this generation.
//
// The proof is the comment [embedgen.TableComment] writes, read back here. It
// is a real refusal rather than a formality: RetireColumns removes columns from
// a relation the application keeps, and this removes the relation and every row
// in it, so the two are not the same operation made general. A registry row
// naming a target Ptah never created reaches this function exactly as a
// well-formed one does -- through a specification whose layout was changed
// after the generation was prepared, or through a target pointed at an
// application table -- and the comment is what separates them.
//
// A relation that is already gone is not an error. Retirement is idempotent for
// the same reason preparation is, and a DROP that finds nothing has reached the
// state it was asked for.
func RetireTable(
	ctx context.Context, db relationInspector, generation embedstore.Generation,
) error {
	table := qualify(generation.TargetSchema, generation.TargetTable)
	want := embedgen.TableComment(generation.Identity)
	const query = `SELECT to_regclass($1) IS NOT NULL,
		COALESCE(obj_description(to_regclass($1), 'pg_class'), '')`
	var exists bool
	var found string
	if err := db.QueryRowContext(ctx, query, table).Scan(&exists, &found); err != nil {
		return fmt.Errorf("read the target table %s of generation %s: %w",
			table, generation.Identity, err)
	}
	if !exists {
		return nil
	}
	if found != want {
		return fmt.Errorf(
			"%w: relation %s is not Ptah's to drop: retiring generation %s would remove it "+
				"and every row in it, and it carries the comment %q rather than %q. Drop it "+
				"by hand if it really is this generation's storage",
			embedstore.ErrConflict, table, generation.Identity, found, want)
	}
	// #nosec G201 -- relation name from the registry, through qualify.
	drop := fmt.Sprintf("DROP TABLE %s", table)
	if _, err := db.ExecContext(ctx, drop); err != nil {
		return fmt.Errorf("drop the target table %s of generation %s: %w",
			table, generation.Identity, err)
	}
	return nil
}

func RetireColumns(
	ctx context.Context, db contextExecer, generation embedstore.Generation,
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
