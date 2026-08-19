package migrator

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/sqlutil"
)

// partialHashPrefix is the tag Atlas puts on every digest it records, the same
// one atlas.sum uses.
const partialHashPrefix = "h1:"

// nativeDirtyChecksumPrefix distinguishes a dirty native revision's committed
// prefix digest from the full migration checksum stored by clean revisions.
// The encoded value remains within the existing VARCHAR(64) checksum column:
// "partial:" plus Atlas's h1-prefixed SHA-256 digest is 55 bytes.
const nativeDirtyChecksumPrefix = "partial:"

// atlasPartialHashes returns the value the `partial_hashes` column takes for a
// migration that stopped after applied statements out of total.
//
// The column records one digest per applied statement, and each digest covers
// every statement up to and including that one: entry i is over
// S1 ‖ S2 ‖ … ‖ Si+1, concatenated with no separator. That is what lets a
// resume verify the statements already applied are the same statements in the
// same order, rather than merely the same count.
//
// Each Sk is the statement's SOURCE text, from its first meaningful token
// through its terminating semicolon, verbatim. Not the executor's normalized
// form: measured against the pinned Atlas community binary v1.3.0,
//
//	CREATE TABLE q (id int)
//	;
//
// hashes as "CREATE TABLE q (id int)\n;" and not as "CREATE TABLE q (id int);".
// Comments preceding a statement are excluded. See stokaro/ptah#1196.
//
// A null is returned for anything that is not a partial application, which is
// what that binary writes: nothing applied has no prefix to record, and a clean
// success has nothing to resume.
func (m *Migrator) atlasPartialHashes(sqlText string, applied, total int) any {
	hashes, ok := atlasPartialHashValues(sqlText, m.connectionDialect(), applied, total)
	if !ok {
		return m.atlasNullJSONValue()
	}
	return m.atlasPartialHashesJSON(hashes)
}

// atlasDirtyPartialHashes preserves enough metadata to validate a later
// resume. Atlas-compatible clean up completion still writes null, while a
// Ptah-owned dirty down row keeps its cumulative prefix even when the whole
// down body committed and a later safety check prevented finalization.
func (m *Migrator) atlasDirtyPartialHashes(
	sqlText string,
	direction MigrationDirection,
	applied,
	total int,
) any {
	if direction == MigrationDirectionUp {
		return m.atlasPartialHashes(sqlText, applied, total)
	}
	hashes, ok := cumulativePartialHashValues(sqlText, m.connectionDialect(), applied)
	if !ok {
		return m.atlasNullJSONValue()
	}
	return m.atlasPartialHashesJSON(hashes)
}

func (m *Migrator) atlasPartialHashesJSON(hashes []string) any {
	encoded, err := json.Marshal(hashes)
	if err != nil {
		// json.Marshal of []string cannot fail; falling back to the null keeps
		// the failure-recording path from turning a migration failure into a
		// second, unrelated one.
		return m.atlasNullJSONValue()
	}
	return m.atlasJSONValue(string(encoded))
}

// atlasPartialHashValues computes the cumulative digests, reporting false when
// the run is not a partial application or the statements cannot be recovered.
func atlasPartialHashValues(sqlText, dialect string, applied, total int) ([]string, bool) {
	if applied <= 0 || applied >= total {
		return nil, false
	}
	return cumulativePartialHashValues(sqlText, dialect, applied)
}

// cumulativePartialHashValues computes the digest chain through applied,
// including the whole body when applied equals total. Atlas writes null for a
// clean full application, but native dirty rows need the full-prefix form when
// statement execution finished and a later safety check blocked completion.
func cumulativePartialHashValues(sqlText, dialect string, applied int) ([]string, bool) {
	if applied <= 0 {
		return nil, false
	}
	statements := sqlutil.SplitSourceStatements(sqlText, dialect)
	if len(statements) < applied {
		// The executor counted more statements than the source split finds.
		// Recording a prefix shorter than the one that ran would tell a resume
		// that fewer statements are committed than really are, so the honest
		// answer is the null the column already carried.
		return nil, false
	}
	hashes := make([]string, 0, applied)
	digest := sha256.New()
	for _, statement := range statements[:applied] {
		digest.Write([]byte(statement.Text))
		hashes = append(hashes, partialHashPrefix+base64.StdEncoding.EncodeToString(digest.Sum(nil)))
	}
	return hashes, true
}

func nativeDirtyChecksum(sqlText, dialect string, applied int, fallback string) string {
	hashes, ok := cumulativePartialHashValues(sqlText, dialect, applied)
	if !ok {
		return fallback
	}
	return nativeDirtyChecksumPrefix + hashes[len(hashes)-1]
}

func parseNativeDirtyChecksum(value string) (string, bool, error) {
	digest, ok := strings.CutPrefix(value, nativeDirtyChecksumPrefix)
	if !ok {
		return "", false, nil
	}
	if err := validatePartialHash(digest); err != nil {
		return "", true, err
	}
	return digest, true, nil
}

func parseAtlasPartialHashes(value any) ([]string, error) {
	var raw string
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case string:
		raw = typed
	case []byte:
		raw = string(typed)
	default:
		return nil, fmt.Errorf("unsupported Atlas partial_hashes value %T", value)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == atlasNullJSON {
		return nil, nil
	}
	var hashes []string
	if err := json.Unmarshal([]byte(raw), &hashes); err != nil {
		return nil, fmt.Errorf("invalid Atlas partial_hashes: %w", err)
	}
	for _, hash := range hashes {
		if err := validatePartialHash(hash); err != nil {
			return nil, fmt.Errorf("invalid Atlas partial_hashes entry: %w", err)
		}
	}
	return hashes, nil
}

func validatePartialHash(value string) error {
	encoded, ok := strings.CutPrefix(value, partialHashPrefix)
	if !ok {
		return fmt.Errorf("digest %q is missing the %q prefix", value, partialHashPrefix)
	}
	digest, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("digest %q is not valid base64: %w", value, err)
	}
	if len(digest) != sha256.Size {
		return fmt.Errorf("digest %q decodes to %d bytes, expected %d", value, len(digest), sha256.Size)
	}
	return nil
}
