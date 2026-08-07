package migrator

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"go.5x5.cz/ptah/core/sqlutil"
)

// partialHashPrefix is the tag Atlas puts on every digest it records, the same
// one atlas.sum uses.
const partialHashPrefix = "h1:"

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
