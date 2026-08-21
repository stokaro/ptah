package mysql

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
)

// readSequences reports the standalone sequences the connected database holds.
//
// MySQL has no SEQUENCE object at all; MariaDB has had one since 10.3. The read
// is therefore behind capability.Sequences rather than behind a version string:
// a target whose preset says it has no sequences has none to read, and asking
// anyway would fail the whole description on a server that answers the question
// with a syntax error (stokaro/ptah#1759).
//
// The enumeration and the options come from two different places, and it is not
// a choice. information_schema.SEQUENCES carries start, bounds, increment and
// cycle -- but not the cache size, which is the one option only the sequence's
// own row reports. Measured on MariaDB 12.3:
//
//	SELECT * FROM s2\G
//	next_not_cached_value: 7   minimum_value: 1   maximum_value: 900
//	start_value: 7   increment: 3   cache_size: 42   cycle_option: 1
//
// Reading that row consumes nothing: it is a SELECT over the sequence's storage,
// not a NEXTVAL, and next_not_cached_value is unchanged afterwards. A read that
// advanced the sequence would be a description with a side effect, which is the
// one thing a reader must never be.
func (r *Reader) readSequences(dbName string) ([]types.DBSequence, error) {
	if !r.caps.Has(capability.Sequences) {
		return nil, nil
	}
	names, err := r.sequenceNames(dbName)
	if err != nil {
		return nil, err
	}

	sequences := make([]types.DBSequence, 0, len(names))
	for _, name := range names {
		sequence, err := r.readSequenceOptions(dbName, name)
		if err != nil {
			return nil, err
		}
		sequences = append(sequences, sequence)
	}
	return sequences, nil
}

// sequenceNames lists the sequences in a database.
//
// TABLE_TYPE = 'SEQUENCE' rather than information_schema.SEQUENCES because the
// two agree on membership and only this one is also the table a name can be
// quoted from with the same rules every other object here uses.
func (r *Reader) sequenceNames(dbName string) ([]string, error) {
	rows, err := r.db.Query(`
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'SEQUENCE'
		ORDER BY TABLE_NAME`, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to query sequences: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan sequence name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read sequences: %w", err)
	}
	return names, nil
}

// readSequenceOptions reads one sequence's own row.
//
// Every option comes back filled in, including the ones the author never wrote:
// the engine resolved its defaults at creation time and keeps no record of which
// clauses the statement actually named. That is safe for the same reason it is
// safe on SQL Server -- the comparator compares only the options a DECLARATION
// sets and treats an unset one as unmanaged, so a fully populated read against a
// declaration that named nothing produces no change.
func (r *Reader) readSequenceOptions(dbName, name string) (types.DBSequence, error) {
	sequence := types.DBSequence{Name: name, Schema: dbName, DataType: "bigint"}
	var start, increment, minValue, maxValue, cache int64
	var cycle bool

	query := fmt.Sprintf(
		"SELECT start_value, increment, minimum_value, maximum_value, cache_size, cycle_option FROM %s.%s",
		quoteMySQLIdentifier(dbName), quoteMySQLIdentifier(name),
	)
	err := r.db.QueryRow(query).Scan(&start, &increment, &minValue, &maxValue, &cache, &cycle)
	if err != nil {
		return types.DBSequence{}, fmt.Errorf("failed to read sequence %s: %w", name, err)
	}

	sequence.Start = &start
	sequence.Increment = &increment
	sequence.MinValue = &minValue
	sequence.MaxValue = &maxValue
	sequence.Cache = &cache
	sequence.Cycle = cycle
	return sequence, nil
}

// quoteMySQLIdentifier wraps a name in backticks, doubling any it contains.
//
// The name comes from the catalog rather than from a user, so this is not the
// boundary an injection would cross -- but a sequence named with a backtick
// would still produce a statement that does not parse, and a reader that fails
// on a legal name is a reader that cannot describe the database it was pointed
// at.
func quoteMySQLIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
