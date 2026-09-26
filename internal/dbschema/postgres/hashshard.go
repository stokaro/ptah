package postgres

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
)

// A CockroachDB key or index built USING HASH spans a column the author never
// declared. Measured on v25.4.16, v26.2.7 and v26.3.1,
//
//	CREATE TABLE hs8 (id INT8, v INT8, PRIMARY KEY (id) USING HASH WITH (bucket_count = 8))
//
// adds `crdb_internal_id_shard_8 INT8 NOT VISIBLE NOT NULL AS (...) VIRTUAL`,
// which pg_attribute reports with attishidden = t, and pg_constraint reports
// the key with conkey {3,1}: the shard column first, although its attnum is
// last. A secondary `INDEX (v) USING HASH` gets its own shard column the same
// way. The server's own definitions name only the declared columns:
// pg_get_constraintdef answers `PRIMARY KEY (id ASC) USING HASH WITH
// (bucket_count=8)`, and pg_get_indexdef ends in the same clause.
//
// The column read leaves every hidden column out, so a key described over the
// shard column names a column the description does not have, and applying it
// fails with `column "crdb_internal_id_shard_8" does not exist`
// (stokaro/ptah#3771). The functions here keep the key as declared and record
// the bucket count beside it.

// visibleKeyColumn is the condition a constraint's key column has to meet to be
// listed, over the alias of its pg_attribute row: on a target with hidden
// columns, that it is not one. It extends a FILTER clause that already requires
// the column to exist, so it starts with AND.
func (r *Reader) visibleKeyColumn(alias string) string {
	if !r.hasHiddenColumns() {
		return ""
	}
	return " AND NOT " + hiddenAttribute(alias)
}

// indexKeyHiddenExpr projects one flag per index key, true where the key is a
// hidden column, in key order. It covers the key columns only, as the other
// per-key projections do, so the flags line up with them position for
// position. A target without hidden columns gets an empty array, which
// [withoutHiddenKeys] reads as "no key is hidden".
func (r *Reader) indexKeyHiddenExpr() string {
	if !r.hasHiddenColumns() {
		return "'[]'"
	}
	return `COALESCE((
				SELECT json_agg(` + hiddenAttribute("keyatt") + ` ORDER BY keys.ordinality)::text
				FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ordinality)
				LEFT JOIN pg_attribute keyatt
					ON keyatt.attrelid = ix.indrelid
					AND keyatt.attnum = keys.attnum
				WHERE keys.ordinality <= ix.indnkeyatts
			), '[]')`
}

// withoutHiddenKeys drops the keys flagged hidden from an index's columns and
// from its parts, which are aligned with the columns when present. An empty
// flag list, or one whose length does not match the columns, leaves both as
// they are: the flags then say nothing about these keys.
func withoutHiddenKeys(
	columns []string,
	parts []catalog.IndexPart,
	hiddenJSON string,
) ([]string, []catalog.IndexPart, error) {
	trimmed := strings.TrimSpace(hiddenJSON)
	if trimmed == "" || trimmed == "[]" {
		return columns, parts, nil
	}
	var hidden []bool
	if err := json.Unmarshal([]byte(trimmed), &hidden); err != nil {
		return nil, nil, err
	}
	if len(hidden) != len(columns) {
		return columns, parts, nil
	}
	keepParts := len(parts) == len(columns)
	keptColumns := make([]string, 0, len(columns))
	var keptParts []catalog.IndexPart
	for position, column := range columns {
		if hidden[position] {
			continue
		}
		keptColumns = append(keptColumns, column)
		if keepParts {
			keptParts = append(keptParts, parts[position])
		}
	}
	if !keepParts {
		keptParts = parts
	}
	return keptColumns, keptParts, nil
}

// hashShardClause is the clause CockroachDB prints after the key list of a key
// or index built USING HASH.
var hashShardClause = regexp.MustCompile(`USING HASH WITH \(bucket_count\s*=\s*(\d+)\)`)

// hashShardBuckets reads the bucket count out of a definition the server
// printed, and answers 0 for anything else.
//
// It is gated on the dialect because PostgreSQL has a hash access method of its
// own, which pg_get_indexdef prints as `USING hash (v)`. That is a different
// index, and it never carries bucket_count, but the gate keeps the question off
// the engines that have no sharding at all.
func hashShardBuckets(dialect, definition string) int {
	if dialect != platform.CockroachDB {
		return 0
	}
	match := hashShardClause.FindStringSubmatch(definition)
	if match == nil {
		return 0
	}
	buckets, err := strconv.Atoi(match[1])
	if err != nil {
		return 0
	}
	return buckets
}
