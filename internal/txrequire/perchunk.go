package txrequire

import "strings"

// PerChunkIndexBuild reports whether words, a statement in the shape this
// package and migration/lint tokenize to, is a CREATE INDEX that sets
// TimescaleDB's `transaction_per_chunk` storage parameter to true.
//
// Two callers need the answer, and they must agree, which is why it is one
// predicate here rather than a scan in each. The linter reads it to leave the
// build out of the blocking-index rule, because it is TimescaleDB's remedy for
// that lock, and to report it in a transactional migration. [Analyze] reads it
// to refuse that migration before it reaches the server. A statement one of
// them treated as a per-chunk build and the other did not would be reported
// as safe and then refused, or refused and never explained.
//
// Measured on TimescaleDB 2.30.1 over PostgreSQL 18.6:
//
//	WITH (timescaledb.transaction_per_chunk)      one chunk at a time
//	... = t, tru, yes, on, 1                      one chunk at a time
//	... = false, off, 0                           the plain build's lock on the
//	                                              hypertable and every chunk
//	... = o, maybe                                refused: not a valid bool
//	the same statement inside BEGIN ... COMMIT    refused: cannot run inside a
//	                                              transaction block (25001)
//	the parameter on an ordinary table            refused: unrecognized
//	                                              parameter namespace
//
// Only a true value counts, because the false spellings build like a plain
// index and must keep being reported as one.
func PerChunkIndexBuild(words []string) bool {
	if !hasPrefixWords(words, "CREATE", "INDEX") && !hasPrefixWords(words, "CREATE", "UNIQUE", "INDEX") {
		return false
	}
	// The storage parameters are the only WITH ( a CREATE INDEX can carry: an
	// index expression or predicate admits no subquery, so no WITH clause of
	// a query can appear inside one, and a column named with is quoted.
	for i := range words {
		if words[i] == "WITH" && i+1 < len(words) && words[i+1] == "(" {
			return storageParameterTrue(words, i+2, "TIMESCALEDB", "TRANSACTION_PER_CHUNK")
		}
	}
	return false
}

// storageParameterTrue reads the storage parameter list that starts at
// words[start] and reports whether it sets namespace.name to a true value. A
// parameter written without a value is true, as PostgreSQL reads it.
func storageParameterTrue(words []string, start int, namespace, name string) bool {
	for i := start; i < len(words) && words[i] != ")"; {
		matches := i+2 < len(words) &&
			foldIdentifier(words[i]) == namespace && words[i+1] == "." && foldIdentifier(words[i+2]) == name
		next := i + 1
		for next < len(words) && words[next] != "," && words[next] != ")" {
			next++
		}
		if matches {
			return storageParameterValueTrue(words[i+3 : next])
		}
		i = next
		if i < len(words) && words[i] == "," {
			i++
		}
	}
	return false
}

// storageParameterValueTrue reads the words after a parameter's name: nothing,
// or `=` and a value.
func storageParameterValueTrue(rest []string) bool {
	if len(rest) == 0 {
		return true
	}
	if len(rest) != 2 || rest[0] != "=" {
		return false
	}
	return postgresBoolTrue(strings.Trim(rest[1], "'"))
}

// postgresBoolTrue reports whether PostgreSQL's boolean input reads value as
// true: `on`, `1`, and `true` or `yes` or any prefix of either, in any case. A
// value the server refuses reads as false, so the statement is treated as the
// plain build and the server reports the rest.
func postgresBoolTrue(value string) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		return false
	}
	if v == "on" || v == "1" {
		return true
	}
	// A prefix of the word, however short: parse_bool reads `t` as true.
	for _, word := range []string{"true", "yes"} {
		if len(v) <= len(word) && word[:len(v)] == v {
			return true
		}
	}
	return false
}

// foldIdentifier compares an identifier the way migration/lint does: quotes
// dropped and the case folded.
func foldIdentifier(word string) string {
	return strings.ToUpper(strings.Trim(word, "`\""))
}
