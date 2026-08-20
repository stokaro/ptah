// Package chrefresh converts a ClickHouse materialized-view refresh schedule
// into the form the server stores, and refuses the schedules it will not keep.
//
// It exists because ClickHouse does not store a schedule as written. Every fact
// below was measured against a live clickhouse-server 26.7.3.19 -- the version
// docker-compose.yaml pins -- rather than read off a manual, and each is a row
// of TestCanonical_ReproducesWhatTheServerStored.
//
// # Why a canonicalizer rather than a text comparison
//
// The schedule survives only in system.tables.create_table_query, and the
// server rewrites the interval before storing it. A declaration of
// `EVERY 60 MINUTE` reads back as `EVERY 1 HOUR`, so comparing the two as text
// finds a difference on every run -- and the plan that difference produces is a
// drop and a create, on an object whose drop takes every row it accumulated.
// Canonical must reproduce the stored spelling exactly, or such a declaration
// can never converge.
//
// This is the same problem [go.5x5.cz/ptah/internal/crdbduration] solves for
// CockroachDB, and it is solved the same way and for the same reason.
//
// # The two unit ladders
//
// Clock units are summed into seconds and decomposed greedily, largest first,
// emitting only non-zero terms:
//
//	declared                    stored
//	EVERY 1 SECOND              EVERY 1 SECOND
//	EVERY 60 SECOND             EVERY 1 MINUTE
//	EVERY 90 SECOND             EVERY 1 MINUTE 30 SECOND
//	EVERY 100 SECOND            EVERY 1 MINUTE 40 SECOND
//	EVERY 3600 SECOND           EVERY 1 HOUR
//	EVERY 3661 SECOND           EVERY 1 HOUR 1 MINUTE 1 SECOND
//	EVERY 86400 SECOND          EVERY 1 DAY
//	EVERY 604800 SECOND         EVERY 1 WEEK
//	EVERY 120 MINUTE            EVERY 2 HOUR
//	EVERY 24 HOUR               EVERY 1 DAY
//	EVERY 7 DAY                 EVERY 1 WEEK
//	EVERY 10 DAY                EVERY 1 WEEK 3 DAY
//	EVERY 14 DAY                EVERY 2 WEEK
//	EVERY 30 DAY                EVERY 4 WEEK 2 DAY
//	EVERY 1 MINUTE 90 SECOND    EVERY 2 MINUTE 30 SECOND
//	EVERY 5 WEEK                EVERY 5 WEEK
//
// A week is the largest clock unit: five weeks stay five weeks rather than
// becoming a month, because a month has no fixed length in seconds.
//
// Calendar units are summed into months and decomposed the same way:
//
//	EVERY 1 QUARTER             EVERY 3 MONTH
//	EVERY 12 MONTH              EVERY 1 YEAR
//	EVERY 18 MONTH              EVERY 1 YEAR 6 MONTH
//
// The two ladders cannot be combined, and the server says so rather than
// picking a meaning:
//
//	EVERY 1 MONTH 1 DAY   Code: 36 ... Interval shouldn't contain both calendar units and clock units
//	EVERY 1 YEAR 1 HOUR   Code: 36 ... (the same)
//	EVERY 0 SECOND        Code: 36 ... Interval must be positive
//
// # The clauses around the interval
//
// AFTER normalizes its interval identically. The rest round-trip as written,
// in this order, which is also the order the server prints:
//
//	EVERY <interval> [OFFSET <interval>] [RANDOMIZE FOR <interval>] [DEPENDS ON <view>...] [APPEND]
//	AFTER <interval>                     [RANDOMIZE FOR <interval>] [DEPENDS ON <view>...] [APPEND]
//
// OFFSET belongs to EVERY alone: `AFTER 1 HOUR OFFSET 5 MINUTE` is answered
// with `Code: 62 ... Syntax error ... failed at position 52 (OFFSET)`.
//
// DEPENDS ON is the one clause whose text the server changes for a reason other
// than the interval: a dependency is stored schema-qualified, so
// `DEPENDS ON mv_every` reads back as `DEPENDS ON ptah_test.mv_every`.
package chrefresh
