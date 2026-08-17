package crdbttl

import (
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
)

// FromReloptions reads a table's TTL configuration out of the storage
// parameters pg_class.reloptions reports, and returns nil for a table that has
// none.
//
// reloptions is where CockroachDB actually keeps this, and finding that out is
// half of what makes the read possible. Measured on v26.2.5, connected to the
// owning database:
//
//	SELECT reloptions FROM pg_class WHERE relname = 't1'
//	{ttl='on',ttl_expiration_expression='expires_at',schema_locked=true}
//
// Three properties of that answer decide this function's shape.
//
// The query must run INSIDE the owning database. pg_class is per-database on
// CockroachDB as it is on PostgreSQL, so the same statement against defaultdb
// returns no row at all -- not an empty reloptions, no row -- which reads as
// "this table has no TTL" and is how a wrong connection would silently erase a
// policy from a description.
//
// The array carries parameters that have nothing to do with TTL, and which ones
// depend on the line: v26.2.5 adds `schema_locked=true` to EVERY table, and
// v25.4.14 adds nothing. So this selects the `ttl_*` keys it knows rather than
// consuming the array, and an unknown key is left alone rather than refused --
// a server that adds a storage parameter must not break a read.
//
// Nothing here parses `crdb_internal`. Access to it is RESTRICTED on v26.2.5,
// which answers `Access to crdb_internal and system is restricted` and suggests
// a session variable it calls not recommended, so a reader built on it would
// fail for exactly the accounts a schema tool runs as. `SHOW CREATE TABLE`
// carries the same information as text; reloptions carries it as data.
func FromReloptions(reloptions []string) *ast.RowTTLSpec {
	spec := &ast.RowTTLSpec{}
	for _, option := range reloptions {
		name, value, ok := strings.Cut(option, "=")
		if !ok {
			continue
		}
		assign(spec, strings.ToLower(strings.TrimSpace(name)), unquote(strings.TrimSpace(value)))
	}
	if spec.IsZero() {
		return nil
	}
	return spec
}

// assign writes one storage parameter into the spec, ignoring every name this
// package does not model.
//
// The refused parameters are ignored here rather than reported. A read
// describes what is there, and a table someone configured outside Ptah with
// ttl_expire_after is not a declaration to refuse -- it is state Ptah does not
// model, and the comparator's own gate is what keeps it from being planned
// against. ValidateDeclared is where a DECLARATION of one is refused.
func assign(spec *ast.RowTTLSpec, name, value string) {
	switch name {
	case ExpirationExpressionParameter:
		spec.ExpirationExpression = value
	case ExpireAfterParameter:
		spec.ExpireAfter = value
	case JobCronParameter:
		spec.JobCron = value
	case SelectBatchSizeParameter:
		spec.SelectBatchSize = parseInt(value)
	case DeleteBatchSizeParameter:
		spec.DeleteBatchSize = parseInt(value)
	case SelectRateLimitParameter:
		spec.SelectRateLimit = parseInt(value)
	case DeleteRateLimitParameter:
		spec.DeleteRateLimit = parseInt(value)
	case PauseParameter:
		spec.Pause = parseBool(value)
	case LabelMetricsParameter:
		spec.LabelMetrics = parseBool(value)
	case DisableChangefeedReplicationParameter:
		spec.DisableChangefeedReplication = parseBool(value)
	}
}

// unquote reads the value half of one storage parameter.
//
// CockroachDB writes a string parameter in one of TWO forms, and which one it
// picks depends on the value, so a decoder that knows only the first silently
// corrupts an expression containing a quote -- which is not an exotic case,
// since `expires_at + INTERVAL '1 day'` is the shape the engine's own
// documentation uses. Measured on v26.2.5, reading the array element back with
// `SELECT unnest(reloptions)`:
//
//	declared expires_at                    element ttl_expiration_expression='expires_at'
//	declared expires_at + INTERVAL '1 day' element ttl_expiration_expression=e'expires_at + INTERVAL \'1 day\''
//
// The second is an escape-string literal: the value is delimited by single
// quotes, prefixed with e, and an embedded quote is BACKSLASH-escaped rather
// than doubled. So a plain literal carries no escapes at all -- any quote in
// the value forces the e form -- and the e literal is unescaped by removing one
// backslash before each escaped character.
//
// A value that is not quoted at all (every numeric and boolean knob, and
// `schema_locked=true`) is returned as it stands.
func unquote(value string) string {
	value = stripTypeAnnotation(value)
	escaped := strings.HasPrefix(value, "e'")
	if escaped {
		value = value[1:]
	}
	if len(value) < 2 || !strings.HasPrefix(value, "'") || !strings.HasSuffix(value, "'") {
		return value
	}
	value = value[1 : len(value)-1]
	if !escaped {
		return value
	}
	return unescape(value)
}

// unescape removes one level of backslash escaping, which is what the escape
// -string form carries. A trailing lone backslash cannot appear in a well-formed literal and
// is kept rather than dropped, so a malformed value round-trips as itself
// instead of losing a character.
func unescape(value string) string {
	var out strings.Builder
	out.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) {
			i++
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

// parseInt returns nil for a value that is not an integer, which leaves the
// knob undeclared rather than recording a zero nobody set.
func parseInt(value string) *int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

// parseBool returns nil for a value that is not a boolean AND for false.
//
// The server never writes a false boolean -- measured, it stores the parameter
// nowhere at all -- so this branch is defensive rather than load-bearing. It is
// here to make the invariant one sentence: a boolean present in a spec is
// always true, on both sides of every comparison, however the value arrived.
func parseBool(value string) *bool {
	parsed, err := strconv.ParseBool(value)
	if err != nil || !parsed {
		return nil
	}
	return &parsed
}

// stripTypeAnnotation removes the `:::TYPE` suffix CockroachDB writes on a
// typed storage parameter.
//
// Only one parameter carries it, and only because its value is not a string:
// measured on v26.2.5, `ttl_expire_after = '3 days'` is stored as the element
// `ttl_expire_after='3 days':::INTERVAL`, while every string-valued parameter
// beside it is stored with no annotation at all. Leaving it on would make the
// value differ from anything a declaration could write, so the parameter would
// never compare equal to itself.
//
// The suffix is removed rather than parsed: what type the server chose is not
// something Ptah models, and the value in front of it is the whole of what a
// comparison needs.
func stripTypeAnnotation(value string) string {
	annotation := strings.LastIndex(value, ":::")
	if annotation < 0 {
		return value
	}
	return value[:annotation]
}
