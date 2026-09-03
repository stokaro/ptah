package embedpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/embedgen"
)

// Relation is one physical PostgreSQL relation, named the way the server names
// it rather than the way a specification spelled it.
//
// Both halves are always set on a resolved value: PostgreSQL has no unqualified
// relation, only an unqualified spelling that a search_path turns into one.
type Relation struct {
	// Schema is the namespace the relation is in.
	Schema string
	// Table is the relation's own name.
	Table string
}

// relationReader is the one method resolution needs, so a *sql.DB, a *sql.Conn
// and a *sql.Tx all answer it.
//
// The connection matters rather than being an implementation detail: an
// unqualified spelling resolves through the SESSION's search_path, so a
// resolution taken on one connection is not a statement about another. Every
// caller passes the handle it is about to use.
type relationReader interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// resolveRelationSQL asks the server which relation a spelling names.
//
// The name is composed with format('%I.%I') on the server rather than
// concatenated here, because quoting an identifier correctly is the server's
// job and to_regclass applies ordinary parsing rules to what it is given: an
// unquoted `Docs` would fold to `docs` and find the wrong relation, or none.
//
// to_regclass answers NULL rather than raising for a name that resolves to
// nothing, which is what lets a caller tell "no such relation" from an error.
const resolveRelationSQL = `
	SELECT n.nspname, c.relname
	FROM pg_catalog.pg_class c
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.oid = to_regclass(
		CASE WHEN $1 = '' THEN format('%I', $2::text)
		     ELSE format('%I.%I', $1::text, $2::text)
		END)`

// ResolveRelation answers the physical relation a specification's schema and
// table name, resolving an omitted schema through the session's search_path.
//
// It exists because an authored spelling is not an identity. Both
// `table: docs` and `schema: public, table: docs` name one pg_class row when
// search_path is `public`, and Ptah derived a separate source identity, outbox
// table, advisory lock and target pointer from each -- so one physical source
// had two outboxes and two lifecycle domains that did not know about each
// other (stokaro/ptah#2806).
//
// Replacing an empty schema with `public` while parsing would be wrong: the
// database may carry a search_path that resolves it somewhere else entirely,
// and the answer belongs to the connected session rather than to the document.
//
// A spelling that names no relation is returned unchanged, with found false.
// Nothing durable should be created for a source that is not there, and the
// verb that needs it reports its absence in its own words; resolving is not
// the place to decide that.
func ResolveRelation(
	ctx context.Context, db relationReader, schema, table string,
) (relation Relation, found bool, err error) {
	authored := Relation{Schema: strings.TrimSpace(schema), Table: strings.TrimSpace(table)}
	if authored.Table == "" {
		return authored, false, nil
	}
	var resolved Relation
	err = db.QueryRowContext(ctx, resolveRelationSQL, authored.Schema, authored.Table).
		Scan(&resolved.Schema, &resolved.Table)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return authored, false, nil
		}
		return authored, false, fmt.Errorf("resolve relation %s: %w", authored, err)
	}
	return resolved, true, nil
}

// String is the qualified name, for a diagnostic that has to say which
// relation it means.
func (r Relation) String() string {
	if r.Schema == "" {
		return r.Table
	}
	return r.Schema + "." + r.Table
}

// WithResolvedRelations answers a copy of spec whose source and target name the
// physical relations this server resolves them to.
//
// It is for deriving a PHYSICAL identity -- the outbox table's name, the
// source-scoped lock, a target pointer -- and for nothing else. In particular
// it must not reach [embedgen.Spec.Identity]: that digest is a content address
// of the authored document, computed by `ptah inference describe` with no
// database at all, and making it depend on a session's search_path would give
// one specification two identities depending on where it was read.
//
// That separation is the answer to stokaro/ptah#2806 rather than a compromise
// in it. Two spellings of one relation are allowed to be two generations --
// they are two documents -- but they must share one outbox, one floor, one
// lock and one pointer, because those name a thing in the database rather than
// a thing in the document.
func WithResolvedRelations(
	ctx context.Context, db relationReader, spec embedgen.Spec,
) (embedgen.Spec, error) {
	source, _, err := ResolveRelation(ctx, db, spec.Source.Schema, spec.Source.Table)
	if err != nil {
		return spec, err
	}
	target, _, err := ResolveRelation(ctx, db, spec.Target.Schema, spec.Target.Table)
	if err != nil {
		return spec, err
	}
	resolved := spec
	resolved.Source.Schema, resolved.Source.Table = source.Schema, source.Table
	resolved.Target.Schema, resolved.Target.Table = target.Schema, target.Table
	return resolved, nil
}
