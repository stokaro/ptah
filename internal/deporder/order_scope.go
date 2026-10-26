package deporder

import (
	"slices"

	"ptah.run/core/platform/identifier"
)

// ViewLikesForCreateIn is [ViewLikesForCreateForDialect] with names resolving
// in the session semantics describe rather than in the dialect's default
// schema. Under `search_path=app`, a view reading `app.zbase` is created after
// a zbase declared without a schema; ordered by the dialect's default, public,
// it comes first and fails with `relation "app.zbase" does not exist`.
//
// A schema semantics do not know adds no edge: the order never guesses, for
// the reason [ViewLikesForCreateForDialect] gives.
func ViewLikesForCreateIn(objects []ViewLike, dialect string, semantics identifier.Semantics) []ViewLike {
	return viewLikesForCreate(objects, dialect, sessionScope(semantics))
}

// ViewLikesForDropIn is [ViewLikesForDropForDialect] with names resolving as
// [ViewLikesForCreateIn] resolves them, so for the edges it finds this order
// is the mirror of that one.
func ViewLikesForDropIn(objects []ViewLike, dialect string, semantics identifier.Semantics) []ViewLike {
	return viewLikesForDrop(objects, dialect, sessionScope(semantics))
}

// orderScope is where the creation order puts a name written without a
// schema. The schemas are folded as [foldName] folds them. It never guesses:
// a schema it does not know adds no edge.
type orderScope struct {
	// defaultSchema is where an object declared without a schema is created.
	defaultSchema string
	// path is every schema an unqualified name in a body may resolve in.
	path []string
}

// offlineScope is the dialect's own default schema, alone on the path: what a
// render with no connection knows.
func offlineScope(dialect string) orderScope {
	return knownScope(identifier.ForDialect(dialect).DefaultSchema, nil)
}

// sessionScope is the default schema and search path a session reported.
func sessionScope(semantics identifier.Semantics) orderScope {
	return knownScope(semantics.DefaultSchema, semantics.SearchPath)
}

// knownScope folds the schemas, and puts the default schema alone on a path
// nobody reported, because that is the one schema an unqualified name is
// known to reach.
func knownScope(defaultSchema string, path []string) orderScope {
	scope := orderScope{defaultSchema: foldName(defaultSchema)}
	for _, schema := range path {
		scope.path = append(scope.path, foldName(schema))
	}
	if len(scope.path) == 0 && scope.defaultSchema != "" {
		scope.path = []string{scope.defaultSchema}
	}
	return scope
}

// resolution is the scope as [namesRead] asks it.
func (s orderScope) resolution() resolution {
	return resolution{
		relationIn: func(schema string) bool {
			return s.defaultSchema != "" && foldName(schema) == s.defaultSchema
		},
		nameReaches: func(schema string) bool {
			return slices.Contains(s.path, foldName(schema))
		},
	}
}
