package devclean

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// WithoutStartingPoint returns current, a read of a dev database the caller
// claimed with this baseline, without the objects the dev database's starting
// point held that declared does not hold too. It applies to a dev database an
// atlas.hcl docker block provisioned, and returns current as it is for any
// other.
//
// The starting point is the dev database's, as its extensions are: the
// Supabase image's auth, storage and realtime schemas, their tables,
// functions, triggers and policies, and the grants on them. A desired schema
// does not declare them, and a comparison that reads them as the replay's
// state plans a migration dropping every one (stokaro/ptah#4056). What the run
// added is left in, wherever it is: a trigger a migration puts on auth.users
// is compared like any other, though its table is not. An object declared
// holds too stays and matches.
//
// Objects are matched by identity, not by definition: one the run changed
// inside the starting point is left out with it. defaultSchema names the
// schema an object with none is in, on either side.
func (b Baseline) WithoutStartingPoint(current, declared *catalog.Database, defaultSchema string) *catalog.Database {
	return WithoutKeptState(current, b.environment, declared, defaultSchema)
}

// WithoutKeptState is [Baseline.WithoutStartingPoint] for a caller that holds
// the starting point as a read: it returns current without the objects env
// holds that declared does not hold too. A nil env returns current as it is,
// and a nil declared declares nothing. current is not changed.
func WithoutKeptState(current, env, declared *catalog.Database, defaultSchema string) *catalog.Database {
	if current == nil || env == nil {
		return current
	}
	if declared == nil {
		declared = &catalog.Database{}
	}
	q := func(schema, name string) string {
		if strings.TrimSpace(schema) == "" {
			schema = defaultSchema
		}
		return schema + "\x00" + name
	}
	filtered := *current
	filtered.Schemas = subtract(current.Schemas, env.Schemas, declared.Schemas,
		func(v catalog.Schema) string { return v.Name })
	filtered.Tables = subtract(current.Tables, env.Tables, declared.Tables,
		func(v catalog.Table) string { return q(v.Schema, v.Name) })
	filtered.Views = subtract(current.Views, env.Views, declared.Views,
		func(v catalog.View) string { return q(v.Schema, v.Name) })
	filtered.MatViews = subtract(current.MatViews, env.MatViews, declared.MatViews,
		func(v catalog.MaterializedView) string { return q(v.Schema, v.Name) })
	filtered.Sequences = subtract(current.Sequences, env.Sequences, declared.Sequences,
		func(v catalog.Sequence) string { return q(v.Schema, v.Name) })
	filtered.Functions = subtract(current.Functions, env.Functions, declared.Functions,
		func(v catalog.Function) string { return q(v.Schema, v.Name) })
	filtered.Enums = subtract(current.Enums, env.Enums, declared.Enums,
		func(v catalog.Enum) string { return q(v.Schema, v.Name) })
	filtered.Domains = subtract(current.Domains, env.Domains, declared.Domains,
		func(v catalog.Domain) string { return q(v.Schema, v.Name) })
	filtered.Composites = subtract(current.Composites, env.Composites, declared.Composites,
		func(v catalog.CompositeType) string { return q(v.Schema, v.Name) })
	filtered.Ranges = subtract(current.Ranges, env.Ranges, declared.Ranges,
		func(v catalog.Range) string { return q(v.Schema, v.Name) })
	filtered.Indexes = subtract(current.Indexes, env.Indexes, declared.Indexes,
		func(v catalog.Index) string { return q(v.Schema, v.TableName) + "\x00" + v.Name })
	filtered.Constraints = subtract(current.Constraints, env.Constraints, declared.Constraints,
		func(v catalog.Constraint) string {
			return q(v.Schema, v.TableName) + "\x00" + v.Name + "\x00" + v.ColumnName
		})
	filtered.Triggers = subtract(current.Triggers, env.Triggers, declared.Triggers,
		func(v catalog.Trigger) string { return q(v.Schema, v.Table) + "\x00" + v.Name })
	filtered.RLSPolicies = subtract(current.RLSPolicies, env.RLSPolicies, declared.RLSPolicies,
		func(v catalog.RLSPolicy) string { return v.Table + "\x00" + v.Name })
	filtered.Grants = subtract(current.Grants, env.Grants, declared.Grants,
		func(v catalog.Grant) string {
			return strings.Join([]string{v.Role, strings.ToUpper(v.Privilege), strings.ToUpper(v.ObjectType),
				q(v.Schema, v.ObjectName), v.Arguments, v.Column}, "\x00")
		})
	filtered.ObjectOwners = subtract(current.ObjectOwners, env.ObjectOwners, declared.ObjectOwners,
		func(v catalog.ObjectOwner) string { return v.Kind + "\x00" + q(v.Schema, v.Name) })
	filtered.Extensions = subtract(current.Extensions, env.Extensions, declared.Extensions,
		func(v catalog.Extension) string { return v.Name })
	filtered.DefaultPrivileges = subtract(current.DefaultPrivileges, env.DefaultPrivileges, declared.DefaultPrivileges,
		func(v catalog.DefaultPrivilege) string {
			return strings.Join([]string{v.Grantor, v.Schema, strings.ToUpper(v.ObjectType), v.Grantee,
				strings.ToUpper(v.Privilege)}, "\x00")
		})
	filtered.Roles = subtract(current.Roles, env.Roles, declared.Roles,
		func(v catalog.Role) string { return v.Name })
	filtered.RoleMemberships = subtract(current.RoleMemberships, env.RoleMemberships, declared.RoleMemberships,
		func(v catalog.RoleMembership) string { return v.Role + "\x00" + v.Member })
	// Named feature objects, such as TimescaleDB continuous aggregates, are
	// matched by their structured identity; settings attached to a table
	// leave with the table above.
	filtered.FeatureObjects = subtractObjects(current.FeatureObjects, env.FeatureObjects, declared.FeatureObjects, func(ref objectidentity.ID) string {
		return strings.Join([]string{string(ref.Kind), ref.Catalog.Normalized, q(ref.Schema.Authored(), ref.Parent.Normalized),
			ref.Name.Normalized, ref.Signature}, "\x00")
	})
	return &filtered
}

// subtractObjects returns the feature objects of current whose identity env
// does not hold, or declared holds too. key resolves an unqualified identity
// under the caller's default schema, as every other family here does: the
// identity a reader builds fills in its own default, which need not be the
// schema the dev database uses.
func subtractObjects(current, env, declared schemaext.Objects, key func(objectidentity.ID) string) schemaext.Objects {
	if env.Len() == 0 || current.Len() == 0 {
		return current
	}
	inEnv := make(map[string]bool, env.Len())
	for _, ref := range env.Refs() {
		inEnv[key(ref)] = true
	}
	inDeclared := make(map[string]bool, declared.Len())
	for _, ref := range declared.Refs() {
		inDeclared[key(ref)] = true
	}
	return current.Select(func(ref objectidentity.ID) bool { return !inEnv[key(ref)] || inDeclared[key(ref)] })
}

// subtract returns the values of current whose key env does not hold, or
// declared holds too.
func subtract[T any](current, env, declared []T, key func(T) string) []T {
	if len(env) == 0 || len(current) == 0 {
		return current
	}
	inEnv := make(map[string]bool, len(env))
	for _, value := range env {
		inEnv[key(value)] = true
	}
	inDeclared := make(map[string]bool, len(declared))
	for _, value := range declared {
		inDeclared[key(value)] = true
	}
	kept := make([]T, 0, len(current))
	for _, value := range current {
		k := key(value)
		if inEnv[k] && !inDeclared[k] {
			continue
		}
		kept = append(kept, value)
	}
	return kept
}
