package devclean

import (
	"context"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/selectext"
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
// Common objects are matched by identity, not by definition. Selected feature
// owners may subtract individual members, such as rights in one ACL. The default
// schema resolves unqualified common objects and table children; global owner
// state stays global. Selection failures return an error and no partial catalog.
func (b Baseline) WithoutStartingPoint(ctx context.Context, current, declared *catalog.Database, defaultSchema string, selections selectext.Set) (*catalog.Database, error) {
	return WithoutKeptState(ctx, current, b.environment, declared, defaultSchema, selections)
}

// WithoutKeptState is [Baseline.WithoutStartingPoint] for a caller that holds
// the starting point as a read: it returns current without the objects env
// holds that declared does not hold too. A nil env returns current as it is,
// and a nil declared declares nothing. current is not changed.
func WithoutKeptState(ctx context.Context, current, env, declared *catalog.Database, defaultSchema string, selections selectext.Set) (*catalog.Database, error) {
	if current == nil || env == nil {
		return current, nil
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
	filtered.Roles = subtract(current.Roles, env.Roles, declared.Roles,
		func(v catalog.Role) string { return v.Name })
	filtered.RoleMemberships = subtract(current.RoleMemberships, env.RoleMemberships, declared.RoleMemberships,
		func(v catalog.RoleMembership) string { return v.Role + "\x00" + v.Member })
	var err error
	filtered.FeatureObjects, err = selections.SubtractBaseline(ctx, selectext.BaselineRequest{
		Current: current.FeatureObjects, StartingPoint: env.FeatureObjects, Declared: declared.FeatureObjects, DefaultSchema: defaultSchema,
	})
	if err != nil {
		return nil, err
	}
	return &filtered, nil
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
