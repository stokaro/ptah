package devclean

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgpublication"
)

// artifactPublication is the kind a PostgreSQL writer gives a publication
// among the database-scoped objects it lists.
const artifactPublication = "publication"

// Environment names, by name, the objects of two kinds a dev database holds
// for the whole database and a desired schema may also declare: its
// extensions and its publications. A baseline's environment is what the dev
// database held when the run claimed it; a declared one is what the other side
// of a comparison holds. The zero value names nothing.
type Environment struct {
	Extensions   []string
	Publications []string
}

// IsZero reports whether e names nothing.
func (e Environment) IsZero() bool { return len(e.Extensions) == 0 && len(e.Publications) == 0 }

// Union returns the names e or other holds, sorted, each once.
func (e Environment) Union(other Environment) Environment {
	union := func(a, b []string) []string {
		names := slices.Concat(a, b)
		slices.Sort(names)
		return slices.Compact(names)
	}
	return Environment{Extensions: union(e.Extensions, other.Extensions), Publications: union(e.Publications, other.Publications)}
}

// Environment returns what the baseline keeps of the dev database's
// environment: the extensions it held, and the publications, which the
// claim listed among its database-scoped objects.
func (b Baseline) Environment() Environment {
	var publications []string
	for _, artifact := range b.artifacts {
		if artifact.Kind == artifactPublication {
			publications = append(publications, artifact.Name)
		}
	}
	return Environment{Extensions: slices.Clone(b.extensions), Publications: publications}
}

// DeclaredEnvironment is what desired declares of an environment, as the
// extension and publication owners' objects. A nil desired declares nothing.
func DeclaredEnvironment(desired *schemamodel.Database) Environment {
	if desired == nil {
		return Environment{}
	}
	return Environment{Extensions: pgextension.Names(desired.FeatureObjects), Publications: pgpublication.Names(desired.FeatureObjects)}
}

// ReadEnvironment is [DeclaredEnvironment] for a read. A nil read holds
// nothing.
func ReadEnvironment(read *catalog.Database) Environment {
	if read == nil {
		return Environment{}
	}
	return Environment{Extensions: pgextension.Names(read.FeatureObjects), Publications: pgpublication.Names(read.FeatureObjects)}
}

// WithoutEnvironment returns current, a read of a dev database the caller
// claimed with this baseline, without the baseline's environment the other
// side of the comparison does not declare. declared is what that side holds.
//
// The baseline's extensions and publications are the dev database's
// environment, which every reset keeps: the dev database held them before the
// run, and the run did not create them. Left in, each is a difference from any
// desired schema that does not name it, and a migration planned against the
// replay drops it. The next replay then executes that drop on the dev
// database, and no reset can put the object back (stokaro/ptah#4070). One the
// other side declares is left in, and matches.
//
// current is not changed; the result shares everything but its feature
// objects. A nil current, or a baseline that keeps nothing, comes back as it
// is.
func (b Baseline) WithoutEnvironment(current *catalog.Database, declared Environment) *catalog.Database {
	return WithoutKept(current, b.Environment(), declared)
}

// WithoutKept is [Baseline.WithoutEnvironment] for a caller that holds the
// kept environment itself.
func WithoutKept(current *catalog.Database, kept, declared Environment) *catalog.Database {
	if current == nil || kept.IsZero() {
		return current
	}
	keep := func(kept, declared []string) func(string) bool {
		return func(name string) bool { return !slices.Contains(kept, name) || slices.Contains(declared, name) }
	}
	filtered := *current
	filtered.FeatureObjects = pgextension.Select(current.FeatureObjects, keep(kept.Extensions, declared.Extensions))
	filtered.FeatureObjects = pgpublication.Select(filtered.FeatureObjects, keep(kept.Publications, declared.Publications))
	return &filtered
}
