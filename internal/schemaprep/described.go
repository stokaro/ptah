package schemaprep

import (
	"sync"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// described is what preparation reads of a dialect's descriptor.
type described struct {
	name             string
	preparation      descriptor.Preparation
	identifiers      descriptor.IdentifierLimit
	checkNaming      descriptor.CheckNaming
	constraintNaming descriptor.ConstraintNaming
}

// describedDialects caches what [describe] found. A spelling keeps the answer
// the process registry first gave it for the life of the process
// (platform.Register only adds), so a found answer never goes stale. A miss is
// not cached: a later registration may describe the spelling.
var describedDialects sync.Map

// describe returns what preparation reads of dialect's descriptor, and the
// zero value for a dialect the process registry does not describe, which
// prepares every declaration as written. Preparation runs once per field and
// constraint, so the answer is cached rather than copied out of the registry
// each time; the cached value is shared and read only.
func describe(dialect string) described {
	if cached, ok := describedDialects.Load(dialect); ok {
		if answer, ok := cached.(described); ok {
			return answer
		}
	}
	found, ok := platform.Lookup(dialect)
	if !ok {
		return described{}
	}
	answer := described{
		name:             found.Name,
		preparation:      found.Preparation,
		identifiers:      found.IdentifierLimit,
		checkNaming:      found.CheckNaming,
		constraintNaming: found.ConstraintNaming,
	}
	describedDialects.Store(dialect, answer)
	return answer
}
