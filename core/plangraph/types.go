// Package plangraph orders owner-contributed operations and validates their
// declared object effects. It knows neither SQL nor concrete feature payloads.
package plangraph

import (
	"errors"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

var (
	// ErrInvalid identifies malformed steps, effects, or dependencies.
	ErrInvalid = errors.New("invalid plan graph")
	// ErrConflict identifies competing emitters or unordered object effects.
	ErrConflict = errors.New("conflicting plan effects")
	// ErrCycle identifies a dependency cycle. No partial plan is returned.
	ErrCycle = errors.New("plan dependency cycle")
)

// StepID identifies one semantic operation within an owner's contribution.
// Owner and Name are separate components; neither is parsed as a compound key.
type StepID struct {
	Owner string
	Name  string
}

// Action describes an operation's effect on an individual object. Reads may
// cross owner boundaries; every writer of one object must have the same owner.
type Action string

const (
	// Read requires an object without changing its definition.
	Read Action = "read"
	// Create establishes an object absent before this operation.
	Create Action = "create"
	// Alter changes an existing object's definition.
	Alter Action = "alter"
	// Drop removes an existing object.
	Drop Action = "drop"
)

// Effect identifies one object's use by a step. Subject retains display spelling
// and normalized identity. A step declares each subject at most once; a compound
// replacement within one step is Alter, not separate Create and Drop entries.
type Effect struct {
	Subject objectidentity.ID
	Action  Action
}

// Transaction describes the owner's execution requirement for an indivisible
// step. Ordering never combines steps into a transaction or infers atomicity.
type Transaction string

const (
	// TransactionUnknown supplies no transaction assessment, including at zero value.
	TransactionUnknown Transaction = ""
	// TransactionAllowed permits either transactional or unwrapped execution.
	TransactionAllowed Transaction = "allowed"
	// TransactionRequired requires one transaction for the complete step.
	TransactionRequired Transaction = "required"
	// TransactionForbidden requires execution outside a transaction.
	TransactionForbidden Transaction = "forbidden"
)

// Step is one indivisible ordering unit. Payload is interpreted by its owner;
// a scheduler never renders or executes it. Empty Effects means no footprint
// was supplied, not proof that the operation changes nothing. Consumers must
// retain unknown footprints and transaction requirements as unknown.
//
// Payloads must remain immutable while scheduling and while a returned plan is
// used. Schedule copies metadata slices but does not clone arbitrary payloads.
type Step[T any] struct {
	ID          StepID
	Payload     T
	Effects     []Effect
	Transaction Transaction
	// Impact retains the owner's conservative safety assessment. Its zero value
	// is unknown; scheduling never infers safety from the action or payload.
	Impact schemaext.Effect
}

// Dependency requires Before to finish before After may start. Both steps must
// exist in the complete graph. Repeated identical dependencies are harmless.
type Dependency struct {
	Before StepID
	After  StepID
}

// Contribution supplies an owner's steps and their ordering requirements.
// Every step's ID must name Owner. A dependency may name another owner's step
// so feature and common operations participate in the same ordering.
type Contribution[T any] struct {
	Owner        string
	Steps        []Step[T]
	Dependencies []Dependency
}

// Plan contains the complete deterministic order and its explicit dependencies.
// A failed or canceled Schedule returns the zero Plan, never a usable prefix.
type Plan[T any] struct {
	Steps        []Step[T]
	Dependencies []Dependency
}
