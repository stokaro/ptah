// Package pgtable owns PostgreSQL table persistence, partition keys and their schema contracts.
package pgtable

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// Owner identifies the provider of PostgreSQL table features.
const Owner = "ptah.run/postgres/table"

// PersistenceKind identifies whether a PostgreSQL table skips the write-ahead log.
const PersistenceKind schemaext.Kind = "ptah.run/postgres/table-persistence"

// Persistence describes a table's write-ahead logging. The zero value is logged.
// An explicit facet preserves a source declaration, including Unlogged=false. Complete coverage makes an absent facet mean a logged table.
type Persistence struct {
	// Unlogged skips the write-ahead log. PostgreSQL truncates an unlogged table
	// after a crash and does not replicate its contents to standby servers.
	Unlogged bool `json:"unlogged"`
}

// DesiredPersistence declares whether a table is unlogged.
type DesiredPersistence Persistence

// ObservedPersistence records whether a table is unlogged.
type ObservedPersistence Persistence

// SupportsPersistence reports whether target creates an unlogged table.
// YugabyteDB accepts UNLOGGED but creates a logged table; it cannot retain this
// declaration. On YugabyteDB 2026.1.2, CREATE UNLOGGED TABLE records
// relpersistence=p. CockroachDB and Spanner do not support it either.
func SupportsPersistence(target string) bool {
	return platform.NormalizeDialect(target) == platform.Postgres
}

// PersistenceTargets returns the targets with a defined persistence policy.
// PostgreSQL supports the setting. Other targets omit it with a diagnostic,
// except YDB, which refuses an unlogged declaration. The returned slice is independent.
func PersistenceTargets() []string {
	return []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
		platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle, platform.ClickHouse, platform.YDB}
}

// Kind returns the owned persistence identity.
func (*DesiredPersistence) Kind() schemaext.Kind { return PersistenceKind }

// Kind returns the owned persistence identity.
func (*ObservedPersistence) Kind() schemaext.Kind { return PersistenceKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (p *DesiredPersistence) Clone() schemaext.Value {
	if p == nil {
		return (*DesiredPersistence)(nil)
	}
	return new(*p)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (p *ObservedPersistence) Clone() schemaext.Value {
	if p == nil {
		return (*ObservedPersistence)(nil)
	}
	return new(*p)
}

// Equal compares declarations. Different representations are unequal.
func (p *DesiredPersistence) Equal(other schemaext.Value) bool {
	q, ok := other.(*DesiredPersistence)
	if !ok || p == nil || q == nil {
		return ok && p == nil && q == nil
	}
	return *p == *q
}

// Equal compares observations. Different representations are unequal.
func (p *ObservedPersistence) Equal(other schemaext.Value) bool {
	q, ok := other.(*ObservedPersistence)
	if !ok || p == nil || q == nil {
		return ok && p == nil && q == nil
	}
	return *p == *q
}

// ValidateDesiredPersistence refuses nil. Its error
// wraps schemaext.ErrInvalidValue and identifies the desired representation.
func ValidateDesiredPersistence(p *DesiredPersistence) error {
	if p == nil {
		return persistenceError(schemaext.Desired)
	}
	return nil
}

// ValidateObservedPersistence refuses nil. Its error
// wraps schemaext.ErrInvalidValue and identifies the observed representation.
func ValidateObservedPersistence(p *ObservedPersistence) error {
	if p == nil {
		return persistenceError(schemaext.Observed)
	}
	return nil
}

func persistenceError(representation schemaext.Representation) error {
	return &schemaext.InvalidModelError{Kind: PersistenceKind, Representation: representation,
		Message: "table persistence requires a non-nil value"}
}

// WithPersistence adds a declaration without mutating facets. A logged value
// adds nothing. A duplicate kind is refused by schemaext.Facets.With.
func WithPersistence(facets schemaext.Facets, persistence Persistence) (schemaext.Facets, error) {
	if !persistence.Unlogged {
		return facets, nil
	}
	return facets.With(new(DesiredPersistence(persistence)))
}

// WithObservedPersistence adds an observation under WithPersistence's rules.
func WithObservedPersistence(facets schemaext.Facets, persistence Persistence) (schemaext.Facets, error) {
	if !persistence.Unlogged {
		return facets, nil
	}
	return facets.With(new(ObservedPersistence(persistence)))
}

// PersistenceOf reads either representation. An absent facet returns the logged
// default and false. Invalid values wrap schemaext.ErrInvalidValue.
func PersistenceOf(facets schemaext.Facets) (Persistence, bool, error) {
	value, found, err := facets.Get(PersistenceKind)
	if err != nil || !found {
		return Persistence{}, false, err
	}
	switch p := value.(type) {
	case *DesiredPersistence:
		if err := ValidateDesiredPersistence(p); err != nil {
			return Persistence{}, true, err
		}
		return Persistence(*p), true, nil
	case *ObservedPersistence:
		if err := ValidateObservedPersistence(p); err != nil {
			return Persistence{}, true, err
		}
		return Persistence(*p), true, nil
	default:
		return Persistence{}, true, fmt.Errorf("%w: table persistence has unexpected type %T", schemaext.ErrInvalidValue, value)
	}
}

const persistenceDefinition = `{"type":"object","required":["unlogged"],"additionalProperties":false,"properties":{"unlogged":{"type":"boolean"}}}`

// PersistenceCodecs returns independent version-one desired and observed codecs.
// Unknown fields, null, duplicate keys and a missing unlogged key are refused.
func PersistenceCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredPersistence]{Prototype: &DesiredPersistence{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(persistenceDefinition), Shape: persistenceShape, Validate: ValidateDesiredPersistence}.Codec(),
		schemaext.ModelCodec[*ObservedPersistence]{Prototype: &ObservedPersistence{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(persistenceDefinition), Shape: persistenceShape, Validate: ValidateObservedPersistence}.Codec(),
	}
}

func persistenceShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table persistence", Allowed: []string{"unlogged"}, Required: []string{"unlogged"}})
	if err != nil {
		return err
	}
	if string(fields["unlogged"]) != "true" && string(fields["unlogged"]) != "false" {
		return fmt.Errorf("%w: table persistence requires a boolean unlogged value", schemaext.ErrInvalidValue)
	}
	return nil
}

var persistenceCoverage = schemaext.OwnedCoverageSource(Owner, PersistenceCodecs)

// PersistenceCoverage records what a source knows about table persistence.
// Complete coverage interprets an absent facet as a logged table. Subject
// records retain knowledge limits for individual tables.
func PersistenceCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return persistenceCoverage(PersistenceKind, representation, knowledge, subjects)
}

// HoldsPersistence reports whether subject identifies a named table.
func HoldsPersistence(subject objectidentity.ID) bool {
	return subject.Kind == objectidentity.KindTable && !subject.Name.Empty()
}
