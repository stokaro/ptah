package capability

import (
	"fmt"
	"unicode/utf8"

	"go.5x5.cz/ptah/core/platform"
)

// Traits are the capability values that are not yes-or-no.
//
// A boolean set answers "is this feature present". Some database differences
// have no such answer: an identifier limit is a number with a unit, and "how
// does this target model an enum" has three outcomes rather than two.
// stokaro/ptah#1230 asks that those not be flattened into a growing collection
// of misleading flags, and this type is where they are not.
//
// The two sources are deliberately different, and the difference is visible in
// how each field is resolved:
//
//   - [Traits.Identifiers] is static engine knowledge keyed on the dialect. No
//     measured release line changes it, so no version is consulted. When one
//     does, the version arrives here rather than in a new switch somewhere
//     else — that centralization is the point.
//   - [Traits.EnumModeling] and [Traits.ForeignKeyReference] are READ OFF the
//     boolean set. Both already existed there as mutually exclusive flag groups
//     [Capabilities.Validate] polices — the enum pair under an "at most one"
//     rule, where both false is a real answer and means the target models enums
//     neither way, and the three reference-policy keys under a stricter
//     "exactly one" rule that fires whenever foreign keys are supported.
//     Either way it is a mode wearing booleans. Deriving it adds no claim: the
//     same preset produces the same answer, spelled as what it always meant.
type Traits struct {
	// Identifiers is the longest identifier the target accepts.
	Identifiers IdentifierLimit `json:"identifiers"`

	// EnumModeling is how the target spells an enumerated column type.
	EnumModeling EnumMode `json:"enum_modeling"`

	// ForeignKeyReference is what the target requires of the columns a
	// foreign key points at.
	ForeignKeyReference ReferencePolicy `json:"foreign_key_reference"`
}

// IdentifierUnit says what an identifier limit counts. The distinction is not
// pedantry: PostgreSQL truncates at 63 BYTES, so a name of 32 two-byte
// characters is already over the limit while its rune count says otherwise.
type IdentifierUnit string

const (
	// IdentifierBytes counts UTF-8 bytes (the PostgreSQL family).
	IdentifierBytes IdentifierUnit = "bytes"
	// IdentifierCharacters counts characters (MySQL, MariaDB, SQL Server,
	// Cloud Spanner).
	IdentifierCharacters IdentifierUnit = "characters"
)

// IdentifierLimit is the longest identifier a target accepts, and the unit that
// length is measured in.
//
// This is the worked example of a capability that cannot be a flag. Written as
// booleans it would need one key per length, and the unit — the part that
// decides whether "éééé…" fits — would have nowhere to live at all.
type IdentifierLimit struct {
	// Max is the limit, or zero when Ptah models no limit for the target.
	Max int `json:"max,omitempty"`
	// Unit is what Max counts, empty exactly when Max is zero.
	Unit IdentifierUnit `json:"unit,omitempty"`
}

// Unlimited reports whether Ptah models no identifier limit for the target.
// That is a statement about Ptah's knowledge, not a promise the server accepts
// any name.
func (l IdentifierLimit) Unlimited() bool {
	return l.Max <= 0
}

// Exceeds reports whether name is longer than the limit allows.
//
// It applies the byte-versus-character rule so a caller does not have to. Two
// callers each applying it themselves is how a limit comes to be enforced
// correctly on one surface and by rune count on another, so callers ask this
// rather than comparing Max to a length they measured.
//
// This is where the rule is DEFINED, and an earlier version of this comment
// claimed it was already the only place it is APPLIED. It was not. Two copies
// existed; core/renderer's and dbschema's now consume this method, and ONE
// remains:
//
//   - internal/convert/fromschema (foreignKeyNameFits, foreignKeyNameWithSuffix)
//     TRUNCATES a generated name to fit rather than refusing it. Its predicate
//     half is exactly !Exceeds — compared against this method over nine dialects
//     and 16 name shapes chosen to straddle every boundary, 144 verdicts, zero
//     disagreements — but the truncation half needs a budget expressed in the
//     limit's unit, which this type does not expose. Consuming only the
//     predicate would split one algorithm across two packages and leave the rule
//     copied regardless. Giving this type unit-aware truncation is what would
//     retire it.
//
// A further caller should ask this rather than add a copy.
func (l IdentifierLimit) Exceeds(name string) bool {
	if l.Unlimited() {
		return false
	}
	if l.Unit == IdentifierBytes {
		return len(name) > l.Max
	}
	return utf8.RuneCountInString(name) > l.Max
}

// String renders the limit as "63 bytes" or "128 characters", and as
// "unlimited" when Ptah models none.
func (l IdentifierLimit) String() string {
	if l.Unlimited() {
		return "unlimited"
	}
	return fmt.Sprintf("%d %s", l.Max, l.Unit)
}

// EnumMode says how a target models an enumerated column type.
type EnumMode string

const (
	// EnumUnspecified is the zero value: the capability set does not
	// determine a mode. A set that claims both modes at once produces it, and
	// [Capabilities.Validate] rejects such a set, so it is reachable only from
	// a hand-built one.
	EnumUnspecified EnumMode = ""
	// EnumInline spells the values in the column type: MySQL and MariaDB
	// ENUM(...), ClickHouse Enum8/Enum16.
	EnumInline EnumMode = "inline"
	// EnumNamedType declares a separate named type: PostgreSQL
	// CREATE TYPE ... AS ENUM.
	EnumNamedType EnumMode = "named-type"
	// EnumUnsupported is a target that models enums neither way.
	EnumUnsupported EnumMode = "unsupported"
)

// ReferencePolicy says what a target requires of the columns a foreign key
// references.
type ReferencePolicy string

const (
	// ReferenceUnspecified is the zero value: foreign keys are supported and
	// the set names no policy, or names more than one. [Capabilities.Validate]
	// rejects both, so it is reachable only from a hand-built set.
	ReferenceUnspecified ReferencePolicy = ""
	// ReferenceUnique requires a declared unique key on the referenced
	// columns.
	ReferenceUnique ReferencePolicy = "unique"
	// ReferenceIndexed requires the referenced columns as a full leftmost
	// index prefix (MySQL before 8.4, and MariaDB).
	ReferenceIndexed ReferencePolicy = "indexed"
	// ReferenceBackingIndex is a target that creates the referenced-key
	// backing index itself (Cloud Spanner).
	ReferenceBackingIndex ReferencePolicy = "backing-index"
	// ReferenceUnsupported is a target with no declarative foreign keys at
	// all, so there is no referenced key to constrain.
	ReferenceUnsupported ReferencePolicy = "unsupported"
)

// identifierLimits is static engine knowledge, keyed on the normalized dialect.
//
// The numbers were already in this repository, inline in
// core/renderer's foreign-key name validation, as a dialect switch three arms
// wide. Moving them here is what lets a second caller ask the same question
// without copying the switch, which is how the answers drift apart.
//
// ClickHouse and SQLite are absent on purpose rather than by oversight: Ptah
// models no identifier limit for either, and the zero value says exactly that.
var identifierLimits = map[string]IdentifierLimit{
	platform.Postgres:    {Max: 63, Unit: IdentifierBytes},
	platform.CockroachDB: {Max: 63, Unit: IdentifierBytes},
	platform.YugabyteDB:  {Max: 63, Unit: IdentifierBytes},
	platform.MySQL:       {Max: 64, Unit: IdentifierCharacters},
	platform.MariaDB:     {Max: 64, Unit: IdentifierCharacters},
	platform.SQLServer:   {Max: 128, Unit: IdentifierCharacters},
	platform.Spanner:     {Max: 128, Unit: IdentifierCharacters},
}

// Identifiers returns the identifier limit Ptah models for a dialect name
// (normalized via platform.NormalizeDialect). A dialect with no modeled limit,
// and an unknown one, both return the zero value, which reports Unlimited.
func Identifiers(dialect string) IdentifierLimit {
	return identifierLimits[platform.NormalizeDialect(dialect)]
}

// TraitsFor resolves the non-boolean capability values for a target: the
// dialect supplies the static engine knowledge, and caps supplies the modes
// that are read off the boolean set.
//
// A nil caps is valid and reads as the conservative empty set, matching
// [Capabilities.Has]: enums resolve to [EnumUnsupported] and foreign keys to
// [ReferenceUnsupported].
func TraitsFor(dialect string, caps Capabilities) Traits {
	return Traits{
		Identifiers:         Identifiers(dialect),
		EnumModeling:        caps.EnumModeling(),
		ForeignKeyReference: caps.ForeignKeyReference(),
	}
}

// EnumModeling reports how the set models enumerated column types.
//
// The two keys are a mutually exclusive pair, so the honest reading of the
// pair is a three-valued mode rather than two independent flags. Both false is
// a real answer — SQLite models enums neither way — and is not the same as
// [EnumUnspecified].
func (c Capabilities) EnumModeling() EnumMode {
	inline, named := c.Has(EnumInlineColumn), c.Has(EnumCustomType)
	switch {
	case inline && named:
		return EnumUnspecified
	case inline:
		return EnumInline
	case named:
		return EnumNamedType
	default:
		return EnumUnsupported
	}
}

// ForeignKeyReference reports what the set requires of a foreign key's
// referenced columns.
//
// [Capabilities.Validate] already requires exactly one policy whenever
// foreign keys are supported, which makes this a total reading of a valid set:
// one of the three policies, or [ReferenceUnsupported] when the target has no
// foreign keys. A set that violates that rule reports
// [ReferenceUnspecified] rather than picking a policy the set did not name.
func (c Capabilities) ForeignKeyReference() ReferencePolicy {
	if !c.Has(ForeignKeys) {
		return ReferenceUnsupported
	}
	var found ReferencePolicy
	for policy, name := range referencePolicyNames {
		if !c.Has(policy) {
			continue
		}
		if found != "" {
			return ReferenceUnspecified
		}
		found = name
	}
	return found
}

// referencePolicyNames maps each policy capability to the mode value that
// names it. It carries the same keys foreignKeyReferencePolicies lists, and the
// census in traits_internal_test.go iterates THAT list to keep the two in step:
// a policy added there without a name here resolves to [ReferenceUnspecified]
// on a set [Capabilities.Validate] accepts, which is a silent wrong answer
// rather than a red test.
//
// The census has to read the list. Measured on a copy of this repository: a
// fourth policy added to the registry, to the mutex group, to
// foreignKeyReferencePolicies and to every preset, with this map left at three
// entries, kept every test in the package green while the census enumerated a
// hand-typed literal of three.
var referencePolicyNames = map[Capability]ReferencePolicy{
	ForeignKeysRequireUniqueReference:  ReferenceUnique,
	ForeignKeysRequireIndexedReference: ReferenceIndexed,
	ForeignKeysCreateBackingIndex:      ReferenceBackingIndex,
}
