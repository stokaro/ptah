package embedgen

import "go.5x5.cz/ptah/internal/embedrun"

// SpecVersion is the version of this specification's own shape.
//
// It is part of the identity: a change to what the fields below MEAN, without a
// change to any of their values, still produces vectors that are not comparable
// to the ones before it. Bump it when the meaning moves, never when a field is
// added whose zero value preserves the old behaviour.
const SpecVersion = 1

// ContractVersion is the version of the operation contract an engine executing
// this specification honours -- batching, checkpointing and write semantics.
//
// Separate from SpecVersion because the two move for different reasons: the
// specification describes what a vector IS, the contract describes how a run
// produces one.
const ContractVersion = 1

// Spec is one embedding transformation: which rows, which text, which model,
// and what the result is stored as.
//
// Every field is either part of [Spec.Identity] or explicitly excluded from it,
// and TestIdentity_EveryFieldIsClassified enumerates the struct to require that
// -- a field added here without a decision fails the build's tests rather than
// silently joining or silently missing the identity.
type Spec struct {
	// Source names the rows and the fields the input is built from.
	Source Source
	// Preprocessing turns one source row into one provider input,
	// deterministically.
	Preprocessing Preprocessing
	// Model names the provider, the model and what it was asked for.
	Model Model
	// Target names what the vector is stored as and how it will be compared.
	Target Target

	// Name is what an operator calls this generation. It is deliberately NOT
	// part of the identity: renaming a generation does not make its vectors
	// incomparable, and an identity that moved on a rename would make every
	// display change a migration.
	Name string
	// Description is prose for a human, and is excluded for the same reason.
	Description string
}

// Source is the rows a generation reads.
type Source struct {
	// Schema and Table name the relation. Both are part of the identity: the
	// same field names over a different table are different vectors.
	Schema string
	Table  string
	// KeyFields identify a row, in order. The order is part of the identity
	// because a composite key's ordering decides how a target row is addressed.
	KeyFields []string
	// InputFields are the columns the provider input is built from, IN ORDER.
	// The order is load-bearing: concatenating title then body produces
	// different text, and therefore different vectors, than body then title.
	InputFields []string
	// Filter restricts the rows in scope, as a SQL boolean expression over the
	// source relation. Empty means every row.
	//
	// It is part of the identity because a generation covering half the corpus
	// is not the generation covering all of it, and a verification that
	// compared them would report a coverage gap that is really a scope
	// difference.
	Filter string
	// VersionStrategy names how a row's version is established, which decides
	// whether a late write may overwrite a newer one.
	VersionStrategy VersionStrategy
	// VersionField is the column carrying that version, for the strategies that
	// read one.
	VersionField string
}

// VersionStrategy is how one source row's version is established.
type VersionStrategy string

const (
	// VersionUnset is the zero value and is not a strategy. A specification
	// reaching an engine with it is refused rather than defaulted: defaulting
	// picks an ordering guarantee on the operator's behalf.
	VersionUnset VersionStrategy = ""
	// VersionMonotonic reads an explicit monotonic version the application
	// maintains. Preferred where one exists.
	VersionMonotonic VersionStrategy = "monotonic"
	// VersionOutboxSequence reads a transactional outbox sequence, which is the
	// preferred strategy for a live migration inside one database.
	VersionOutboxSequence VersionStrategy = "outbox_sequence"
	// VersionUpdatedAt reads an update timestamp. Allowed, and weaker: two
	// writes inside one clock tick are indistinguishable, and a clock that
	// moves backwards reorders them.
	VersionUpdatedAt VersionStrategy = "updated_at"
	// VersionInputHash uses the source-input hash. It establishes freshness --
	// whether a target row matches its source today -- and NOT order, so it
	// cannot by itself decide which of two writes is newer.
	VersionInputHash VersionStrategy = "input_hash"
)

// Preprocessing is the deterministic path from one source row to one provider
// input.
type Preprocessing struct {
	// Separator joins the input fields. Part of the identity: a different
	// separator is different text.
	Separator string
	// Prefix is a fixed instruction string some models expect ahead of the
	// input, for example "query: ". Part of the identity for the same reason.
	Prefix string
	// NullPolicy decides what a NULL input field contributes.
	NullPolicy NullPolicy
	// EmptyPolicy decides what happens when the whole canonical input is empty.
	EmptyPolicy EmptyPolicy
	// UnicodeNormalization names the normalization form applied to the input,
	// or none.
	UnicodeNormalization UnicodeForm
	// CollapseWhitespace folds runs of whitespace to a single space and trims
	// the ends.
	CollapseWhitespace bool
	// MaxInputBytes bounds the canonical input. Zero means unbounded.
	MaxInputBytes int
	// Truncate decides what happens when the input exceeds MaxInputBytes.
	//
	// Silent truncation is not available: the two values are "refuse" and
	// "truncate", and truncating is part of the identity because a truncated
	// input produces a different vector than the whole one.
	Truncate TruncatePolicy
}

// NullPolicy is what a NULL input field contributes to the canonical input.
type NullPolicy string

const (
	// NullAsEmpty renders a NULL field as the empty string, so the separators
	// around it remain and the field's position is preserved.
	NullAsEmpty NullPolicy = "empty"
	// NullSkipField omits the field and the separator that would follow it.
	NullSkipField NullPolicy = "skip"
	// NullRefuseRow refuses the row rather than embedding a guess about it.
	NullRefuseRow NullPolicy = "refuse"
)

// EmptyPolicy is what happens when the canonical input comes out empty.
type EmptyPolicy string

const (
	// EmptyRefuseRow refuses the row. A model asked to embed nothing answers
	// with a vector that means nothing, and a corpus carrying those answers
	// retrieves them.
	EmptyRefuseRow EmptyPolicy = "refuse"
	// EmptySkipRow leaves the row without a vector in this generation, which
	// verification then reports as an intentional gap rather than a miss.
	EmptySkipRow EmptyPolicy = "skip"
)

// UnicodeForm names a Unicode normalization form.
type UnicodeForm string

const (
	// UnicodeNone applies no normalization, which keeps the source bytes as the
	// application stored them.
	UnicodeNone UnicodeForm = "none"
	// UnicodeNFC is the composed form.
	UnicodeNFC UnicodeForm = "nfc"
	// UnicodeNFD is the decomposed form.
	UnicodeNFD UnicodeForm = "nfd"
	// UnicodeNFKC is the compatibility composed form.
	UnicodeNFKC UnicodeForm = "nfkc"
	// UnicodeNFKD is the compatibility decomposed form.
	UnicodeNFKD UnicodeForm = "nfkd"
)

// TruncatePolicy is what happens to an input over the size bound.
type TruncatePolicy string

const (
	// TruncateRefuse refuses the row, which is the default because it is the
	// answer that cannot be wrong quietly.
	TruncateRefuse TruncatePolicy = "refuse"
	// TruncateBytes cuts the canonical input at the bound, on a rune boundary.
	TruncateBytes TruncatePolicy = "bytes"
)

// Model is the provider and what it was asked for.
type Model struct {
	// Provider is the provider type, for example "openai-compatible". It is the
	// CLASS rather than one vendor: two providers speaking one protocol are
	// still two providers, and the endpoint below separates them.
	Provider string
	// EndpointClass distinguishes a local endpoint from a hosted one, without
	// carrying the URL: an endpoint moving from one host to another does not
	// change what the vectors mean, and putting a URL in the identity would
	// make a DNS change a migration.
	EndpointClass EndpointClass
	// Identifier is the model as the provider names it.
	Identifier string
	// Revision is an immutable revision or digest, empty when the provider
	// exposes none. See [Spec.Reproducibility].
	Revision string
	// RequestedDimension is what the specification asked for, zero when the
	// model's native dimension is taken.
	RequestedDimension int
	// ReportedDimension is what the provider answered with, and it is the one a
	// target column has to match.
	ReportedDimension int
	// Normalization names what is done to the returned vector.
	Normalization VectorNormalization
	// Pooling names the pooling or encoding option where the provider exposes
	// one, empty where it does not.
	Pooling string
}

// EndpointClass separates the kinds of endpoint whose difference is semantic.
type EndpointClass string

const (
	// EndpointLocal is a model served on the operator's own machine or network.
	EndpointLocal EndpointClass = "local"
	// EndpointHosted is a third-party hosted service.
	EndpointHosted EndpointClass = "hosted"
	// EndpointGateway is an enterprise gateway standing in front of one or more
	// models, which is its own class because the gateway decides what the model
	// identifier resolves to.
	EndpointGateway EndpointClass = "gateway"
)

// VectorNormalization is what is done to a returned vector before it is stored.
type VectorNormalization string

const (
	// NormalizationNone stores the vector as returned.
	NormalizationNone VectorNormalization = "none"
	// NormalizationL2 scales the vector to unit length, which makes inner
	// product and cosine agree.
	NormalizationL2 VectorNormalization = "l2"
)

// Target is what the vector is stored as, and how it will be compared.
type Target struct {
	// Schema, Table and Column name where the vector goes. A generation writes
	// its own column or its own table; it never overwrites another generation's
	// (Decision 6).
	Schema string
	Table  string
	Column string
	// Representation is the stored vector type, for example "vector" or
	// "halfvec". Part of the identity: a half-precision copy of a vector is not
	// the vector.
	Representation string
	// Metric is the distance metric the index and the queries use. Part of the
	// identity because a corpus embedded for cosine is not the corpus for L2.
	Metric DistanceMetric
	// IndexMethod and IndexOptions describe the index built over the column.
	//
	// The METHOD is part of the identity and the OPTIONS are not: hnsw and
	// ivfflat answer a query differently enough to be a different generation,
	// while `m`, `ef_construction` and `lists` trade recall against build cost
	// over the same vectors. ADR 0010 measured that trade -- recall@10 spans
	// 26.5% to 100% on one unchanged index purely through a session setting --
	// which is what makes tuning a property of the query rather than of the
	// generation.
	IndexMethod  string
	IndexOptions map[string]string
}

// DistanceMetric is how two vectors are compared.
type DistanceMetric string

const (
	// MetricCosine compares by angle.
	MetricCosine DistanceMetric = "cosine"
	// MetricL2 compares by Euclidean distance.
	MetricL2 DistanceMetric = "l2"
	// MetricInnerProduct compares by inner product.
	MetricInnerProduct DistanceMetric = "inner_product"
)

// VersionOrder is how two versions this strategy produces are put in order.
//
// It lives on the strategy because the strategy is what decides the shape of
// the value: `updated_at` yields a rendered instant and `monotonic` yields a
// counter, and a comparison holding the string cannot tell them apart. Reading
// both as opaque strings ordered by length then lexicographically is right for
// the counter and wrong for the instant, which is how a fresh answer came to be
// discarded as stale (stokaro/ptah#2635).
//
// A strategy that records no version orders nothing, and says so.
func (s VersionStrategy) VersionOrder() embedrun.VersionOrder {
	switch s {
	case VersionMonotonic, VersionOutboxSequence:
		return embedrun.OrderNumeric
	case VersionUpdatedAt:
		return embedrun.OrderTimestamp
	default:
		return embedrun.OrderUnknown
	}
}
