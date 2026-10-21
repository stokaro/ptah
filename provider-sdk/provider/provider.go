// Package provider defines the independent, experimental provider services.
// Providers implement only the services they advertise. Transport records are
// explicit Protobuf messages; no host package or concrete engine is imported.
package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// Revision identifies this experimental contract. A different revision is
// refused before a session opens; it is not a stable protocol major version.
const Revision uint32 = 17

const (
	// CommonTableIdentityFeature promises the common table comparison contract:
	// no implicit schema objects, no undeclared table facets, and no additional
	// creation defaults or type coercions. Unsupported declarations are refused.
	CommonTableIdentityFeature = "schema.common-table-identity.v1"
	// InspectFeature advertises complete, scoped catalog inspection.
	InspectFeature = "schema.inspect"
	// RenderFeature advertises batched rendering of typed changes.
	RenderFeature = "schema.render"
	// AnalyzeSQLFeature advertises owner-defined statement boundaries and safety.
	AnalyzeSQLFeature = "sql.analyze"
	// ExecuteFeature advertises execution with explicit terminal outcomes.
	ExecuteFeature = "units.execute"
)

var (
	// ErrProtocol identifies an incompatible or malformed protocol exchange.
	ErrProtocol = errors.New("invalid provider protocol exchange")
	// ErrIncomplete identifies a read that cannot support an authoritative diff.
	ErrIncomplete = errors.New("provider inspection is incomplete")
	// ErrOutcomeUnknown identifies execution whose final effect is unconfirmed.
	ErrOutcomeUnknown = errors.New("provider execution outcome is unknown")
	// ErrExecution identifies a confirmed refusal or partial execution.
	ErrExecution = errors.New("provider execution did not complete")
)

// Sessions owns live connections and handles. Handles are scoped to one server
// process and must not be accepted after that process restarts. Close releases
// one handle; repeated closes of that handle succeed without affecting others.
type Sessions interface {
	Open(context.Context, *protocol.OpenRequest) (*protocol.Session, error)
	Close(context.Context, *protocol.CloseRequest) error
}

// Inspector reads a complete scoped snapshot. Unobserved facts must be marked
// explicitly; an error or incomplete snapshot is never an empty database.
type Inspector interface {
	Inspect(context.Context, *protocol.InspectRequest) (*protocol.Schema, error)
}

// Renderer renders the complete input batch without connecting to a database.
// Every input change has exactly one unit in the same order. Unsupported
// changes refuse the whole batch and expose no executable prefix.
type Renderer interface {
	Render(context.Context, *protocol.RenderRequest) (*protocol.RenderResult, error)
}

// Executor executes the exact units in order. It never retries a request merely
// because it has an ID. A lost reply leaves the outcome unknown. Cancellation
// does not establish rollback; a returned result records the confirmed prefix.
type Executor interface {
	Execute(context.Context, *protocol.ExecuteRequest) (*protocol.Execution, error)
}

// ValidateDescription checks the revision, identity, unique engines, and
// required features. It does not authorize the executable or trust its claims.
func ValidateDescription(value *protocol.Description, required []string) error {
	if value == nil || value.GetRevision() != Revision {
		return fmt.Errorf("%w: expected experimental revision %d", ErrProtocol, Revision)
	}
	if strings.TrimSpace(value.GetProviderId()) == "" || value.GetVersion() == "" || value.GetSemanticIdentity() == "" {
		return fmt.Errorf("%w: provider identity, version, and semantic identity are required", ErrProtocol)
	}
	if err := validateSemanticServices(value); err != nil {
		return err
	}
	names := make(map[string]bool)
	for _, engine := range value.GetEngines() {
		if engine == nil || !validName(engine.GetId()) {
			return fmt.Errorf("%w: invalid engine ID", ErrProtocol)
		}
		if err := validateEngineServices(value, engine); err != nil {
			return err
		}
		if err := ValidateIdentifierRules(engine.GetIdentifiers()); err != nil {
			return err
		}
		for _, name := range append([]string{engine.GetId()}, engine.GetAliases()...) {
			if !validName(name) || names[name] {
				return fmt.Errorf("%w: invalid or conflicting engine name %q", ErrProtocol, name)
			}
			names[name] = true
		}
	}
	if len(names) == 0 {
		return fmt.Errorf("%w: provider declares no engines", ErrProtocol)
	}
	for _, feature := range required {
		if !slices.Contains(value.GetFeatures(), feature) {
			return fmt.Errorf("%w: required feature %q is unavailable", ErrProtocol, feature)
		}
	}
	return nil
}

func validateEngineServices(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), HistoryFeature) && !slices.Contains(description.GetFeatures(), HistoryFeature) {
		return fmt.Errorf("%w: engine history requires the history service", ErrProtocol)
	}
	if slices.Contains(engine.GetFeatures(), TransactionsFeature) && !slices.Contains(description.GetFeatures(), TransactionsFeature) {
		return fmt.Errorf("%w: engine transactions require the transaction service", ErrProtocol)
	}
	if err := validateValueModels(engine.GetValueModels()); err != nil {
		return err
	}
	if err := validateModelComparisons(description, engine); err != nil {
		return err
	}
	if err := validateModelPlanning(description, engine); err != nil {
		return err
	}
	return validateYAMLSourceKeys(description, engine)
}

func validateSemanticServices(value *protocol.Description) error {
	if slices.Contains(value.GetFeatures(), TransactionsFeature) && !slices.Contains(value.GetFeatures(), ExecuteFeature) {
		return fmt.Errorf("%w: transactions require execution", ErrProtocol)
	}
	if slices.Contains(value.GetFeatures(), PlanFeaturesFeature) && !slices.Contains(value.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: feature planning requires value normalization", ErrProtocol)
	}

	if slices.Contains(value.GetFeatures(), CompareModelsFeature) && !slices.Contains(value.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: model comparison requires value normalization", ErrProtocol)
	}

	if slices.Contains(value.GetFeatures(), DecodeSourceFeature) && !slices.Contains(value.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: source decoding requires value normalization", ErrProtocol)
	}
	if slices.Contains(value.GetFeatures(), ConvertValuesFeature) && !slices.Contains(value.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: value conversion requires value normalization", ErrProtocol)
	}
	return nil
}

// ValidateIdentifierRules refuses missing or unknown comparison facts. The
// experimental contract supports exact and ASCII-insensitive comparison only.
func ValidateIdentifierRules(rules *protocol.IdentifierRules) error {
	if rules == nil {
		return fmt.Errorf("%w: identifier rules are required", ErrProtocol)
	}
	for _, comparison := range []protocol.IdentifierRules_Comparison{rules.GetTableNames(), rules.GetColumnNames(), rules.GetIndexNames()} {
		switch comparison {
		case protocol.IdentifierRules_EXACT, protocol.IdentifierRules_ASCII_CASE_INSENSITIVE:
		default:
			return fmt.Errorf("%w: unsupported identifier comparison", ErrProtocol)
		}
	}
	switch rules.GetIndexNamespace() {
	case protocol.IdentifierRules_TABLE, protocol.IdentifierRules_SCHEMA:
		return nil
	default:
		return fmt.Errorf("%w: unsupported index namespace", ErrProtocol)
	}
}

func validName(name string) bool {
	if name == "" || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '_' && char != '+' {
			return false
		}
	}
	return true
}

// ValidateSchema refuses incomplete inspection and invalid coverage. A complete
// empty schema is valid. Provider-owned values still require owner validation
// before the host uses their semantics.
func ValidateSchema(value *protocol.Schema) error {
	if value == nil || !value.GetComplete() {
		return ErrIncomplete
	}
	for _, disposition := range value.GetDispositions() {
		switch disposition.GetState() {
		case protocol.Disposition_MODELED, protocol.Disposition_NORMALIZED:
		default:
			return fmt.Errorf("%w: field %q has no complete interpretation", ErrIncomplete, disposition.GetFieldPath())
		}
	}
	seen := make(map[string]map[string]bool)
	for _, table := range value.GetTables() {
		if table == nil || table.GetIdentity() == nil || table.GetIdentity().GetName() == "" {
			return fmt.Errorf("%w: table identity is missing", ErrProtocol)
		}
		columns := make(map[string]bool)
		for _, column := range table.GetColumns() {
			if column.GetName() == "" || column.GetType() == "" || columns[column.GetName()] {
				return fmt.Errorf("%w: malformed or duplicate column", ErrProtocol)
			}
			columns[column.GetName()] = true
		}
		// The first contract admits schema/name table identity only. Other identity
		// components must be handled explicitly rather than concatenated or lost.
		ref := table.GetIdentity()
		if ref.GetCatalog() != "" || len(ref.GetPath()) != 0 || len(ref.GetOverload()) != 0 || ref.GetKind() != "table" {
			return fmt.Errorf("%w: unsupported table identity components", ErrProtocol)
		}
		if seen[ref.GetSchema()] == nil {
			seen[ref.GetSchema()] = make(map[string]bool)
		}
		if seen[ref.GetSchema()][ref.GetName()] {
			return fmt.Errorf("%w: duplicate table", ErrProtocol)
		}
		seen[ref.GetSchema()][ref.GetName()] = true
	}
	return nil
}

// ValidateRender checks completeness and input provenance. The result has no
// valid executable prefix when this function returns an error.
func ValidateRender(request *protocol.RenderRequest, result *protocol.RenderResult) error {
	if request == nil || result == nil || !result.GetComplete() || len(result.GetUnits()) != len(request.GetChanges()) {
		return fmt.Errorf("%w: incomplete render", ErrProtocol)
	}
	seen := make(map[string]bool)
	for i, change := range request.GetChanges() {
		unit := result.GetUnits()[i]
		if change.GetId() == "" || seen[change.GetId()] || unit.GetChangeId() != change.GetId() {
			return fmt.Errorf("%w: render input identity mismatch", ErrProtocol)
		}
		seen[change.GetId()] = true
		if err := ValidateUnit(unit); err != nil {
			return err
		}
	}
	return nil
}

// ValidateUnit refuses a missing executable body or an unversioned native
// payload. It grants no authorization to execute the unit.
func ValidateUnit(unit *protocol.Unit) error {
	if unit == nil || unit.GetChangeId() == "" {
		return fmt.Errorf("%w: unit identity is missing", ErrProtocol)
	}
	switch body := unit.GetBody().(type) {
	case *protocol.Unit_Sql:
		if body == nil || strings.TrimSpace(body.Sql) == "" {
			return fmt.Errorf("%w: empty SQL unit", ErrProtocol)
		}
	case *protocol.Unit_Native:
		if body == nil || body.Native.GetProviderId() == "" || body.Native.GetKind() == "" || body.Native.GetSchemaVersion() == 0 || body.Native.GetEncoding() == "" {
			return fmt.Errorf("%w: invalid native unit envelope", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: missing unit body", ErrProtocol)
	}
	return nil
}

// ExecutionError records the last confirmed prefix and terminal outcome. Its
// text excludes provider messages, which may contain connection secrets.
type ExecutionError struct {
	Outcome protocol.Execution_Outcome
	Applied uint32
}

// Error returns an outcome summary without provider-authored text.
func (e *ExecutionError) Error() string {
	return fmt.Sprintf("provider execution: %s (%d units confirmed applied)", e.Outcome, e.Applied)
}

// Unwrap distinguishes an unknown outcome from a confirmed execution failure.
func (e *ExecutionError) Unwrap() error {
	if e.Outcome == protocol.Execution_UNKNOWN {
		return ErrOutcomeUnknown
	}
	return ErrExecution
}

// CheckExecution validates a terminal result and reports any non-success as an
// ExecutionError. Malformed replies are unknown outcomes, never proof of a
// rollback. Empty execution batches must be refused before sending a request.
func CheckExecution(units int, result *protocol.Execution) error {
	unknown := &ExecutionError{Outcome: protocol.Execution_UNKNOWN}
	if result == nil || units <= 0 || uint64(result.GetAppliedCount()) > uint64(units) {
		return unknown
	}
	count := result.GetAppliedCount()
	switch result.GetOutcome() {
	case protocol.Execution_APPLIED:
		if uint64(count) != uint64(units) {
			return unknown
		}
		return nil
	case protocol.Execution_NOT_STARTED, protocol.Execution_NOT_APPLIED:
		if count != 0 {
			return unknown
		}
	case protocol.Execution_PARTIAL:
		if count == 0 || uint64(count) >= uint64(units) {
			return unknown
		}
	case protocol.Execution_UNKNOWN:
	default:
		return unknown
	}
	return &ExecutionError{Outcome: result.GetOutcome(), Applied: count}
}
