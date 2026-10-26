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

// Revision is the major version of this experimental contract. It changes only
// with an incompatible change, and a host and a provider of different majors
// are refused before a session opens. Compatible additions raise
// ContractMinor instead.
const Revision uint32 = 25

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
	// ErrStatementTimeout identifies execution that a unit stopped at the
	// request's statement timeout. It accompanies ErrExecution or
	// ErrOutcomeUnknown, never replaces them.
	ErrStatementTimeout = errors.New("provider statement exceeded its timeout")
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
	if err := validateMinimumContract(value); err != nil {
		return err
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
	if err := validateColumnsFeature(description, engine); err != nil {
		return err
	}
	if err := validateOwnedSchemaServices(description, engine); err != nil {
		return err
	}
	if slices.Contains(engine.GetFeatures(), MetadataFeature) && !slices.Contains(description.GetFeatures(), MetadataFeature) {
		return fmt.Errorf("%w: engine metadata requires the metadata service", ErrProtocol)
	}
	if slices.Contains(engine.GetFeatures(), LocksFeature) && !slices.Contains(description.GetFeatures(), LocksFeature) {
		return fmt.Errorf("%w: engine locks require the lock service", ErrProtocol)
	}
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
	if err := validateLintRules(description, engine); err != nil {
		return err
	}
	if err := validateReleaseLines(engine); err != nil {
		return err
	}
	if err := validateDevServices(description, engine); err != nil {
		return err
	}
	if err := validateYAMLSourceKeys(description, engine); err != nil {
		return err
	}
	if err := validateGoSourceDirectives(description, engine); err != nil {
		return err
	}
	if err := validateEngineAdditions(description, engine); err != nil {
		return err
	}
	return validateSQLSchemaDeclaration(description, engine)
}

// validateEngineAdditions checks what contracts 25.1 to 25.4 add to an
// engine: the data services it names, its statement facts, its query dialect,
// its embedding store, its common objects, its HCL claims, its scope
// selections and the features it lists beside them.
func validateEngineAdditions(description *protocol.Description, engine *protocol.Engine) error {
	if err := validateDataServices(description, engine); err != nil {
		return err
	}
	if err := validateEmbeddingEngine(description, engine); err != nil {
		return err
	}
	for _, check := range []func(*protocol.Description, *protocol.Engine) error{
		validateCommonObjectSupport, validateContextFeatures, validateViewFacetFeature, validateObservedColumnsFeature,
		validateHCLSource, validateEncodeEngine, validateScopeSelections, validateModelDeclarations, validateSourceLimitsEngine, validatePlanningLifecycleEngine,
	} {
		if err := check(description, engine); err != nil {
			return err
		}
	}
	for _, feature := range []string{TextUnitsFeature, PlanningPolicyFeature, EntryKeysFeature, StatementsFeature, SensitiveInputsFeature} {
		if slices.Contains(engine.GetFeatures(), feature) && !slices.Contains(description.GetFeatures(), feature) {
			return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, feature)
		}
	}
	if slices.Contains(engine.GetFeatures(), LintStatementFactsFeature) && !slices.Contains(engine.GetFeatures(), LintFeature) {
		return fmt.Errorf("%w: lint statement facts require engine lint", ErrProtocol)
	}
	return ValidateQueryDialect(engine.GetQueryDialect())
}

func validateSemanticServices(value *protocol.Description) error {
	if err := validateSQLSchemaService(value); err != nil {
		return err
	}
	if slices.Contains(value.GetFeatures(), ValidateSchemaFeature) && !slices.Contains(value.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: schema validation requires value normalization", ErrProtocol)
	}
	if slices.Contains(value.GetFeatures(), ProjectTableCreationsFeature) && !slices.Contains(value.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: creation projection requires value normalization", ErrProtocol)
	}
	if slices.Contains(value.GetFeatures(), PrepareTablesFeature) && !slices.Contains(value.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: table preparation requires value normalization", ErrProtocol)
	}

	if slices.Contains(value.GetFeatures(), TransactionsFeature) && !slices.Contains(value.GetFeatures(), ExecuteFeature) {
		return fmt.Errorf("%w: transactions require execution", ErrProtocol)
	}
	if slices.Contains(value.GetFeatures(), StatementTimeoutFeature) && !slices.Contains(value.GetFeatures(), ExecuteFeature) {
		return fmt.Errorf("%w: statement timeouts require execution", ErrProtocol)
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
	return validateContractAdditions(value)
}

// validateContractAdditions checks the services that negotiated additions
// extend, so an advertised behavior always has its underlying service.
func validateContractAdditions(value *protocol.Description) error {
	features := value.GetFeatures()
	if slices.Contains(features, ValueContextFeature) && !slices.Contains(features, NormalizeValuesFeature) {
		return fmt.Errorf("%w: value context requires normalization", ErrProtocol)
	}
	if slices.Contains(features, SensitiveInputsFeature) && !slices.Contains(features, ExecuteFeature) {
		return fmt.Errorf("%w: sensitive inputs require execution", ErrProtocol)
	}
	if slices.Contains(features, TextUnitsFeature) && !slices.Contains(features, RenderFeature) && !slices.Contains(features, RenderRowWritesFeature) {
		return fmt.Errorf("%w: text units require rendering", ErrProtocol)
	}
	if slices.Contains(features, PlanningPolicyFeature) && !slices.Contains(features, PlanFeaturesFeature) && !slices.Contains(features, CompareModelsFeature) {
		return fmt.Errorf("%w: a planning policy requires planning or comparison", ErrProtocol)
	}
	for _, feature := range []string{EntryKeysFeature, HCLSourceFeature, EncodeSourceFeature} {
		if slices.Contains(features, feature) && !slices.Contains(features, DecodeSourceFeature) {
			return fmt.Errorf("%w: %s requires source decoding", ErrProtocol, feature)
		}
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
	if err := validateReservedObjects(value); err != nil {
		return err
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
		for i, column := range table.GetColumns() {
			if column.GetName() == "" || columns[column.GetName()] {
				return fmt.Errorf("%w: malformed or duplicate column", ErrProtocol)
			}
			if err := validateInspectedColumn(column, i+1); err != nil {
				return err
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
	if result.GetRefusal() != nil && request.GetForm() == protocol.RenderRequest_TEXT {
		return validateTextRefusal(result, func(id string) bool {
			return slices.ContainsFunc(request.GetChanges(), func(change *protocol.Change) bool { return change.GetId() == id })
		})
	}
	if request == nil || result == nil || !result.GetComplete() || len(result.GetUnits()) != len(request.GetChanges()) {
		return fmt.Errorf("%w: incomplete render", ErrProtocol)
	}
	if result.GetRefusal() != nil {
		return fmt.Errorf("%w: a schema render refuses only in the TEXT form", ErrProtocol)
	}
	if err := validateTextUnits(request.GetForm(), result.GetUnits()); err != nil {
		return err
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

// validateTextRefusal accepts a refusal of a change without a text form: no
// unit, a change the request holds, the reason NO_TEXT_FORM and one line of
// text.
func validateTextRefusal(result *protocol.RenderResult, requested func(string) bool) error {
	refusal := result.GetRefusal()
	if result.GetComplete() || len(result.GetUnits()) != 0 {
		return fmt.Errorf("%w: a refused render exposes units", ErrProtocol)
	}
	if refusal.GetReason() != protocol.RenderRefusal_NO_TEXT_FORM || !requested(refusal.GetChangeId()) || !diagnosticLine(refusal.GetMessage()) {
		return fmt.Errorf("%w: a text refusal needs a change of the request, NO_TEXT_FORM and one line of text", ErrProtocol)
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
		if body != nil && body.Native.GetContext() != nil {
			if err := ValidateDatabaseContext(body.Native.GetContext()); err != nil {
				return err
			}
		}
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
// TimedOut reports that the unit after the prefix stopped at the request's
// statement timeout; such an error also matches ErrStatementTimeout.
type ExecutionError struct {
	Outcome  protocol.Execution_Outcome
	Applied  uint32
	TimedOut bool
}

// Error returns an outcome summary without provider-authored text.
func (e *ExecutionError) Error() string {
	if e.TimedOut {
		return fmt.Sprintf("provider execution: %s at the statement timeout (%d units confirmed applied)", e.Outcome, e.Applied)
	}
	return fmt.Sprintf("provider execution: %s (%d units confirmed applied)", e.Outcome, e.Applied)
}

// Is reports a stop at the statement timeout as ErrStatementTimeout.
func (e *ExecutionError) Is(target error) bool {
	return e.TimedOut && target == ErrStatementTimeout
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

// ValidateExecuteRequest refuses a request without a session, a request ID or
// units, an invalid unit, a negative statement timeout, and a timeout the
// negotiated description does not list StatementTimeoutFeature for. It grants
// no authorization to execute the units.
func ValidateExecuteRequest(description *protocol.Description, request *protocol.ExecuteRequest) error {
	if request.GetSessionId() == "" || request.GetRequestId() == "" || len(request.GetUnits()) == 0 {
		return fmt.Errorf("%w: session ID, request ID, and units are required", ErrProtocol)
	}
	if request.GetStatementTimeoutNanoseconds() < 0 {
		return fmt.Errorf("%w: a statement timeout cannot be negative", ErrProtocol)
	}
	if request.GetStatementTimeoutNanoseconds() > 0 && !slices.Contains(description.GetFeatures(), StatementTimeoutFeature) {
		return fmt.Errorf("%w: statement timeouts were not negotiated", ErrProtocol)
	}
	for _, unit := range request.GetUnits() {
		if err := ValidateUnit(unit); err != nil {
			return err
		}
	}
	if err := validateUnitContexts(description, request.GetEngine(), request.GetUnits()); err != nil {
		return err
	}
	return validateExecuteInputs(description, request)
}

// CheckExecuted is CheckExecution for the request that produced result. A stop
// at the statement timeout is valid only when the request set a timeout and
// the outcome confirms no completion: NOT_APPLIED, PARTIAL or UNKNOWN. The
// returned ExecutionError then records TimedOut. Any other use of the flag is
// a malformed reply and an unknown outcome.
func CheckExecuted(request *protocol.ExecuteRequest, result *protocol.Execution) error {
	err := CheckExecution(len(request.GetUnits()), result)
	if !result.GetStatementTimedOut() {
		return err
	}
	outcome := result.GetOutcome()
	if request.GetStatementTimeoutNanoseconds() <= 0 ||
		outcome != protocol.Execution_NOT_APPLIED && outcome != protocol.Execution_PARTIAL && outcome != protocol.Execution_UNKNOWN {
		return &ExecutionError{Outcome: protocol.Execution_UNKNOWN}
	}
	// A malformed reply is reported as an unknown outcome with no confirmed
	// prefix, which is not the reply's own outcome and count.
	var failed *ExecutionError
	if errors.As(err, &failed) && failed.Outcome == outcome && failed.Applied == result.GetAppliedCount() {
		failed.TimedOut = true
	}
	return err
}

// The common identity policy supplies these services as a single contract.
// An owned implementation must be advertised separately and cannot overlap it.
func validateOwnedSchemaServices(description *protocol.Description, engine *protocol.Engine) error {
	for _, feature := range []string{PrepareTablesFeature, ProjectTableCreationsFeature, ValidateSchemaFeature} {
		if !slices.Contains(engine.GetFeatures(), feature) {
			continue
		}
		if !slices.Contains(description.GetFeatures(), feature) {
			return fmt.Errorf("%w: engine %s requires its service", ErrProtocol, feature)
		}
		if slices.Contains(engine.GetFeatures(), CommonTableIdentityFeature) {
			return fmt.Errorf("%w: engine cannot select both identity and owned %s", ErrProtocol, feature)
		}
	}
	return nil
}
