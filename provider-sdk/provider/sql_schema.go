package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ReadSQLSchemaFeature advertises offline reading of the SQL schema documents
// of an engine that declares the DOCUMENT mode. The engine's grammar is then not
// the shared one, so the owner reads every document in full.
const ReadSQLSchemaFeature = "schema.read-sql"

// MaxSQLDiagnosticMessage bounds a diagnostic message in bytes. A message names
// a span and a short token; it does not quote the statement, because a schema
// file can carry a secret value.
const MaxSQLDiagnosticMessage = 512

// SQLSchemaReader reads a complete load of SQL documents without a session,
// an address or a credential. It receives text and opens nothing. The reply is
// the result of running every document in order, so a later document can alter
// what an earlier one declared. A construct the owner recognizes and cannot
// represent is reported as an UNSUPPORTED diagnostic naming it, never skipped.
// A failure is an error or a reply with complete false and error diagnostics;
// neither exposes a partial schema.
type SQLSchemaReader interface {
	ReadSQLSchema(context.Context, *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error)
}

func sqlSchemaError(reason string) error {
	return fmt.Errorf("%w: SQL schema: %s", ErrProtocol, reason)
}

// validateSQLSchemaDeclaration checks the mode an engine declares. An engine
// with no declaration reads SQL in FRAGMENTS mode and offers no operation.
func validateSQLSchemaDeclaration(description *protocol.Description, engine *protocol.Engine) error {
	declaration := engine.GetSqlSchema()
	offered := slices.Contains(engine.GetFeatures(), ReadSQLSchemaFeature)
	if offered && !slices.Contains(description.GetFeatures(), ReadSQLSchemaFeature) {
		return sqlSchemaError("engine reading requires the service")
	}
	if declaration == nil {
		if offered {
			return sqlSchemaError("engine reading requires the DOCUMENT mode")
		}
		return nil
	}
	switch declaration.GetMode() {
	case protocol.SQLSchemaDeclaration_FRAGMENTS:
		if offered || len(declaration.GetHeaderKinds()) != 0 {
			return sqlSchemaError("FRAGMENTS mode declares no reader and no header kinds")
		}
	case protocol.SQLSchemaDeclaration_DOCUMENT:
		if !offered {
			return sqlSchemaError("DOCUMENT mode requires the engine to offer reading")
		}
	default:
		return sqlSchemaError("unknown mode")
	}
	seen := make(map[string]bool)
	for _, kind := range declaration.GetHeaderKinds() {
		if !comparisonReason(kind) || seen[kind] {
			return sqlSchemaError("invalid or duplicate header kind")
		}
		seen[kind] = true
	}
	return nil
}

// validateSQLSchemaService requires the provider-level feature to have an
// engine that uses it.
func validateSQLSchemaService(description *protocol.Description) error {
	if !slices.Contains(description.GetFeatures(), ReadSQLSchemaFeature) {
		return nil
	}
	for _, engine := range description.GetEngines() {
		if engine.GetSqlSchema().GetMode() == protocol.SQLSchemaDeclaration_DOCUMENT {
			return nil
		}
	}
	return sqlSchemaError("service advertised without a DOCUMENT engine")
}

// ValidateReadSQLSchemaRequest checks the load before dispatch: the engine
// declares the DOCUMENT mode, identifier facts are complete, documents have
// unique identities in a nonempty order, and every limit names a header kind
// the engine declared. It reads no file.
func ValidateReadSQLSchemaRequest(description *protocol.Description, request *protocol.ReadSQLSchemaRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), ReadSQLSchemaFeature) {
		return sqlSchemaError("reading is unavailable")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	declaration := engine.GetSqlSchema()
	if declaration.GetMode() != protocol.SQLSchemaDeclaration_DOCUMENT || !slices.Contains(engine.GetFeatures(), ReadSQLSchemaFeature) {
		return sqlSchemaError("engine does not read SQL documents")
	}
	if err := validateComparisonContext(&protocol.CompareModelsRequest{Identifiers: request.GetIdentifiers()}, &protocol.ModelComparison{Placement: protocol.ModelComparison_OBJECTS}); err != nil {
		return err
	}
	if len(request.GetDocuments()) == 0 {
		return sqlSchemaError("a load has at least one document")
	}
	ids := make(map[string]bool)
	for _, document := range request.GetDocuments() {
		if document == nil || strings.TrimSpace(document.GetId()) == "" || ids[document.GetId()] || strings.TrimSpace(document.GetPath()) == "" {
			return sqlSchemaError("missing or duplicate document identity")
		}
		ids[document.GetId()] = true
	}
	for _, limit := range request.GetLimits() {
		if !slices.Contains(declaration.GetHeaderKinds(), limit.GetKind()) {
			return sqlSchemaError("limit kind is not a declared header kind")
		}
	}
	return nil
}

// ValidateSQLSchema checks a reply against its request. A reply that fails a
// check is an error and exposes no schema. A reply with complete false is valid
// when it carries an error diagnostic and no schema; the caller refuses the
// load. A complete reply carries one declaration entry per document in request
// order, owned and unique objects, spans inside the document text, DESIRED
// coverage, and exactly one trace for every object. It validates the wire
// contract, not whether the owner read the dialect correctly.
func ValidateSQLSchema(description *protocol.Description, request *protocol.ReadSQLSchemaRequest, result *protocol.SQLSchema) error {
	if err := ValidateReadSQLSchemaRequest(description, request); err != nil {
		return err
	}
	if result == nil {
		return sqlSchemaError("reply is missing")
	}
	failures, err := validateSQLDiagnostics(request, result.GetDiagnostics())
	if err != nil {
		return err
	}
	if !result.GetComplete() {
		if failures == 0 {
			return sqlSchemaError("an incomplete reply needs an error diagnostic")
		}
		return validateSQLSchemaEmpty(result)
	}
	if failures != 0 {
		return sqlSchemaError("a complete reply carries no error diagnostic")
	}
	engine, features, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	rules := request.GetIdentifiers()
	for _, check := range []func() error{
		func() error { return validateSQLDeclarations(request, result) },
		func() error { return validateSQLTables(rules, result) },
		func() error { return validateSQLObjects(rules, result) },
		func() error { return validateSQLCommonAdditions(engine, result) },
		func() error {
			return validateSQLNativeObjects(description, engine, features, result)
		},
		func() error { return validateSQLTraces(request, result) },
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func validateSQLSchemaEmpty(result *protocol.SQLSchema) error {
	if len(result.GetTables()) != 0 || len(result.GetNativeObjects()) != 0 || len(result.GetDeclarations()) != 0 || result.GetCoverage() != nil ||
		len(result.GetNamespaces()) != 0 || len(result.GetEnums()) != 0 || len(result.GetViews()) != 0 || len(result.GetSequences()) != 0 ||
		len(result.GetRoles()) != 0 || len(result.GetGrants()) != 0 || len(result.GetTraces()) != 0 {
		return sqlSchemaError("an incomplete reply exposes no schema")
	}
	return nil
}

// validateSQLDiagnostics checks every diagnostic and returns the number of
// failures among them.
func validateSQLDiagnostics(request *protocol.ReadSQLSchemaRequest, diagnostics []*protocol.SQLDiagnostic) (int, error) {
	lengths := sqlDocumentLengths(request)
	failures := 0
	for _, diagnostic := range diagnostics {
		length, ok := lengths[diagnostic.GetDocumentId()]
		switch diagnostic.GetCode() {
		case protocol.SQLDiagnostic_SYNTAX, protocol.SQLDiagnostic_UNSUPPORTED, protocol.SQLDiagnostic_INVALID, protocol.SQLDiagnostic_INCOMPLETE:
		default:
			return 0, sqlSchemaError("unknown diagnostic code")
		}
		switch diagnostic.GetSeverity() {
		case protocol.SQLDiagnostic_ERROR:
			failures++
		case protocol.SQLDiagnostic_WARNING:
		default:
			return 0, sqlSchemaError("unknown diagnostic severity")
		}
		if !ok || !comparisonReason(diagnostic.GetMessage()) || len(diagnostic.GetMessage()) > MaxSQLDiagnosticMessage {
			return 0, sqlSchemaError("diagnostic needs a known document and a short single-line message")
		}
		if err := validateSQLSpan(diagnostic.GetSpan(), length); err != nil {
			return 0, err
		}
	}
	return failures, nil
}

func sqlDocumentLengths(request *protocol.ReadSQLSchemaRequest) map[string]uint64 {
	lengths := make(map[string]uint64, len(request.GetDocuments()))
	for _, document := range request.GetDocuments() {
		lengths[document.GetId()] = uint64(len(document.GetText()))
	}
	return lengths
}

func validateSQLSpan(span *protocol.SourceSpan, length uint64) error {
	walk := sourceWalk{length: length}
	if err := walk.span(span, authoredSource); err != nil {
		return sqlSchemaError("span is outside the document text")
	}
	return nil
}

func validateSQLDeclarations(request *protocol.ReadSQLSchemaRequest, result *protocol.SQLSchema) error {
	if len(result.GetDeclarations()) != len(request.GetDocuments()) {
		return sqlSchemaError("declarations differ from the requested documents")
	}
	for i, document := range request.GetDocuments() {
		declared := result.GetDeclarations()[i]
		if declared.GetDocumentId() != document.GetId() {
			return sqlSchemaError("declarations changed document identity or order")
		}
		for _, list := range [][]*protocol.SQLDeclaration{declared.GetObjects(), declared.GetGuards()} {
			for _, object := range list {
				if strings.TrimSpace(object.GetKind()) == "" || strings.TrimSpace(object.GetName()) == "" {
					return sqlSchemaError("declaration needs a kind and a name")
				}
			}
		}
		for _, statement := range declared.GetTables() {
			if strings.TrimSpace(statement.GetName()) == "" || (statement.GetDrop() && statement.GetGuarded()) {
				return sqlSchemaError("invalid table statement declaration")
			}
		}
	}
	return nil
}

// sqlFold applies an identifier comparison rule to a name.
func sqlFold(comparison protocol.IdentifierRules_Comparison, name string) string {
	if comparison == protocol.IdentifierRules_ASCII_CASE_INSENSITIVE {
		return strings.Map(func(r rune) rune {
			if r >= 'A' && r <= 'Z' {
				return r + ('a' - 'A')
			}
			return r
		}, name)
	}
	return name
}

type sqlNames map[string]bool

// add records a qualified name and reports whether it was new.
func (n sqlNames) add(schema, name string) bool {
	key := schema + "\x00" + name
	if n[key] {
		return false
	}
	n[key] = true
	return true
}

func validateSQLTables(identifiers *protocol.ComparisonIdentifiers, result *protocol.SQLSchema) error {
	rules := identifiers.GetRules()
	names := make(sqlNames)
	for _, table := range result.GetTables() {
		if strings.TrimSpace(table.GetName()) == "" {
			return sqlSchemaError("table needs a name")
		}
		schema := table.GetSchema()
		if schema == "" {
			schema = identifiers.GetDefaultSchema()
		}
		if !names.add(sqlFold(rules.GetTableNames(), schema), sqlFold(rules.GetTableNames(), table.GetName())) {
			return sqlSchemaError("duplicate table")
		}
		columns := make(map[string]bool)
		for _, column := range table.GetColumns() {
			folded := sqlFold(rules.GetColumnNames(), column.GetName())
			if strings.TrimSpace(column.GetName()) == "" || strings.TrimSpace(column.GetType()) == "" || columns[folded] {
				return sqlSchemaError("malformed or duplicate column")
			}
			columns[folded] = true
		}
	}
	return nil
}

// validateSQLObjects checks the collections that have no wire form of their
// own: namespaces, enums, views, sequences, roles and grants.
func validateSQLObjects(identifiers *protocol.ComparisonIdentifiers, result *protocol.SQLSchema) error {
	if err := validateNamespaceDeclarations(result.GetNamespaces()); err != nil {
		return err
	}
	enums := make(sqlNames)
	for _, declared := range result.GetEnums() {
		if !comparisonReason(declared.GetName()) || !enums.add(declared.GetSchema(), declared.GetName()) {
			return sqlSchemaError("missing or duplicate enum")
		}
	}
	if err := validateSQLViews(identifiers, result); err != nil {
		return err
	}
	if err := validateSQLSequences(identifiers, result.GetSequences()); err != nil {
		return err
	}
	return validateSQLPrincipals(result)
}

// validateSQLViews requires a view to share the table namespace of the rules:
// a view cannot take the name of a table or of another view.
func validateSQLViews(identifiers *protocol.ComparisonIdentifiers, result *protocol.SQLSchema) error {
	rules := identifiers.GetRules()
	relations := make(sqlNames)
	qualified := func(schema, name string) (string, string) {
		if schema == "" {
			schema = identifiers.GetDefaultSchema()
		}
		return sqlFold(rules.GetTableNames(), schema), sqlFold(rules.GetTableNames(), name)
	}
	for _, table := range result.GetTables() {
		relations.add(qualified(table.GetSchema(), table.GetName()))
	}
	for _, view := range result.GetViews() {
		if strings.TrimSpace(view.GetName()) == "" || strings.TrimSpace(view.GetBody()) == "" || !relations.add(qualified(view.GetSchema(), view.GetName())) {
			return sqlSchemaError("view needs a unique name and a body")
		}
	}
	return nil
}

func validateSQLSequences(identifiers *protocol.ComparisonIdentifiers, sequences []*protocol.SQLSequence) error {
	names := make(sqlNames)
	for _, sequence := range sequences {
		schema := sequence.GetSchema()
		if schema == "" {
			schema = identifiers.GetDefaultSchema()
		}
		if strings.TrimSpace(sequence.GetName()) == "" || !names.add(schema, sequence.GetName()) ||
			(sequence.Increment != nil && sequence.GetIncrement() == 0) {
			return sqlSchemaError("sequence needs a unique name and a nonzero increment")
		}
	}
	return nil
}

func validateSQLPrincipals(result *protocol.SQLSchema) error {
	roles := make(sqlNames)
	for _, role := range result.GetRoles() {
		if strings.TrimSpace(role.GetName()) == "" || !roles.add("", role.GetName()) {
			return sqlSchemaError("role needs a unique name")
		}
	}
	for _, grant := range result.GetGrants() {
		if strings.TrimSpace(grant.GetRole()) == "" || len(grant.GetPrivileges()) == 0 || strings.TrimSpace(grant.GetTarget().GetKind()) == "" ||
			slices.ContainsFunc(grant.GetPrivileges(), func(privilege string) bool { return strings.TrimSpace(privilege) == "" }) {
			return sqlSchemaError("grant needs a role, privileges and a target kind")
		}
	}
	return nil
}

// validateSQLCommonAdditions refuses the fields contract 25.3 adds to a role
// and a grant unless the host negotiated the common-object wire.
func validateSQLCommonAdditions(engine *protocol.Engine, result *protocol.SQLSchema) error {
	negotiated := engine.GetCommonObjects() != nil
	for _, role := range result.GetRoles() {
		if role.GetPasswordDeclared() && !negotiated {
			return sqlSchemaError("a declared password needs " + CommonObjectsFeature)
		}
	}
	for _, grant := range result.GetGrants() {
		if len(grant.GetColumns()) != 0 && !negotiated {
			return sqlSchemaError("grant columns need " + CommonObjectsFeature)
		}
		if err := validateColumnList(grant.GetColumns()); err != nil {
			return err
		}
	}
	return nil
}

func validateSQLNativeObjects(description *protocol.Description, engine *protocol.Engine, features map[string]bool, result *protocol.SQLSchema) error {
	if result.GetCoverage() == nil {
		return sqlSchemaError("coverage is required")
	}
	coverage := result.GetCoverage()
	if err := ValidateModelCoverage(description, engine.GetId(), coverage); err != nil {
		return err
	}
	covered := make(map[string]bool)
	for _, record := range coverage.GetKinds() {
		covered[record.GetModel().GetKind()] = true
	}
	seen := make(map[string]bool)
	for _, object := range result.GetNativeObjects() {
		if err := validateValueEnvelope(description.GetProviderId(), engine.GetId(), features, object); err != nil {
			return err
		}
		model := findValueModel(description, engine.GetId(), object.GetKind(), protocol.ValueInput_DESIRED)
		if !envelopeMatchesModel(object, model) || !slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_SQL) {
			return sqlSchemaError("native object is outside the negotiated SQL models")
		}
		if object.GetKind() != object.GetOwner().GetKind() || !covered[object.GetKind()] {
			return sqlSchemaError("native object identity or coverage differs from its model")
		}
		key, err := proto.MarshalOptions{Deterministic: true}.Marshal(object.GetOwner())
		if err != nil || seen[string(key)] {
			return sqlSchemaError("duplicate native object")
		}
		seen[string(key)] = true
	}
	return nil
}

func validateSQLTraces(request *protocol.ReadSQLSchemaRequest, result *protocol.SQLSchema) error {
	counts := map[protocol.SQLObjectTrace_Collection]uint64{
		protocol.SQLObjectTrace_TABLES:         uint64(len(result.GetTables())),
		protocol.SQLObjectTrace_NATIVE_OBJECTS: uint64(len(result.GetNativeObjects())),
		protocol.SQLObjectTrace_NAMESPACES:     uint64(len(result.GetNamespaces())),
		protocol.SQLObjectTrace_ENUMS:          uint64(len(result.GetEnums())),
		protocol.SQLObjectTrace_VIEWS:          uint64(len(result.GetViews())),
		protocol.SQLObjectTrace_SEQUENCES:      uint64(len(result.GetSequences())),
		protocol.SQLObjectTrace_ROLES:          uint64(len(result.GetRoles())),
		protocol.SQLObjectTrace_GRANTS:         uint64(len(result.GetGrants())),
	}
	var want uint64
	for _, count := range counts {
		want += count
	}
	if uint64(len(result.GetTraces())) != want {
		return sqlSchemaError("every object needs exactly one trace")
	}
	type key struct {
		collection protocol.SQLObjectTrace_Collection
		index      uint32
	}
	lengths := sqlDocumentLengths(request)
	seen := make(map[key]bool)
	for _, trace := range result.GetTraces() {
		source := trace.GetSource()
		length, ok := lengths[source.GetDocumentId()]
		id := key{trace.GetCollection(), trace.GetIndex()}
		if count, known := counts[trace.GetCollection()]; !known || uint64(trace.GetIndex()) >= count || seen[id] {
			return sqlSchemaError("trace names no object or repeats one")
		}
		seen[id] = true
		if !ok || source.GetFragmentId() != "" || len(source.GetPath()) != 0 || source.GetRoot() != protocol.SourceTrace_ROOT_UNSPECIFIED {
			return sqlSchemaError("trace must name a requested document and no fragment")
		}
		if err := validateSQLSpan(source.GetSpan(), length); err != nil {
			return err
		}
	}
	return nil
}
