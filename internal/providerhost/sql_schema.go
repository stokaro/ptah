package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/sqlext"
	"ptah.run/internal/providerdiagnostic"
)

// sqlExtensions returns one SQL extension for every engine that reads its
// SQL schema documents whole (the DOCUMENT mode). Ptah does not lex or parse
// such a document: the extension's grammar sends the text to the provider and
// maps its reading back. An engine without the declaration is read with the
// shared grammar and contributes nothing here.
func (h *Host) sqlExtensions() []sqlext.Extension {
	var result []sqlext.Extension
	for _, engine := range h.description.GetEngines() {
		declaration := engine.GetSqlSchema()
		if declaration.GetMode() != protocol.SQLSchemaDeclaration_DOCUMENT {
			continue
		}
		extension := sqlext.Extension{Owner: h.description.GetProviderId(), Targets: []string{engine.GetId()}, Grammar: sqlGrammar{target: h.targets[engine.GetId()]}}
		for _, kind := range declaration.GetHeaderKinds() {
			extension.Header = append(extension.Header, coverage.Kind(kind))
		}
		result = append(result, extension)
	}
	return result
}

// checkSQLHeaderKinds refuses a header kind no header can hand to the owner:
// one a header reads as a common kind, which every owner's document shares,
// and one that cannot be written as one word of a header directive.
func checkSQLHeaderKinds(description *protocol.Description) error {
	for _, engine := range description.GetEngines() {
		for _, kind := range engine.GetSqlSchema().GetHeaderKinds() {
			if _, err := coverage.ParseKind(kind); err == nil {
				return fmt.Errorf("%w: SQL header kind %q of engine %q is a common coverage kind", provider.ErrProtocol, kind, engine.GetId())
			}
			if strings.ContainsFunc(kind, func(r rune) bool { return r <= ' ' || r == '"' }) {
				return fmt.Errorf("%w: SQL header kind %q of engine %q is not one word", provider.ErrProtocol, kind, engine.GetId())
			}
		}
	}
	return nil
}

// sqlGrammar reads every SQL schema document of one engine's load through the
// provider, in one call.
type sqlGrammar struct{ target *target }

// ReadSQL sends the load to the provider and maps its reading. A reply the
// provider marks incomplete is a refusal that names the first error and its
// position and exposes no schema, and so is a complete one with warnings.
func (g sqlGrammar) ReadSQL(ctx context.Context, request sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
	remote, err := g.target.sqlSchemaRequest(request)
	if err != nil {
		return sqlext.GrammarResult{}, err
	}
	reply, err := g.target.host.client.ReadSQLSchema(ctx, remote)
	if err != nil {
		return sqlext.GrammarResult{}, err
	}
	if !reply.GetComplete() || len(reply.GetDiagnostics()) != 0 {
		return sqlext.GrammarResult{}, sqlSchemaRefusal(remote, reply)
	}
	result, err := g.target.sqlSchemaResult(ctx, request, reply)
	if err != nil {
		return sqlext.GrammarResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return sqlext.GrammarResult{}, err
	}
	return result, nil
}

// sqlSchemaRequest is the provider's request for a load: the documents in
// order, the engine's offline identifier rules, and the entries of each
// document's header whose kind the engine reads. A document without a path,
// such as one read alone, is named by its place in the load.
func (t *target) sqlSchemaRequest(request sqlext.GrammarRequest) (*protocol.ReadSQLSchemaRequest, error) {
	if request.Target != t.engine.GetId() {
		return nil, fmt.Errorf("%w: SQL schema target changed", provider.ErrProtocol)
	}
	rules, err := identifierRules(t.engine.GetIdentifiers(), t.engine.GetDefaultSchema())
	if err != nil {
		return nil, err
	}
	identifiers, err := comparisonIdentifiers(rules)
	if err != nil {
		return nil, err
	}
	remote := &protocol.ReadSQLSchemaRequest{Engine: t.engine.GetId(), TargetFeatures: featureSet(t.engine.GetFeatures()), Identifiers: identifiers, DatabasePath: request.DatabasePath}
	for i, file := range request.Files {
		path := file.Path
		if strings.TrimSpace(path) == "" {
			path = "document " + strconv.Itoa(i+1)
		}
		remote.Documents = append(remote.Documents, &protocol.SQLDocument{Id: strconv.Itoa(i + 1), Path: path, Text: file.Text})
		limits, err := t.sqlHeaderLimits(file.Text)
		if err != nil {
			return nil, &sqlext.FileError{Path: file.Path, Err: err}
		}
		remote.Limits = append(remote.Limits, limits...)
	}
	return remote, nil
}

// sqlHeaderLimits returns the entries of a document's `ptah:not-described`
// header whose kind the engine declared, in written order. Entries of other
// kinds are left to the reader, which reads the header again and refuses a kind
// nobody knows.
func (t *target) sqlHeaderLimits(text string) ([]*protocol.SchemaCoverageLimit, error) {
	kinds := t.engine.GetSqlSchema().GetHeaderKinds()
	var limits []*protocol.SchemaCoverageLimit
	_, err := coverage.DecodeHeader(text, coverage.Vocabulary{}, func(object coverage.Object) (bool, error) {
		index := slices.IndexFunc(kinds, func(kind string) bool { return strings.EqualFold(kind, strings.TrimSpace(string(object.Kind))) })
		if index >= 0 {
			limits = append(limits, &protocol.SchemaCoverageLimit{Kind: kinds[index], Name: object.Name, Reason: string(object.Reason), Provenance: string(object.Provenance)})
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return limits, nil
}

// sqlSchemaRefusal is the provider's refusal of a load: the first error names
// its document, and every diagnostic is kept for a JSON report. A reply that
// is complete reports warnings only, which the reader has no channel for yet,
// so it is refused rather than read with its warnings dropped.
func sqlSchemaRefusal(request *protocol.ReadSQLSchemaRequest, reply *protocol.SQLSchema) error {
	paths := make(map[string]string, len(request.GetDocuments()))
	for _, document := range request.GetDocuments() {
		paths[document.GetId()] = document.GetPath()
	}
	refusal := &providerdiagnostic.SchemaRefusal{}
	for _, diagnostic := range reply.GetDiagnostics() {
		refusal.Diagnostics = append(refusal.Diagnostics, providerdiagnostic.SchemaDiagnostic{
			Code: strings.ToLower(diagnostic.GetCode().String()), Severity: strings.ToLower(diagnostic.GetSeverity().String()), Path: paths[diagnostic.GetDocumentId()],
			Start: schemaPosition(diagnostic.GetSpan().GetStart()), End: schemaPosition(diagnostic.GetSpan().GetEnd()), Message: diagnostic.GetMessage(),
		})
	}
	first := slices.IndexFunc(reply.GetDiagnostics(), func(diagnostic *protocol.SQLDiagnostic) bool {
		return diagnostic.GetSeverity() == protocol.SQLDiagnostic_ERROR
	})
	if first < 0 {
		// No error is a complete reading with warnings: the SDK refuses an
		// incomplete reply without an error. The first warning stands in.
		first = 0
		refusal.Cause = ptaherr.ErrUnsupportedFeature
		count := "a warning"
		if len(refusal.Diagnostics) > 1 {
			count = strconv.Itoa(len(refusal.Diagnostics)) + " warnings"
		}
		refusal.Message = fmt.Sprintf("the provider reported %s, which Ptah does not report for a SQL schema reading yet; the first: %s",
			count, positioned(refusal.Diagnostics[first]))
		return &sqlext.FileError{Path: refusal.Diagnostics[first].Path, Err: refusal}
	}
	if reply.GetDiagnostics()[first].GetCode() == protocol.SQLDiagnostic_UNSUPPORTED {
		refusal.Cause = ptaherr.ErrUnsupportedFeature
	}
	refusal.Message = positioned(refusal.Diagnostics[first])
	return &sqlext.FileError{Path: refusal.Diagnostics[first].Path, Err: refusal}
}

// positioned writes a diagnostic as the shared SQL parser writes a refusal:
// the reason, then where it starts.
func positioned(diagnostic providerdiagnostic.SchemaDiagnostic) string {
	return fmt.Sprintf("%s at position %d (line %d, column %d)", diagnostic.Message, diagnostic.Start.Offset, diagnostic.Start.Line, diagnostic.Start.Column)
}

func schemaPosition(position *protocol.SourcePosition) providerdiagnostic.SchemaPosition {
	return providerdiagnostic.SchemaPosition{Offset: position.GetByteOffset(), Line: position.GetLine(), Column: position.GetColumn()}
}

// sqlSchemaIdentity is how an engine reads its SQL schema documents, in the
// negotiated contract a plan binds: who reads the text, and which header kinds
// reach the provider as limits on its claim.
type sqlSchemaIdentity struct {
	Mode        string   `json:"mode"`
	HeaderKinds []string `json:"header_kinds,omitempty"`
}

func sqlSchemaIdentityOf(declaration *protocol.SQLSchemaDeclaration) *sqlSchemaIdentity {
	if declaration == nil {
		return nil
	}
	return &sqlSchemaIdentity{Mode: declaration.GetMode().String(), HeaderKinds: featureSet(declaration.GetHeaderKinds())}
}
