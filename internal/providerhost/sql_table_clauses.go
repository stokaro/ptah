package providerhost

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/providersource"
	"ptah.run/internal/tableref"
)

// tableClauseExtension is the SQL extension of an engine that reads the
// shared grammar (the FRAGMENTS mode) and claims clauses of CREATE TABLE
// (provider.SQLTableClausesFeature): one pattern among a table's options for
// each claim, targeted at the engine's documents, so a document of another
// engine or of none reads the clause as it did, and ClickHouse keeps its TTL.
// The parser sends a document's clauses to the provider in one call, and the
// facets it decodes are the tables'. A document of the engine states each
// claimed model in full for every table it declares, so its claim of them is
// complete: a table without the clause has none. It reports false for an
// engine without claims, of which the SDK keeps none unless both sides listed
// the feature.
func (h *Host) tableClauseExtension(engine *protocol.Engine) (sqlext.Extension, bool) {
	claims := engine.GetSqlSchema().GetTableClauses()
	if len(claims) == 0 || engine.GetSqlSchema().GetMode() == protocol.SQLSchemaDeclaration_DOCUMENT {
		return sqlext.Extension{}, false
	}
	extension := sqlext.Extension{Owner: h.description.GetProviderId(), Targets: []string{engine.GetId()},
		Parser: tableClauseParser{host: h, engine: engine}}
	for i, claim := range claims {
		extension.Patterns = append(extension.Patterns, sqlext.Pattern{Name: tableClausePattern(i), Site: sqlext.TableOption, Words: slices.Clone(claim.GetWords()),
			Targeted: true})
		for _, kind := range claim.GetModels() {
			if !slices.Contains(extension.Facets, schemaext.Kind(kind)) {
				extension.Facets = append(extension.Facets, schemaext.Kind(kind))
			}
		}
	}
	extension.Coverage = func(sqlext.CoverageRequest) (schemaext.Coverage, error) {
		return h.tableClauseCoverage(engine.GetId(), extension.Facets)
	}
	return extension, true
}

// tableClausePattern names the pattern of the engine's claim at index, which
// each claim the parser receives carries back.
func tableClausePattern(index int) string {
	return "table-clause-" + strconv.Itoa(index)
}

// tableClauseCoverage is the claim a SQL document of engine makes of the
// models its table clauses declare: each complete for every table, as far as
// SQL describes the model for every engine of the provider that declares it.
func (h *Host) tableClauseCoverage(engine string, kinds []schemaext.Kind) (schemaext.Coverage, error) {
	records := make([]schemaext.KindCoverage, 0, len(kinds))
	for _, kind := range kinds {
		contract := h.models[modelKey{kind: kind, representation: schemaext.Desired}]
		if contract == nil {
			return schemaext.Coverage{}, fmt.Errorf("%w: table clause model %q is not registered", provider.ErrProtocol, kind)
		}
		records = append(records, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: h.description.GetProviderId(), Kind: kind,
			Representation: schemaext.Desired, Version: 1, Definition: contract.signature},
			Knowledge: schemaext.GlobalKnowledge(h.sourceModelKnowledge(engine, contract, protocol.SourceDocument_SQL))})
	}
	return schemaext.NewCoverage(schemaext.Desired, records, nil)
}

// tableClauseParser reads the claimed clauses of one document of an engine
// through the provider's DecodeSource.
type tableClauseParser struct {
	host   *Host
	engine *protocol.Engine
}

// ParseSQL answers every claim of the document with the facets the provider
// decodes from its clause, each bound to the engine. The clauses go in one
// call; when the provider refuses it, each goes again alone, so the refusal
// states the place of the clause it is about.
func (p tableClauseParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	clauses := make([]providersource.TableClause, len(request.Claims))
	for i, claim := range request.Claims {
		clause, err := p.clause(claim)
		if err != nil {
			return sqlext.Result{}, err
		}
		clauses[i] = clause
	}
	values, err := p.decode(ctx, request.Length, clauses)
	if err == nil {
		return sqlext.Result{Values: values}, nil
	}
	if ctx.Err() != nil || len(clauses) == 1 {
		return sqlext.Result{}, p.refusal(request.Claims[0], err)
	}
	for i, clause := range clauses {
		if _, alone := p.decode(ctx, request.Length, []providersource.TableClause{clause}); alone != nil {
			return sqlext.Result{}, p.refusal(request.Claims[i], alone)
		}
	}
	return sqlext.Result{}, p.refusal(request.Claims[0], err)
}

// clause is the captured clause of claim: the engine's claim the pattern
// stands for, the table the clause belongs to, its text and where it starts.
func (p tableClauseParser) clause(claim sqlext.Claim) (providersource.TableClause, error) {
	claims := p.engine.GetSqlSchema().GetTableClauses()
	index := -1
	for i := range claims {
		if claim.Pattern == tableClausePattern(i) {
			index = i
		}
	}
	if claim.Site != sqlext.TableOption || index < 0 {
		return providersource.TableClause{}, fmt.Errorf("%w: SQL table clause pattern %q at %s", schemaext.ErrInvalidValue, claim.Pattern, claim.Site)
	}
	table, valid := tableref.Parse(claim.Subject)
	if !valid {
		return providersource.TableClause{}, ptaherr.At(claim.Offset, fmt.Errorf("%w: a table clause of table %q, whose name Ptah cannot read", ptaherr.ErrInvalidAttributeValue, claim.Subject))
	}
	return providersource.TableClause{Claim: claims[index], Schema: table.Schema, Table: table.Name, Text: claim.Text, Start: claim.Written}, nil
}

// decode sends clauses to the provider as one SQL document and answers each
// with the facets it decodes, in order.
func (p tableClauseParser) decode(ctx context.Context, length int, clauses []providersource.TableClause) ([]sqlext.Value, error) {
	request, err := providersource.SQLDocument(ctx, p.engine, "", length, clauses)
	if err != nil {
		return nil, err
	}
	decoded, err := p.host.client.DecodeSource(ctx, request)
	if err != nil {
		return nil, err
	}
	fragments := decoded.GetDocuments()[0].GetFragments()
	values := make([]sqlext.Value, len(fragments))
	for i, fragment := range fragments {
		for _, value := range fragment.GetValues() {
			facets, err := p.facet(values[i].Facets, value)
			if err != nil {
				return nil, err
			}
			values[i].Facets = facets
		}
	}
	return values, nil
}

// facet adds one decoded value to facets. The SDK has checked that it is a
// facet of the clause's table, of a model the claim declares.
func (p tableClauseParser) facet(facets schemaext.Facets, value *protocol.SourceModelValue) (schemaext.Facets, error) {
	decoded, err := p.host.decodedValue(p.engine.GetId(), value)
	if err != nil {
		return schemaext.Facets{}, err
	}
	if err := decoded.requireTable("SQL"); err != nil {
		return schemaext.Facets{}, err
	}
	facets, err = facets.With(decoded.value)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(decoded.kind, p.engine.GetId())
}

// refusal states the provider's refusal of a clause at the clause's place in
// the document, which the reader of the document turns into a line and a
// column. A refusal that already states a place keeps it.
func (p tableClauseParser) refusal(claim sqlext.Claim, err error) error {
	if _, placed := errors.AsType[*ptaherr.ParseError](err); placed {
		return err
	}
	return ptaherr.At(claim.Offset, fmt.Errorf("table clause of %s: %w", claim.Subject, err))
}
