package schemascope

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/offlinenames"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tableref"
)

// Pairing is how a target compares the schema and relation names a selection
// meets on the two sides of a comparison: a --schemas name, and the names an
// --include or --exclude selector matches.
//
// A selector matches a name as it is written, and the two sides of a
// comparison may write one name two ways. A server started with
// lower_case_table_names=1 reads the declared database Sales and the database
// sales it holds as one database, and a SQL Server database under a
// case-insensitive collation reads the schemas sales and Sales as one. A
// selection that keeps one spelling and leaves out the other makes the plan
// drop what the document declares. Measured on MySQL 8.4.13 and 26.10.0 and
// MariaDB 11.8.9 and 12.3.3 with lower_case_table_names=1, and on SQL Server
// 2025 under Latin1_General_100_CI_AS (stokaro/ptah#6785):
//
//   - `schema apply --schemas sales` for a document that declares Sales.t
//     planned DROP DATABASE `sales` on the MySQL family, and `--schemas Sales`
//     for one that declares sales.t planned DROP TABLE [Sales].[t] on SQL
//     Server;
//   - `--exclude Sales.u` left out the declared table and kept the held one,
//     and the plan dropped the table the selector was there to protect;
//   - `--include sales.t` kept the held table and left out the declared one,
//     and the plan dropped it.
//
// A comparison with a selection therefore spells every name the target reads
// as one name the way the target holds it, on both sides, before the
// selection runs: the --schemas names ([Pairing.ScopeNames]) and the
// document ([Pairing.Respell]). A selector then meets one spelling, and keeps
// both sides of a name or neither. A selector spelled in another case than
// the server holds the name in matches neither side, which a command refuses
// as a selection that matched nothing.
//
// Where no connection said how the target compares names, a name that matches
// another only without regard to case is refused, rather than selected apart:
// whether the two are one name decides what the plan drops.
type Pairing struct {
	rule    schemaprep.NameRule
	session string
	dialect string
}

// PairingOf is the pairing of a comparison with the server info describes. On
// the MySQL family it is the folding the connection read,
// [catalog.ServerInfo.NameFolding]. On a target whose catalog compares names
// ([offlinenames.Unanswered] of its offline rules), such as SQL Server, it is
// resolved: the semantics a connection resolved for the names the comparison
// meets ([PairingNames]), and nil where none was resolved, which refuses a
// name that differs from another only in case. Any other target compares the
// names as bytes, and its pairing changes nothing.
func PairingOf(info catalog.ServerInfo, resolved *identifier.Semantics) Pairing {
	pairing := Pairing{session: info.Schema, dialect: info.Dialect}
	switch {
	case platform.NamesFoldBySetting(info.Dialect):
		pairing.rule = schemaprep.FoldingRule(info.NameFolding)
	case offlinenames.Unanswered(identifier.ForDialect(info.Dialect)):
		// A snapshot whose mappings are not canonical normalizes to the
		// offline rules, which answer nothing about the collation.
		var semantics identifier.Semantics
		if resolved != nil {
			semantics = resolved.Normalize(info.Dialect)
		}
		if semantics.TableNames != identifier.ComparisonCatalogResolved {
			pairing.rule = schemaprep.UnansweredCatalogRule()
			break
		}
		pairing.rule = schemaprep.CatalogRule(semantics.TableIdentityKey)
	}
	return pairing
}

// Exact reports whether the pairing compares names as bytes, so that it
// respells nothing and refuses nothing.
func (p Pairing) Exact() bool {
	return p.rule.Exact()
}

// Resolver is a connection that answers how its catalog compares names.
// [ptah.run/dbschema.DatabaseConnection] is one.
type Resolver interface {
	ResolveIdentifierSemantics(ctx context.Context, names []string) (identifier.Semantics, error)
}

// Target is the target a pairing is resolved for: the server Info describes,
// and Names, the connection that answers how its catalog compares names. A
// nil Names answers nothing, and the pairing of a target whose catalog
// compares names then refuses a name that differs from another only in case.
type Target struct {
	Info  catalog.ServerInfo
	Names Resolver
}

// Pairing is [PairingOf] for the target: on a target whose catalog compares
// names, it asks t.Names about every name [PairingNames] lists for
// requested, desired and current, and pairs by the answer.
func (t Target) Pairing(
	ctx context.Context,
	requested []string,
	desired *schemamodel.Database,
	current *catalog.Database,
) (Pairing, error) {
	return t.pairingOf(ctx, PairingNames(requested, desired, current))
}

// pairingOf is the pairing of the target for names.
func (t Target) pairingOf(ctx context.Context, names []string) (Pairing, error) {
	if t.Names == nil || !offlinenames.Unanswered(identifier.ForDialect(t.Info.Dialect)) {
		return PairingOf(t.Info, nil), nil
	}
	resolved, err := t.Names.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return Pairing{}, fmt.Errorf("compare schema names under the database collation: %w", err)
	}
	return PairingOf(t.Info, &resolved), nil
}

// Respell is desired with each schema and relation name it declares spelled as
// the target holds the name in current, for a selection about to meet both:
// [Pairing.Respell] under the pairing of the target for the names of both.
// A target that compares names as bytes returns desired as it came.
func (t Target) Respell(ctx context.Context, desired *schemamodel.Database, current *catalog.Database) (*schemamodel.Database, error) {
	if desired == nil || current == nil || PairingOf(t.Info, nil).Exact() {
		return desired, nil
	}
	pairing, err := t.Pairing(ctx, nil, desired, current)
	if err != nil {
		return nil, err
	}
	return pairing.Respell(desired, current)
}

// SpellAsHeld is requested spelled as the target holds the schemas they name,
// where held is the schemas the target holds: see [Pairing.ScopeNames]. A
// target that compares names as bytes returns requested as it came.
func (t Target) SpellAsHeld(ctx context.Context, requested, held []string) ([]string, error) {
	if len(requested) == 0 || PairingOf(t.Info, nil).Exact() {
		return requested, nil
	}
	pairing, err := t.pairingOf(ctx, slices.Concat(requested, held))
	if err != nil {
		return nil, err
	}
	return pairing.ScopeNames(requested, held, nil)
}

// PairingNames lists the names a pairing compares: requested, and the schema
// and relation names of desired and current, each schema and each name of a
// qualified name on its own.
func PairingNames(requested []string, desired *schemamodel.Database, current *catalog.Database) []string {
	names := slices.Clone(requested)
	split := func(values ...string) {
		for _, value := range values {
			if ref, ok := tableref.Parse(value); ok {
				names = append(names, ref.Schema, ref.Name)
				continue
			}
			names = append(names, value)
		}
	}
	if desired != nil {
		for _, schema := range desired.Schemas {
			names = append(names, schema.Name)
		}
		for _, table := range desired.Tables {
			names = append(names, table.Schema)
			split(table.Name)
		}
		for _, view := range desired.Views {
			split(view.Name)
		}
		for _, sequence := range desired.Sequences {
			names = append(names, sequence.Schema)
			split(sequence.Name)
		}
	}
	read := schemaprep.ReadNamesOf(current, "", "")
	names = append(names, read.Databases...)
	for _, relation := range read.Relations {
		names = append(names, relation.Name)
	}
	return UniqueNames(names)
}

// ScopeNames spells each of requested as the target holds the schema it names:
// as the one of held, the schemas the target holds, that the target reads as
// it, and otherwise as the one of declared, the schemas the document
// declares, that the target reads as it. A name the target holds as written
// stays as written, and so does a name nothing matches. The order and the
// repeats of requested are kept.
//
// A name that matches more than one spelling is refused, and so is a name that
// would be respelled where no connection said how the target compares names.
func (p Pairing) ScopeNames(requested, held, declared []string) ([]string, error) {
	if len(requested) == 0 {
		return requested, nil
	}
	respelled := make([]string, len(requested))
	for i, name := range requested {
		spelled, err := p.scopeName(name, held, declared)
		if err != nil {
			return nil, err
		}
		respelled[i] = spelled
	}
	return respelled, nil
}

// scopeName is name spelled by [Pairing.ScopeNames]. The held spellings come
// first, matched exactly and then by the rule, because [Pairing.Respell]
// spells the document as the target holds a name: a scope that kept the
// document's spelling of a held schema would match neither side once the
// document is respelled.
func (p Pairing) scopeName(name string, held, declared []string) (string, error) {
	for _, spellings := range [][]string{held, declared} {
		if slices.Contains(spellings, name) {
			return name, nil
		}
		respelled, err := schemaprep.RespellName(p.rule, name, spellings)
		if err != nil || respelled != name {
			return respelled, err
		}
	}
	return name, nil
}

// Respell is desired with each schema and relation name the target reads as a
// name current holds spelled as current holds it; see
// [schemaprep.RespellReadNamesBy], which also refuses two declarations the
// target reads as one name.
func (p Pairing) Respell(desired *schemamodel.Database, current *catalog.Database) (*schemamodel.Database, error) {
	if desired == nil || current == nil {
		return desired, nil
	}
	return schemaprep.RespellReadNamesBy(desired, schemaprep.ReadNamesOf(current, p.session, p.dialect), p.rule)
}

// Selection is [Pairing.ScopeNames] of requested against the schemas current
// holds and desired declares, and [Pairing.Respell] of desired: the scope
// and the document a selection between desired and current meets.
func (p Pairing) Selection(
	requested []string,
	desired *schemamodel.Database,
	current *catalog.Database,
) ([]string, *schemamodel.Database, error) {
	return p.selection(requested, desired, schemaprep.ReadNamesOf(current, p.session, p.dialect))
}

// DocumentSelection is [Pairing.Selection] for a comparison of two documents,
// where held is the document that stands for what the target holds: the --from
// side of a diff.
func (p Pairing) DocumentSelection(
	requested []string,
	desired, held *schemamodel.Database,
) ([]string, *schemamodel.Database, error) {
	return p.selection(requested, desired, schemaprep.DeclaredNamesOf(held, p.session, p.dialect))
}

func (p Pairing) selection(
	requested []string,
	desired *schemamodel.Database,
	read schemaprep.ReadNames,
) ([]string, *schemamodel.Database, error) {
	declared, err := DeclaredSchemaNames(desired)
	if err != nil {
		return nil, nil, err
	}
	scope, err := p.ScopeNames(requested, read.Databases, declared)
	if err != nil {
		return nil, nil, err
	}
	if desired == nil || p.Exact() {
		return scope, desired, nil
	}
	respelled, err := schemaprep.RespellReadNamesBy(desired, read, p.rule)
	if err != nil {
		return nil, nil, err
	}
	return scope, respelled, nil
}
