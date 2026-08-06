package lint

import (
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/tableref"
)

// schemaScope is the set of schema objects a lint run reviews.
//
// A lint run that validates against a dev database reviews what that database
// puts in scope. When the dev URL names one schema, an object a migration
// creates or destroys in a different schema was never part of the before-state
// the run compares against, so destroying it is not a covered change: it is
// neither a diagnostic nor a schema change. When the dev URL names no schema
// the whole database is under review and nothing is filtered.
//
// The zero value reviews everything, which is what every caller without a dev
// URL gets, and what keeps a run that cannot determine its boundary reporting
// more rather than less.
type schemaScope struct {
	name string
}

func newSchemaScope(name string) schemaScope {
	return schemaScope{name: strings.TrimSpace(name)}
}

// unrestricted reports whether the scope covers every schema.
func (s schemaScope) unrestricted() bool {
	return s.name == ""
}

// allowsSchema reports whether a schema name is the reviewed one.
//
// The comparison folds case because PostgreSQL folds unquoted identifiers, and
// because the wrong answer in that direction keeps an object under review
// rather than silencing it.
func (s schemaScope) allowsSchema(name string) bool {
	if s.unrestricted() {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(name), s.name)
}

// allowsObject reports whether a source-spelled object reference denotes an
// object under review.
//
// An unqualified reference resolves into the reviewed schema, so it is always
// in scope. A reference that does not parse has no recoverable schema, and is
// kept in scope for the same reason an unreadable DROP target stays reported:
// an unreadable name must not be able to silence a rule.
func (s schemaScope) allowsObject(reference string) bool {
	if s.unrestricted() {
		return true
	}
	ref, ok := tableref.Parse(reference)
	if !ok || !ref.Qualified {
		return true
	}
	return s.allowsSchema(ref.Schema)
}

// allowsSubject reports whether one finding subject is under review. A column
// belongs to its table's schema, never to its own name.
func (s schemaScope) allowsSubject(subject Subject) bool {
	reference := subject.Name
	if subject.Kind == SubjectColumn {
		reference = subject.Parent
	}
	if strings.TrimSpace(reference) == "" {
		return true
	}
	return s.allowsObject(reference)
}

// allowsFinding reports whether a finding names at least one object under
// review.
//
// A finding that names no object at all is kept: statement rules report the
// statement rather than the objects in it (see [runRules]), so there is nothing
// to measure the scope against, and dropping it would silence a hazard on the
// strength of a boundary that was never established.
func (s schemaScope) allowsFinding(finding Finding) bool {
	if s.unrestricted() || finding.Context == nil || len(finding.Context.Subjects) == 0 {
		return true
	}
	return slices.ContainsFunc(finding.Context.Subjects, s.allowsSubject)
}

// keepChange returns the change when the object it names is under review, and
// nothing when it is not. An object the node grammar exposes no name for (an
// opaque routine body, a COMMENT ON target) is kept, for the same reason an
// unparsable reference is.
func (s schemaScope) keepChange(change SchemaChange, object string) []SchemaChange {
	if !s.allowsObject(object) {
		return nil
	}
	return []SchemaChange{change}
}

// keepFindings returns the findings that name an object under review, in the
// order given.
func (s schemaScope) keepFindings(findings []Finding) []Finding {
	if s.unrestricted() {
		return findings
	}
	kept := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		if s.allowsFinding(finding) {
			kept = append(kept, finding)
		}
	}
	return kept
}
