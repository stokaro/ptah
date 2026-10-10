package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/migration/safety"
)

type unknownPayload struct{}

func (*unknownPayload) Kind() schemaext.Kind                 { return "example.org/unknown" }
func (*unknownPayload) CloneExtension() ast.ExtensionPayload { return &unknownPayload{} }

type unclassifiedPayload struct {
	unknownPayload
	effect schemaext.Effect
}

func (p *unclassifiedPayload) CloneExtension() ast.ExtensionPayload {
	return &unclassifiedPayload{effect: p.effect}
}
func (p *unclassifiedPayload) Effect() schemaext.Effect { return p.effect }

func TestAssess_UnknownExtensionsRequireReview(t *testing.T) {
	payloads := []ast.ExtensionPayload{
		nil, (*unknownPayload)(nil), &unknownPayload{}, &unclassifiedPayload{},
		&unclassifiedPayload{effect: schemaext.Effect{Impact: schemaext.Additive}},
		&unclassifiedPayload{effect: schemaext.Effect{Impact: "future", Reason: "not understood"}},
	}
	for _, payload := range payloads {
		c := qt.New(t)
		nodes := []ast.Node{
			(*ast.ExtensionStatement)(nil),
			(*ast.ExtensionAlterOperation)(nil),
			&ast.ExtensionStatement{Payload: payload},
			&ast.StatementList{Statements: []ast.Node{&ast.ExtensionStatement{Payload: payload}}},
			&ast.ExtensionAlterOperation{Payload: payload},
			&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: payload}}},
		}
		for _, assessment := range safety.Assess(nodes) {
			c.Assert(assessment.Severity, qt.Equals, safety.Destructive)
			c.Assert(assessment.Reason, qt.Equals, "extension effects are unknown; manual review is required")
		}
	}
}

type scopedPayload struct{ subject objectidentity.ID }

func (*scopedPayload) Kind() schemaext.Kind { return "example.org/scoped" }
func (p *scopedPayload) CloneExtension() ast.ExtensionPayload {
	return &scopedPayload{subject: p.subject}
}
func (p *scopedPayload) Subject() objectidentity.ID { return p.subject }
func (*scopedPayload) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes routing"}
}

func TestAssessExtensionPreservesScopedSubject(t *testing.T) {
	c := qt.New(t)
	ref := objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts("example.org/scoped", "jobs", "route.daily")
	assessments := safety.Assess([]ast.Node{&ast.ExtensionStatement{Payload: &scopedPayload{subject: ref}}})
	c.Assert(assessments, qt.HasLen, 1)
	// The dotted name is quoted, so it cannot read as a third component
	// (stokaro/ptah#4276).
	c.Assert(assessments[0].Subject, qt.Equals, `jobs."route.daily"`)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Warning)
}
