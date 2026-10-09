// Package ydb renders Ptah AST nodes to YQL DDL for YDB row tables.
//
// YDB is its own dialect rather than a member of the PostgreSQL or MySQL
// family, and the renderer decides with capability keys rather than with its
// own name: a YDB release that gains an ability is a preset change, and the
// statements here follow it. Where YDB has no counterpart for a declaration,
// the render is refused with a [ptaherr.CapabilityError] naming the key, never
// rendered as something else and never answered with a comment that lets an
// apply exit 0 without the object. Unsupported object families are refused
// before a statement is rendered.
//
// Every statement ends with a semicolon on a line of its own, so the YQL
// splitter yields one statement per DDL. That is what YDB needs: a query is
// compiled against the schema as it stood before the query, so `ALTER TABLE t
// ADD COLUMN v` and `ALTER TABLE t ADD INDEX i ON (v)` fail as one query and
// succeed as two (measured on 26.2.1.14).
package ydb

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/engine/builtin/internal/dialects/internal/bufwriter"
	"ptah.run/internal/renderdiag"
	"ptah.run/internal/sqlident"
)

// DialectName is the dialect this renderer writes.
const DialectName = platform.YDB

// Renderer writes YQL DDL for one capability set.
type Renderer struct {
	w    bufwriter.Writer
	caps capability.Capabilities
	// sink receives a record for each declaration YDB keeps less of than was
	// declared -- a string length, a timestamp precision. A nil sink drops
	// what it is given.
	sink *renderdiag.Sink
}

// New constructs a renderer for the newest YDB line Ptah measured.
func New() *Renderer {
	return NewWithCapabilities(capability.YDB262())
}

// NewWithCapabilities constructs a renderer for a concrete server capability
// set. The set is cloned so later caller mutations cannot change what renders.
func NewWithCapabilities(caps capability.Capabilities) *Renderer {
	return &Renderer{caps: caps.Clone()}
}

// ReportOmissionsTo directs this renderer's omission records to sink.
func (r *Renderer) ReportOmissionsTo(sink *renderdiag.Sink) {
	r.sink = sink
}

// Dialect returns the dialect this renderer writes.
func (r *Renderer) Dialect() string { return DialectName }

// GetDialect returns the dialect this renderer writes.
func (r *Renderer) GetDialect() string { return r.Dialect() }

// Reset clears the output buffer.
func (r *Renderer) Reset() { r.w.Reset() }

// Output returns what was rendered since the last Reset.
func (r *Renderer) Output() string { return r.w.Output() }

// GetOutput returns what was rendered since the last Reset.
func (r *Renderer) GetOutput() string { return r.Output() }

// Render renders one node from a clean buffer.
func (r *Renderer) Render(node ast.Node) (string, error) {
	r.Reset()
	if err := node.Accept(r); err != nil {
		r.Reset()
		return "", err
	}
	return r.Output(), nil
}

// quote quotes one identifier the way YQL reads it.
func quote(name string) string {
	return sqlident.Quote(DialectName, name)
}

// tablePath writes a table reference as one quoted YDB path. A Ptah schema is
// a directory on YDB, so `app.users` is `app/users`; a reference whose parts
// are already quoted keeps the dots inside them, because a YDB table name may
// contain one.
func tablePath(name string) string {
	return quote(ydbscheme.ObjectPath(name))
}

// refuseKey refuses a declaration the target lacks the capability for.
func refuseKey(key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{
		Dialect: DialectName,
		Feature: string(key),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target",
			subject, key, DialectName),
	}
}

// refuseUnwritten refuses a declaration whose key the target holds and whose
// YQL spelling this renderer does not write. It is the answer a preset that
// claims a key ahead of its renderer gets, instead of a silent drop.
func refuseUnwritten(feature, subject string) error {
	return &ptaherr.CapabilityError{
		Dialect: DialectName,
		Feature: feature,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s renderer writes no %s", subject, DialectName, feature),
	}
}

// refuseFact refuses a declaration no YDB line can hold and no capability key
// describes, with the reason the server gives.
func refuseFact(subject, reason string) error {
	return &ptaherr.CapabilityError{
		Dialect: DialectName,
		Feature: subject,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s", subject, reason),
	}
}

// keyed refuses through refuseKey when the target lacks key and through
// refuseUnwritten when it holds it: both are refusals, and only the first is
// a statement about the server.
func (r *Renderer) keyed(key capability.Capability, feature, subject string) error {
	if !r.caps.Has(key) {
		return refuseKey(key, subject)
	}
	return refuseUnwritten(feature, subject)
}
