package schemaext

import (
	"strconv"
	"strings"

	"ptah.run/core/objectidentity"
)

// OffTarget is what a host does with an object of an owner's model on a
// target the owner does not serve: one where no owner compares or plans the
// model. A declaration written without a target scope reaches every target,
// so the owner states once, for its model, which answer is safe.
type OffTarget string

const (
	// OffTargetRefuse refuses the declaration and names the targets that
	// host the model, so its author can scope it to them. It is the answer
	// for a model whose absence is the dangerous direction, such as a
	// row-level security policy: skipping access control in silence would
	// leave rows readable.
	OffTargetRefuse OffTarget = "refuse"
	// OffTargetSkip leaves the object out and reports one line saying so.
	// It is the answer for a model whose absence the target does not notice,
	// such as a PostgreSQL extension in a schema that also targets MySQL.
	OffTargetSkip OffTarget = "skip"
)

// Valid reports whether a is one of the declared answers.
func (a OffTarget) Valid() bool { return a == OffTargetRefuse || a == OffTargetSkip }

// SkippedObject is one object a host left out on a target its owner does not
// serve, because the owner answered [OffTargetSkip]. It is reported, never
// planned: a comparison carries it beside its changes, and a render or a plan
// writes [SkippedObject.Note] where the object would have been.
type SkippedObject struct {
	// Subject is the object's identity.
	Subject objectidentity.ID `json:"subject"`
	// Label names the object's model for a reader, such as "PostgreSQL
	// extension".
	Label string `json:"label"`
	// Target is the target the object was left out on.
	Target string `json:"target"`
}

// Note is the sentence a render or a plan writes for the object: its model,
// its name as the declaration spelled it, and that the target skipped it.
func (s SkippedObject) Note() string {
	return s.Label + " " + strconv.Quote(DisplayName(s.Subject)) + " is not supported by this target; skipped."
}

// DisplayName joins the parts of an identity, as the source spelled them, for
// a reader: the catalog, schema and parent that are set, then the name.
func DisplayName(ref objectidentity.ID) string {
	var parts []string
	for _, part := range []objectidentity.Part{ref.Catalog, ref.Schema, ref.Parent, ref.Name} {
		if source := part.Authored(); source != "" {
			parts = append(parts, source)
		}
	}
	name := strings.Join(parts, ".")
	if ref.Signature != "" {
		name += "(" + ref.Signature + ")"
	}
	return name
}

// OffTargetScoper is a runtime that leaves out, for one target, the objects
// whose owners answer [OffTargetSkip] there, and refuses the ones whose
// owners answer [OffTargetRefuse]. A caller that converts or compares objects
// for a target asks it first, so an object the target skips does not reach a
// service the target does not have.
type OffTargetScoper interface {
	// ScopeOffTarget returns objects without the ones target skips, and those
	// it skipped in identity order. objects is not changed. The error wraps
	// [ptah.run/core/ptaherr.ErrUnsupportedFeature] for a refused object.
	ScopeOffTarget(target string, objects Objects) (Objects, []SkippedObject, error)
}
