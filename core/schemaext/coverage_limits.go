package schemaext

import (
	"fmt"
	"maps"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
)

// LimitMapping says that a kind of the common not-described vocabulary also
// limits an owner's model. A description that says it does not describe
// Header does not describe Model either: a document's `ptah:not-described
// policy` header, written because the document left its policy blocks out,
// says nothing about the policies the database holds.
//
// The common vocabulary ([coverage.Set]) records what a description declines
// as kinds and names, and a model's [Coverage] records it as knowledge of
// subjects. The mapping is the owner's statement of how the two meet, so the
// runtime can fold one into the other before the owner compares; see
// [Coverage.WithLimits].
type LimitMapping struct {
	// Header is the not-described kind, such as [coverage.Policy].
	Header coverage.Kind
	// Model is the owner's named-object model the kind limits.
	Model Kind
	// Names returns the names a not-described record may use for one subject
	// of Model. A record names an object, not its identity, so the owner says
	// which spellings name it.
	Names func(objectidentity.ID) []string
}

// Validate refuses a mapping that names no kind, no model, or no names.
// Whether the kinds exist is the registering runtime's question.
func (m LimitMapping) Validate() error {
	if strings.TrimSpace(string(m.Header)) == "" || !m.Model.Valid() || m.Names == nil {
		return fmt.Errorf("%w: a coverage limit mapping needs a not-described kind, a model and its names", ErrInvalidValue)
	}
	return nil
}

// WithLimits returns c with the records of limits that m maps folded into
// m.Model's knowledge. A record naming the whole kind leaves the model
// uninspected. A record naming one object leaves uninspected each subject in
// subjects that it names, which are the subjects a comparison asks about: the
// objects the other side holds. The record's reason and provenance become the
// reason of the knowledge.
//
// A model c does not enroll is already uninspected and is left alone, and so
// is a subject c makes an explicit claim about: the claim is the source's own
// statement about that one object. Records of other kinds, and records naming
// a schema, change nothing; the comparison projects a schema's limits on its
// own. The receiver is not changed.
func (c Coverage) WithLimits(limits coverage.Set, m LimitMapping, subjects []objectidentity.ID) (Coverage, error) {
	if err := m.Validate(); err != nil {
		return Coverage{}, err
	}
	record, enrolled := c.kinds[m.Model]
	if !enrolled || limits.IsZero() {
		return c, nil
	}
	result := Coverage{representation: c.representation, kinds: maps.Clone(c.kinds), subjects: maps.Clone(c.subjects)}
	if limit, found := limits.Limit(m.Header); found {
		if record.Knowledge.State != Uninspected && record.Knowledge.State != Unrepresentable {
			record.Knowledge = Knowledge{State: Uninspected, Reason: limitReason(limit)}
			result.kinds[m.Model] = record
		}
		return result, nil
	}
	for _, subject := range subjects {
		if Kind(subject.Kind) != m.Model {
			continue
		}
		key := subjectKey{kind: m.Model, ref: subject.Key()}
		if _, claimed := result.subjects[key]; claimed {
			continue
		}
		limit, found := limits.Limit(m.Header, m.Names(subject)...)
		if !found {
			continue
		}
		result.subjects[key] = SubjectCoverage{Kind: m.Model, Subject: subject,
			Knowledge: Knowledge{State: Uninspected, Reason: limitReason(limit)}}
	}
	return result, nil
}

// limitReason is the knowledge reason a not-described record gives: the kind
// the description declined and, when the record says, why.
func limitReason(limit coverage.Object) string {
	reason := fmt.Sprintf("the description does not describe %s", limit.Kind)
	if !limit.WholeKind() {
		reason += fmt.Sprintf(" %q", limit.Name)
	}
	if explained := limit.Explain(); explained != "" {
		reason += ": " + explained
	}
	return reason
}
