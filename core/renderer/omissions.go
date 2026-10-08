package renderer

import (
	"fmt"
	"strings"
)

// validateOmissions keeps AST and whole-schema replies on the same reporting
// contract, so a provider cannot lose required fields at one boundary only.
func validateOmissions(omissions []Omission) error {
	for _, omission := range omissions {
		if strings.TrimSpace(omission.Dialect) == "" || strings.TrimSpace(omission.Kind) == "" || strings.TrimSpace(omission.Reason) == "" {
			return fmt.Errorf("%w: omission requires target, kind, and reason", ErrInvalidResult)
		}
	}
	return nil
}

// Omission is one declaration a target did not render.
//
// A render answers a declaration by emitting it, by refusing the whole render
// with an error, or by continuing without it. Only the third answer produces an
// Omission, and it is the answer nothing else observes: a
// PostgreSQL-family target writes a `skipped` comment beside the statement
// while SQLite, SQL Server and Oracle drop the same table options without a
// word, and both spellings exit 0 (stokaro/ptah#2976).
//
// Kind and Name identify the object that owned the declaration. Property names
// what the object lost and is empty when the object itself was lost. Detail
// carries the declared value where repeating it helps a reader recognize what
// went missing, and Remedy is set only where a remedy exists on this target.
//
// An Omission is not a portability verdict. A declaration excluded from a
// target by a `dialects=` scope is not part of that target's desired state at
// all, so it is absent rather than omitted and produces nothing here.
type Omission struct {
	// Dialect is the normalized target the render was for.
	Dialect string
	// Reason is a stable token for why the declaration did not reach the
	// output. Branch on it rather than on Message, whose wording is free to
	// change.
	Reason string
	// Kind is the owning object's kind, such as "table".
	Kind string
	// Name is the owning object's name as the declaration spells it.
	Name string
	// Property names the lost property, empty when the whole object was lost.
	Property string
	// Detail is the declared value, empty when there is nothing to repeat.
	Detail string
	// Remedy states how to keep the declaration on this target, empty when
	// there is no remedy that works here.
	Remedy string
}

// Message states the loss in one sentence, without naming the object.
//
// The object's identity is in Kind and Name, so a caller that already prints
// those does not repeat them. The wording is presentation and may change; a
// caller deciding anything reads Reason.
func (o Omission) Message() string {
	subject := o.Property
	if subject == "" {
		subject = o.Kind + " " + o.Name
	}
	if o.Detail != "" {
		subject += "=" + o.Detail
	}
	return subject + " would be skipped"
}
