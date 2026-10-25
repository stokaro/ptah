package lintext

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/platform/capability"
)

// Target is the server a lint run plans against.
type Target struct {
	// Name is the canonical target name, empty for a run that names none.
	// Such a run sends every rule, over the host's dialect-blind split.
	Name string `json:"name"`
	// Version is the server version the run plans against, as the operator
	// wrote it or the server reported it, empty when the run named none.
	Version string `json:"version,omitempty"`
	// Capabilities are what a server of that version can do.
	Capabilities capability.Capabilities `json:"capabilities"`
}

// File is one migration file of a lint run.
type File struct {
	// Name is the file's slash-separated path relative to the linted
	// directory.
	Name string `json:"name"`
	// Version is the file's migration version, zero when its name carries
	// none.
	Version int64 `json:"version,omitempty"`
	// Up and Down report which half of a migration the file is. A file may
	// be neither, and then no statement rule judges it.
	Up   bool `json:"up,omitempty"`
	Down bool `json:"down,omitempty"`
	// HasPair reports that the file's other half exists.
	HasPair bool `json:"has_pair,omitempty"`
	// NoTransaction reports that the file runs outside a transaction.
	NoTransaction bool `json:"no_transaction,omitempty"`
	// Repeatable reports a repeatable migration rather than an ordered
	// version.
	Repeatable bool `json:"repeatable,omitempty"`
	// Statements are the file's statements, split by the target's owner, or
	// by the host where the run names no target. A statement's Line counts
	// the lines of the file as written, which differ from the lines of the
	// split SQL where the host analyzes rendered or extracted text.
	Statements []Statement `json:"statements"`
}

// CheckRequest asks one owner for its findings over a whole lint run.
type CheckRequest struct {
	// Target is the server the run plans against.
	Target Target `json:"target"`
	// Codes are the owner's rules the run enables. The owner reports no
	// other.
	Codes []string `json:"codes"`
	// Files are every file of the run, in the order the migrator applies
	// them, selected or not: a rule may read the files before the one it
	// judges.
	Files []File `json:"files"`
	// Compatibility names the command surface the run reports on: empty for
	// Ptah's own, "atlas" for the Atlas-compatible one. A rule whose report
	// differs between the two reads it.
	Compatibility string `json:"compatibility,omitempty"`
	// TransactionOwner says who opens the transaction each file of the run
	// runs in. The zero value is the migrator, which is what a host that does
	// not know the tool that applies the files sends.
	TransactionOwner TransactionOwner `json:"transaction_owner,omitempty"`
	// Baseline is the schema state the versions a host read start from, nil
	// on a call made without one. The package documentation says when a
	// host reads it.
	Baseline *Baseline `json:"baseline,omitempty"`
}

// TransactionOwner is who opens the transaction a migration file runs in. A
// rule about transaction control reads it, because a BEGIN that would nest
// inside a migrator's transaction is the file's only transaction under a tool
// that opens none.
type TransactionOwner string

const (
	// TransactionOwnerMigrator is a migrator that opens a transaction around
	// each file and commits it after the file's last statement, unless the
	// file opts out ([File.NoTransaction]). Ptah's migrator and Atlas's run
	// files this way. It is the zero value, and an owner judges a value it
	// does not know the same way.
	TransactionOwnerMigrator TransactionOwner = ""
	// TransactionOwnerFile is a migrator that opens no transaction around a
	// file. The file runs in a transaction it opens itself, in the one the
	// server opens around a single query, or in none. golang-migrate runs
	// files this way.
	TransactionOwnerFile TransactionOwner = "file"
)

// SubjectKind is the kind of schema object a finding is about.
type SubjectKind string

const (
	// SubjectTable is a table.
	SubjectTable SubjectKind = "table"
	// SubjectColumn is a column.
	SubjectColumn SubjectKind = "column"
)

// Subject is one schema object a finding is about.
type Subject struct {
	// Kind is what the object is.
	Kind SubjectKind `json:"kind"`
	// Name is the object's name as the statement spells it.
	Name string `json:"name"`
	// Parent is the object the subject belongs to, a column's table, empty
	// where it names none.
	Parent string `json:"parent,omitempty"`
	// DataType is a column's type, empty where the rule does not know it.
	DataType string `json:"data_type,omitempty"`
}

// Finding is one violation an owner reports.
type Finding struct {
	// Code is the rule that found it, one of the request's codes.
	Code string `json:"code"`
	// File is the position of the file in the request.
	File int `json:"file"`
	// Statement is the position of the statement in its file, or -1 for a
	// finding about the file as a whole.
	Statement int `json:"statement"`
	// Message says what was found.
	Message string `json:"message"`
	// Subjects are the schema objects the finding is about, in the order a
	// report lists them. A finding about the file as a whole carries none.
	Subjects []Subject `json:"subjects,omitempty"`
}

// CheckResult is an owner's complete answer to one request.
type CheckResult struct {
	// Complete is set by an owner that judged every enabled rule over every
	// file. A host refuses a result without it rather than read silence as
	// a clean run.
	Complete bool `json:"complete"`
	// Findings are the violations, in any order.
	Findings []Finding `json:"findings,omitempty"`
	// BaselineSubjects are, for each rule whose input reads a baseline, the
	// statements it would judge further with the schema state their file's
	// version starts from, in any order. The package documentation says
	// what a host does with them.
	BaselineSubjects []BaselineSubject `json:"baseline_subjects,omitempty"`
}

// Service checks the rules one owner publishes. A host calls it once per
// lint run with every file of the run.
type Service interface {
	// CheckMigrations returns the findings of the request's rules, or an
	// error and no result. An error is never a clean run.
	CheckMigrations(ctx context.Context, request CheckRequest) (CheckResult, error)
}

// ValidateCheck reports a result that does not answer request: one not
// marked complete, a finding naming a rule the request did not enable, a file
// or statement it did not hold, or no message, a subject without a known kind
// or a name or on a finding about the file as a whole, and a baseline subject
// naming a rule the request did not enable, a file or statement it did not
// hold, or no statement. Each failure wraps [ErrInvalid].
func ValidateCheck(request CheckRequest, result CheckResult) error {
	if !result.Complete {
		return fmt.Errorf("%w: a lint check did not complete", ErrInvalid)
	}
	for _, finding := range result.Findings {
		if err := validateFinding(request, finding); err != nil {
			return err
		}
	}
	for _, subject := range result.BaselineSubjects {
		if !slices.Contains(request.Codes, subject.Code) || subject.File < 0 || subject.File >= len(request.Files) ||
			len(subject.Statements) == 0 || slices.ContainsFunc(subject.Statements, func(statement int) bool {
			return statement < 0 || statement >= len(request.Files[subject.File].Statements)
		}) {
			return fmt.Errorf("%w: baseline subject of %s names a rule, file or statement the request did not hold, or no statement",
				ErrInvalid, subject.Code)
		}
	}
	return nil
}

// validateFinding is [ValidateCheck] for one finding.
func validateFinding(request CheckRequest, finding Finding) error {
	if !slices.Contains(request.Codes, finding.Code) || finding.File < 0 || finding.File >= len(request.Files) ||
		finding.Statement < -1 || finding.Statement >= len(request.Files[finding.File].Statements) || finding.Message == "" {
		return fmt.Errorf("%w: finding %s names a rule, file or statement the request did not hold, or says nothing",
			ErrInvalid, finding.Code)
	}
	if len(finding.Subjects) > 0 && finding.Statement < 0 {
		return fmt.Errorf("%w: finding %s is about a whole file and names subjects", ErrInvalid, finding.Code)
	}
	for _, subject := range finding.Subjects {
		if (subject.Kind != SubjectTable && subject.Kind != SubjectColumn) || subject.Name == "" {
			return fmt.Errorf("%w: finding %s names a subject without a known kind or a name", ErrInvalid, finding.Code)
		}
	}
	return nil
}
