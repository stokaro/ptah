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
	// Statements are the file's statements, split by the target's owner, or
	// by the host where the run names no target.
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
}

// CheckResult is an owner's complete answer to one request.
type CheckResult struct {
	// Complete is set by an owner that judged every enabled rule over every
	// file. A host refuses a result without it rather than read silence as
	// a clean run.
	Complete bool `json:"complete"`
	// Findings are the violations, in any order.
	Findings []Finding `json:"findings,omitempty"`
}

// Service checks the rules one owner publishes. A host calls it once per
// lint run with every file of the run.
type Service interface {
	// CheckMigrations returns the findings of the request's rules, or an
	// error and no result. An error is never a clean run.
	CheckMigrations(ctx context.Context, request CheckRequest) (CheckResult, error)
}

// ValidateCheck reports a result that does not answer request: one not
// marked complete, or a finding naming a rule the request did not enable, a
// file or statement it did not hold, or no message. Each failure wraps
// [ErrInvalid].
func ValidateCheck(request CheckRequest, result CheckResult) error {
	if !result.Complete {
		return fmt.Errorf("%w: a lint check did not complete", ErrInvalid)
	}
	for _, finding := range result.Findings {
		if !slices.Contains(request.Codes, finding.Code) || finding.File < 0 || finding.File >= len(request.Files) ||
			finding.Statement < -1 || finding.Statement >= len(request.Files[finding.File].Statements) || finding.Message == "" {
			return fmt.Errorf("%w: finding %s names a rule, file or statement the request did not hold, or says nothing",
				ErrInvalid, finding.Code)
		}
	}
	return nil
}
