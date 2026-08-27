// Package embedspec loads an embedding-migration specification from a file.
//
// The specification is what an operator writes and what the generation identity
// is taken over, so what this package refuses matters as much as what it
// accepts: a field silently defaulted here becomes part of a content address,
// and two operators who wrote different files get one generation
// (stokaro/ptah#2068).
package embedspec

import (
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedgen"
)

// Document is one specification file.
type Document struct {
	// Version is the file format's own version, which is separate from the
	// specification's: a file may be rewritten without the meaning of a
	// generation changing.
	Version int `yaml:"version"`
	// Name and Description are for a person.
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	// Source names the rows and the fields the input is built from.
	Source SourceDocument `yaml:"source"`
	// Preprocessing turns one source row into one provider input.
	Preprocessing PreprocessingDocument `yaml:"preprocessing"`
	// Model names the provider, the model, and what it was asked for.
	Model ModelDocument `yaml:"model"`
	// Target names what the vector is stored as and how it will be compared.
	Target TargetDocument `yaml:"target"`
	// Consistency is how source changes during the run are accounted for.
	Consistency ConsistencyDocument `yaml:"consistency"`
	// Policy is what the environment requires before a pointer moves.
	Policy PolicyDocument `yaml:"policy"`
}

// SourceDocument is the source half.
type SourceDocument struct {
	Schema          string   `yaml:"schema"`
	Table           string   `yaml:"table"`
	Filter          string   `yaml:"filter"`
	KeyFields       []string `yaml:"key_fields"`
	InputFields     []string `yaml:"input_fields"`
	VersionStrategy string   `yaml:"version_strategy"`
	VersionField    string   `yaml:"version_field"`
	// Mutable declares whether the source can change during the run.
	//
	// It is a pointer so that "not stated" is distinguishable from "false". A
	// migration over a live table that forgot to say so would otherwise be
	// planned as one over a frozen one, and the difference is the whole of
	// Phase F.
	Mutable *bool `yaml:"mutable"`
}

// PreprocessingDocument is the input pipeline.
type PreprocessingDocument struct {
	Separator            string `yaml:"separator"`
	Prefix               string `yaml:"prefix"`
	NullPolicy           string `yaml:"null_policy"`
	EmptyPolicy          string `yaml:"empty_policy"`
	UnicodeNormalization string `yaml:"unicode_normalization"`
	CollapseWhitespace   bool   `yaml:"collapse_whitespace"`
	MaxInputBytes        int    `yaml:"max_input_bytes"`
	Truncate             string `yaml:"truncate"`
}

// ModelDocument is the provider half.
type ModelDocument struct {
	Provider           string `yaml:"provider"`
	EndpointClass      string `yaml:"endpoint_class"`
	Endpoint           string `yaml:"endpoint"`
	Identifier         string `yaml:"identifier"`
	Revision           string `yaml:"revision"`
	RequestedDimension int    `yaml:"requested_dimension"`
	ReportedDimension  int    `yaml:"reported_dimension"`
	Normalization      string `yaml:"normalization"`
	Pooling            string `yaml:"pooling"`
	// Credential is a REFERENCE, never a value: `env:NAME` or `file:/path`.
	//
	// A file that could hold the token itself would be a file operators commit,
	// and the epic's rule is that a key must not appear in project
	// configuration at all.
	Credential string `yaml:"credential"`
}

// TargetDocument is the storage half.
type TargetDocument struct {
	Schema         string            `yaml:"schema"`
	Table          string            `yaml:"table"`
	Column         string            `yaml:"column"`
	Representation string            `yaml:"representation"`
	Metric         string            `yaml:"metric"`
	IndexMethod    string            `yaml:"index_method"`
	IndexOptions   map[string]string `yaml:"index_options"`
}

// ConsistencyDocument is the mode and what it needs.
type ConsistencyDocument struct {
	Mode string `yaml:"mode"`
	// Paused declares that writes are stopped for the duration, which is what
	// makes the immutable mode true of a table that can otherwise change.
	Paused bool `yaml:"paused"`
}

// PolicyDocument is what the environment requires.
type PolicyDocument struct {
	RequireExactApproval   bool   `yaml:"require_exact_approval"`
	RequireConsistencyMode bool   `yaml:"require_consistency_mode"`
	AllowAcceptedFindings  bool   `yaml:"allow_accepted_findings"`
	MaxPlanAge             string `yaml:"max_plan_age"`
}

// Loaded is a specification and everything read alongside it.
type Loaded struct {
	// Spec is the transformation, which is what the identity is taken over.
	Spec embedgen.Spec
	// Mode is the consistency mode.
	Mode embedcatchup.Mode
	// Source is what the file says about the source's mutability.
	Source embedcatchup.SourceState
	// Policy is what the environment requires before a pointer moves.
	Policy embedcutover.Policy
	// Credential is where the provider's credential lives, never what it is.
	Credential string
	// Endpoint is the provider endpoint.
	Endpoint string
}

// FormatVersion is the file format this build reads.
const FormatVersion = 1

// Load reads a specification file.
func Load(path string) (Loaded, error) {
	body, err := os.ReadFile(path) //gosec:disable G304 -- the operator named this file on the command line
	if err != nil {
		return Loaded{}, fmt.Errorf("read %s: %w", path, err)
	}
	return Parse(body, path)
}

// Parse reads a specification from bytes.
//
// Unknown fields are refused rather than ignored. A typo in a field that
// decides a vector -- `input_field` for `input_fields` -- would otherwise
// produce a valid specification for a different generation, and the operator's
// evidence that it worked would be that it ran.
func Parse(body []byte, path string) (Loaded, error) {
	decoder := yaml.NewDecoder(strings.NewReader(string(body)))
	decoder.KnownFields(true)
	var document Document
	if err := decoder.Decode(&document); err != nil {
		return Loaded{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return document.Resolve(path)
}
