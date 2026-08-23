// Package agentpolicy decides what an AI-driven caller may do, outside the
// model loop and before any work happens.
//
// # Why a broker at all
//
// ADR 0003 measured the read-only agent surface and recorded the sentence this
// package is written against: the model is untrusted and its arguments are
// attacker-influenced. A schema comment, a column name or a README is a place
// somebody can write a sentence addressed to the model, and that text arrives
// as tool output in the same context window the model chooses its next
// arguments from. Nothing the model says can therefore decide what it is
// allowed to do -- not a claim about the user's intent, not a path, not a
// promise that an operation is safe.
//
// The containment is arithmetic instead: every operation names a [Capability],
// the effective verdict for it is resolved from operator-controlled
// configuration, and the resolution reads none of the model's text.
//
// # The three verdicts
//
// [VerdictAllow] proceeds, [VerdictDeny] refuses, and [VerdictAsk] refuses
// until a human approves this exact operation. There is no fourth state and no
// implicit promotion: a session with nobody to ask resolves [VerdictAsk] to a
// refusal naming the missing approval, which is what `--non-interactive` has to
// mean if it is to be safe in CI. Reading it as "approve everything" is the
// defect this package exists to make unavailable.
//
// # Layers, and the one that may only narrow
//
// Policy is assembled from four layers. [LayerBuiltin] is the secure default
// below; [LayerUser] is the operator's own configuration; [LayerInvocation] is
// the flags and environment of the command the operator started. Those three
// are operator-controlled and may widen or narrow.
//
// [LayerProject] is different in kind, and the difference is the whole reason
// the layers are ordered rather than merged. A project file lives in the
// repository the model is reading and proposing patches to, so treating it as a
// grant would close the loop: content the attacker controls would decide what
// the attacker's next tool call is permitted to do. A project rule that is more
// permissive than the effective verdict is therefore ignored, and reported
// through [Resolution.Ignored] rather than dropped in silence -- a policy file
// whose lines do nothing should say so to the person who wrote it.
//
// # Hard constraints
//
// Four capabilities are refused by every layer, including the operator's:
// arbitrary filesystem read and write, shell execution, and arbitrary network
// access. They are not features Ptah has and gates; they are the shape of tool
// this surface is not. A configuration that tries to grant one is a
// configuration error rather than an unusual setup, so it fails at assembly
// with the name of the capability in the message, not at the call site where a
// missing grant would read as a bug.
package agentpolicy

import (
	"fmt"
	"slices"
	"strings"
)

// Capability names one class of operation the broker decides about.
//
// The names are Ptah's own rather than the CLI's verbs, for the reason ADR 0002
// gives for the operation names: a verb carries flag history an authorization
// model should not inherit, and a capability that renames itself whenever a
// command is renamed cannot be written down in a policy file.
type Capability string

const (
	// ProjectRead covers reading project identity and configuration that
	// describes the workspace: which directories hold what, which dialect is
	// declared. It does not cover reading the artifacts themselves.
	ProjectRead Capability = "project.read"

	// SchemaValidate covers reporting structural problems in a declared schema.
	SchemaValidate Capability = "schema.validate"
	// SchemaRender covers turning a declared schema into DDL text.
	SchemaRender Capability = "schema.render"
	// SchemaLineage covers tracing which base columns feed a view column.
	SchemaLineage Capability = "schema.lineage"
	// SchemaExternalExecute covers resolving a desired schema that requires
	// running something the repository chose: an ORM loader, an external
	// command, a containerized loader. It is separate from reading HCL or SQL
	// because it is arbitrary code execution wearing a schema-source name, and
	// the model must not reach it by asking for "the desired schema".
	SchemaExternalExecute Capability = "schema.external_execute"

	// DatabaseInspect covers reading catalog metadata from a live database.
	DatabaseInspect Capability = "database.inspect"
	// DatabaseReadRows covers retrieving table rows. Ptah's agent surface has
	// no operation that does this; the capability exists so that adding one
	// cannot happen without meeting a deny that has to be argued with.
	DatabaseReadRows Capability = "database.read_rows"
	// DatabaseExecuteSQL covers running caller-supplied SQL.
	DatabaseExecuteSQL Capability = "database.execute_sql"
	// MigrationApply covers applying migrations to a database.
	MigrationApply Capability = "migration.apply"

	// ArtifactRead covers reading a file inside a configured artifact scope.
	ArtifactRead Capability = "artifact.read"
	// ArtifactWrite covers creating or replacing a file inside a configured
	// artifact scope.
	ArtifactWrite Capability = "artifact.write"
	// ArtifactDelete covers removing one. It is separate from ArtifactWrite
	// because the mistakes are not the same size: a bad write is reviewable in
	// a diff and a deletion of the migration that recorded a production change
	// is not.
	ArtifactDelete Capability = "artifact.delete"

	// FilesystemArbitraryRead covers reading a path outside every configured
	// scope. Hard-denied.
	FilesystemArbitraryRead Capability = "filesystem.arbitrary_read"
	// FilesystemArbitraryWrite covers writing one. Hard-denied.
	FilesystemArbitraryWrite Capability = "filesystem.arbitrary_write"
	// ShellExecute covers running a command the caller names. Hard-denied.
	ShellExecute Capability = "shell.execute"
	// NetworkArbitrary covers reaching a network address the caller names,
	// other than the database and provider endpoints an operation already
	// carries its own capability for. Hard-denied.
	NetworkArbitrary Capability = "network.arbitrary"
)

// ArtifactClass names a family of files the workspace declares, and is the
// scope an artifact capability is granted over.
//
// A class is not a path. The operator grants `artifact.write` over
// [ClassMigrations], and where the migration directory actually is comes from
// the workspace resolution rather than from the grant -- so a grant cannot be
// satisfied by pointing a path at something else later.
type ArtifactClass string

const (
	// ClassMigrations is the migration directory and its integrity file.
	ClassMigrations ArtifactClass = "migrations"
	// ClassSchema is the declared schema: annotated Go sources, HCL, YAML or
	// SQL files the project names as its desired state.
	ClassSchema ArtifactClass = "schema"
	// ClassTests is the Ptah test directory.
	ClassTests ArtifactClass = "tests"
)

// ArtifactClasses is every class in the order they are reported.
func ArtifactClasses() []ArtifactClass {
	return []ArtifactClass{ClassMigrations, ClassSchema, ClassTests}
}

// DatabaseClass names how much a database is worth protecting, and is the scope
// a database capability is granted over.
//
// The classification is resolved from configuration, never from the connection
// string's text: a database called `dev` in a URL the model composed is a claim
// by the untrusted party, and #1483 states the rule plainly -- a connection
// named dev must not be trusted solely because of its name.
type DatabaseClass string

const (
	// ClassEphemeral is a throwaway database Ptah itself started and will
	// destroy, such as a container it manages for the duration of one command.
	ClassEphemeral DatabaseClass = "ephemeral"
	// ClassDev is a configured development database.
	ClassDev DatabaseClass = "dev"
	// ClassTarget is a configured non-production target.
	ClassTarget DatabaseClass = "target"
	// ClassProduction is a target the configuration marks as production.
	ClassProduction DatabaseClass = "production"
	// ClassUnclassified is a database the configuration says nothing about,
	// which includes every URL that arrived as a tool argument. It is
	// deliberately the most restricted class rather than the least: an
	// unrecognized address is not evidence of harmlessness.
	ClassUnclassified DatabaseClass = "unclassified"
)

// Verdict is what the broker decided.
type Verdict int

const (
	// VerdictDeny refuses the operation. Nothing satisfies it later in the same
	// session; the configuration has to change.
	VerdictDeny Verdict = iota
	// VerdictAsk refuses the operation until a human approves this exact
	// operation, including its digests. It is never a weaker allow.
	VerdictAsk
	// VerdictAllow permits the operation.
	VerdictAllow
)

// String names the verdict as a policy file spells it.
func (v Verdict) String() string {
	switch v {
	case VerdictDeny:
		return "deny"
	case VerdictAsk:
		return "ask"
	case VerdictAllow:
		return "allow"
	}
	return fmt.Sprintf("verdict(%d)", int(v))
}

// ParseVerdict reads the spelling a policy file carries.
//
// Nothing is trimmed and no case folding happens, for the reason
// internal/envbool states about boolean environment values: a quoting mistake
// that silently resolves to a valid verdict is the class of accident an
// authorization file can least afford.
func ParseVerdict(text string) (Verdict, error) {
	switch text {
	case "deny":
		return VerdictDeny, nil
	case "ask":
		return VerdictAsk, nil
	case "allow":
		return VerdictAllow, nil
	}
	return VerdictDeny, fmt.Errorf("invalid verdict %q: want allow, ask or deny", text)
}

// atMost returns the more restrictive of two verdicts.
func atMost(a, b Verdict) Verdict {
	return min(a, b)
}

// Layer names where a rule came from, which decides whether it may widen.
type Layer int

const (
	// LayerBuiltin is the default table in this package.
	LayerBuiltin Layer = iota
	// LayerUser is the operator's own configuration, outside any repository.
	LayerUser
	// LayerInvocation is the flags and environment of the command the operator
	// started.
	LayerInvocation
	// LayerProject is configuration carried by the repository under
	// examination. It may only narrow.
	LayerProject
)

// String names the layer as diagnostics spell it.
func (l Layer) String() string {
	switch l {
	case LayerBuiltin:
		return "builtin"
	case LayerUser:
		return "user"
	case LayerInvocation:
		return "invocation"
	case LayerProject:
		return "project"
	}
	return fmt.Sprintf("layer(%d)", int(l))
}

// mayWiden reports whether a layer's rule is allowed to be more permissive than
// what the layers below it resolved to.
func (l Layer) mayWiden() bool {
	return l != LayerProject
}

// hardDenied is the set no layer may lift.
//
// Each is a general coding-agent capability rather than a database one. Ptah's
// agent surface is not the place to acquire them, and a configuration asking
// for one has misunderstood what it is configuring rather than made an unusual
// choice.
var hardDenied = []Capability{
	FilesystemArbitraryRead,
	FilesystemArbitraryWrite,
	ShellExecute,
	NetworkArbitrary,
}

// IsHardDenied reports whether nothing can grant the capability.
func IsHardDenied(capability Capability) bool {
	return slices.Contains(hardDenied, capability)
}

// scopeKind says which scope, if any, a capability is granted over.
type scopeKind int

const (
	scopeNone scopeKind = iota
	scopeArtifact
	scopeDatabase
)

// capabilityScope is the one declaration of which capabilities carry a scope.
//
// It is a map rather than a convention on the name because the alternative --
// deciding from the string's prefix -- makes every future capability's scope a
// consequence of what it was called.
var capabilityScope = map[Capability]scopeKind{
	ProjectRead:              scopeNone,
	SchemaValidate:           scopeNone,
	SchemaRender:             scopeNone,
	SchemaLineage:            scopeNone,
	SchemaExternalExecute:    scopeNone,
	DatabaseInspect:          scopeDatabase,
	DatabaseReadRows:         scopeDatabase,
	DatabaseExecuteSQL:       scopeDatabase,
	MigrationApply:           scopeDatabase,
	ArtifactRead:             scopeArtifact,
	ArtifactWrite:            scopeArtifact,
	ArtifactDelete:           scopeArtifact,
	FilesystemArbitraryRead:  scopeNone,
	FilesystemArbitraryWrite: scopeNone,
	ShellExecute:             scopeNone,
	NetworkArbitrary:         scopeNone,
}

// Capabilities lists every capability the broker knows, in a stable order.
//
// The order is the declaration order above rather than alphabetical, so a
// rendered policy reads as project, schema, database, artifact, and the four
// things this surface refuses to be.
func Capabilities() []Capability {
	return []Capability{
		ProjectRead,
		SchemaValidate,
		SchemaRender,
		SchemaLineage,
		SchemaExternalExecute,
		DatabaseInspect,
		DatabaseReadRows,
		DatabaseExecuteSQL,
		MigrationApply,
		ArtifactRead,
		ArtifactWrite,
		ArtifactDelete,
		FilesystemArbitraryRead,
		FilesystemArbitraryWrite,
		ShellExecute,
		NetworkArbitrary,
	}
}

// ParseCapability reads a capability name, refusing one this build does not
// know.
//
// A policy file naming an unknown capability is a configuration error rather
// than a line to skip: the two readings are "this Ptah is older than the file"
// and "this is a typo", and both are better reported than obeyed halfway.
func ParseCapability(name string) (Capability, error) {
	candidate := Capability(name)
	if _, known := capabilityScope[candidate]; !known {
		return "", fmt.Errorf("unknown capability %q", name)
	}
	return candidate, nil
}

// Rule is one line of policy.
//
// An empty Artifact or Database means the rule applies to every scope of that
// kind, which is what a file writes when it says `artifact.write ask` without
// naming a class.
type Rule struct {
	Capability Capability
	Artifact   ArtifactClass
	Database   DatabaseClass
	Verdict    Verdict
}

// String renders the rule the way a policy file spells it.
func (r Rule) String() string {
	name := string(r.Capability)
	if r.Artifact != "" {
		name += ":" + string(r.Artifact)
	}
	if r.Database != "" {
		name += ":" + string(r.Database)
	}
	return name + " " + r.Verdict.String()
}

// validate refuses a rule whose scope does not belong to its capability.
//
// The check matters because the two mistakes it catches read as grants:
// `artifact.write:production allow` looks like it says something about
// production and says nothing at all, and `database.inspect:migrations allow`
// looks like it narrows an inspection grant to a directory.
func (r Rule) validate() error {
	kind, known := capabilityScope[r.Capability]
	if !known {
		return fmt.Errorf("unknown capability %q", r.Capability)
	}
	switch kind {
	case scopeNone:
		if r.Artifact != "" || r.Database != "" {
			return fmt.Errorf("capability %q takes no scope", r.Capability)
		}
	case scopeArtifact:
		if r.Database != "" {
			return fmt.Errorf("capability %q is scoped by artifact class, not by database class", r.Capability)
		}
		if r.Artifact != "" && !slices.Contains(ArtifactClasses(), r.Artifact) {
			return fmt.Errorf("unknown artifact class %q", r.Artifact)
		}
	case scopeDatabase:
		if r.Artifact != "" {
			return fmt.Errorf("capability %q is scoped by database class, not by artifact class", r.Capability)
		}
		if r.Database != "" && !slices.Contains(DatabaseClasses(), r.Database) {
			return fmt.Errorf("unknown database class %q", r.Database)
		}
	}
	return nil
}

// DatabaseClasses is every database class, in the order they are reported.
//
// The order is not a severity ranking: ClassUnclassified sits last because it
// is the residue, and it is the most restricted class rather than the least.
func DatabaseClasses() []DatabaseClass {
	return []DatabaseClass{
		ClassEphemeral,
		ClassDev,
		ClassTarget,
		ClassProduction,
		ClassUnclassified,
	}
}

// matches reports whether the rule governs the request.
func (r Rule) matches(req Request) bool {
	if r.Capability != req.Capability {
		return false
	}
	if r.Artifact != "" && r.Artifact != req.Artifact {
		return false
	}
	if r.Database != "" && r.Database != req.Database {
		return false
	}
	return true
}

// specificity orders rules so a scoped rule beats an unscoped one for the scope
// it names.
func (r Rule) specificity() int {
	score := 0
	if r.Artifact != "" {
		score++
	}
	if r.Database != "" {
		score++
	}
	return score
}

// ParseRule reads one `capability[:scope] verdict` line.
//
// Fields are separated by a single space and nothing is trimmed, so a line that
// looks aligned in an editor is a parse error rather than a rule that quietly
// governs something else.
func ParseRule(line string) (Rule, error) {
	name, verdictText, found := strings.Cut(line, " ")
	if !found {
		return Rule{}, fmt.Errorf("invalid rule %q: want \"capability[:scope] verdict\"", line)
	}
	verdict, err := ParseVerdict(verdictText)
	if err != nil {
		return Rule{}, fmt.Errorf("invalid rule %q: %w", line, err)
	}
	head, scope, scoped := strings.Cut(name, ":")
	capability, err := ParseCapability(head)
	if err != nil {
		return Rule{}, fmt.Errorf("invalid rule %q: %w", line, err)
	}
	rule := Rule{Capability: capability, Verdict: verdict}
	if scoped {
		switch capabilityScope[capability] {
		case scopeArtifact:
			rule.Artifact = ArtifactClass(scope)
		case scopeDatabase:
			rule.Database = DatabaseClass(scope)
		case scopeNone:
			return Rule{}, fmt.Errorf("invalid rule %q: capability %q takes no scope", line, capability)
		}
	}
	if err := rule.validate(); err != nil {
		return Rule{}, fmt.Errorf("invalid rule %q: %w", line, err)
	}
	return rule, nil
}

// Request is one operation asking for a decision.
//
// Paths are carried for the approval prompt and the audit record. They are not
// what containment is enforced with: a path is checked against the workspace's
// opened directories, where the check binds a filesystem object rather than a
// spelling, and a broker that decided from the string would be deciding from
// something the model wrote.
type Request struct {
	Capability Capability
	Artifact   ArtifactClass
	Database   DatabaseClass
	Paths      []string
	// Reason is the operation's own words for what it is doing, shown to the
	// person being asked. It is Ptah's text, never the model's.
	Reason string
}

// validate refuses a request whose scope does not belong to its capability, and
// refuses an unscoped request for a scoped capability.
//
// The second half is what keeps a decision from being broader than the thing it
// decided about: `artifact.write` with no class named is not a question the
// broker can answer, because the answer differs per class.
func (r Request) validate() error {
	kind, known := capabilityScope[r.Capability]
	if !known {
		return fmt.Errorf("unknown capability %q", r.Capability)
	}
	switch kind {
	case scopeNone:
		if r.Artifact != "" || r.Database != "" {
			return fmt.Errorf("capability %q takes no scope", r.Capability)
		}
	case scopeArtifact:
		if r.Database != "" {
			return fmt.Errorf("capability %q is scoped by artifact class, not by database class", r.Capability)
		}
		if r.Artifact == "" {
			return fmt.Errorf("capability %q requires an artifact class", r.Capability)
		}
		if !slices.Contains(ArtifactClasses(), r.Artifact) {
			return fmt.Errorf("unknown artifact class %q", r.Artifact)
		}
	case scopeDatabase:
		if r.Artifact != "" {
			return fmt.Errorf("capability %q is scoped by database class, not by artifact class", r.Capability)
		}
		if r.Database == "" {
			return fmt.Errorf("capability %q requires a database class", r.Capability)
		}
		if !slices.Contains(DatabaseClasses(), r.Database) {
			return fmt.Errorf("unknown database class %q", r.Database)
		}
	}
	return nil
}

// String renders the request's scoped capability name, which is what an audit
// record and an approval prompt both show.
func (r Request) String() string {
	name := string(r.Capability)
	if r.Artifact != "" {
		name += ":" + string(r.Artifact)
	}
	if r.Database != "" {
		name += ":" + string(r.Database)
	}
	return name
}
