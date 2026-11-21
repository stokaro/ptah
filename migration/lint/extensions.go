package lint

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/lintkit"
)

// ExtensionRequest is an extension a statement creates or updates, as the
// statement names it: CREATE EXTENSION name [VERSION version] [CASCADE], or
// ALTER EXTENSION name UPDATE [TO version].
type ExtensionRequest struct {
	// Extension is the extension's name as the server stores it: a bare name
	// folded to lower case, a quoted one as written.
	Extension string
	// Version is the version the statement names, and empty when it names
	// none, so the server installs the extension's default version.
	Version string
	// Cascade reports CREATE EXTENSION ... CASCADE, which also installs each
	// extension the installed version requires and the database lacks.
	Cascade bool
}

// ExtensionAssessments is what a dev database installs for the
// [ExtensionRequest]s of a run, and what reviewed advisory data says about
// each extension installed, for the rules security/vulnerable-extension and
// coverage/unassessed-extension. [Options.Extensions] carries it.
type ExtensionAssessments struct {
	// Server names the server the answers describe, such as "PostgreSQL
	// 17.5", or is empty when the server reported no version.
	Server string
	// Answers holds one answer per request. A request with no answer is
	// reported as unassessed, never as clean.
	Answers []ExtensionAnswer
}

// ExtensionAnswer is what one [ExtensionRequest] installs on the dev
// database.
type ExtensionAnswer struct {
	// Request is the request answered.
	Request ExtensionRequest
	// Installs are the extensions the statement installs: the requested one
	// first and, with Cascade, the extensions it requires after it. Empty
	// means the dev database could not say what the statement installs, which
	// is reported as unassessed.
	Installs []InstalledExtension
}

// InstalledExtension is one extension a statement installs on the dev
// database, and what the advisory data says about it there.
type InstalledExtension struct {
	// Extension and Version name the extension and the version installed.
	Extension string
	Version   string
	// Advisories are the published advisories that affect the extension on
	// the dev database's server.
	Advisories []ExtensionAdvisory
	// Unassessed are the reasons the advisory data cannot call the extension
	// clean, such as an extension no reviewed data covers or data past its
	// age. Empty, with no Advisories, is a clean answer.
	Unassessed []string
}

// ExtensionAdvisory is one published advisory that affects an installed
// extension.
type ExtensionAdvisory struct {
	// ID identifies the advisory, such as a CVE ID.
	ID string
	// Summary is the advisory's one-line description.
	Summary string
	// FixedIn is the first server release that carries the fix, and empty
	// when the data names none.
	FixedIn string
	// URL is the advisory's page.
	URL string
}

// answerFor returns the answer to request, and false when there is none.
func (a *ExtensionAssessments) answerFor(request ExtensionRequest) (ExtensionAnswer, bool) {
	index := slices.IndexFunc(a.Answers, func(answer ExtensionAnswer) bool { return answer.Request == request })
	if index < 0 {
		return ExtensionAnswer{}, false
	}
	return a.Answers[index], true
}

// The titles of the extension rules, which their findings carry.
const (
	vulnerableExtensionTitle = "extension a published advisory affects"
	unassessedExtensionTitle = "extension the advisory data could not assess"
)

// extensionRules are security/vulnerable-extension and
// coverage/unassessed-extension. Both read the statements that install an
// extension ([extensionRequestAt]) and the answers [Options.Extensions]
// carries, and neither runs on the compatibility surface: the community
// binary's analyzer for vulnerable extensions has no check code in the
// reviewed Atlas reference, so `ptah-compat migrate lint` reports neither.
//
// Their dialects are those whose descriptor says a run reads what a dev
// database installs for an extension and matches it against advisory data.
func extensionRules() []Rule {
	dialects := lintDialects(func(described descriptor.Lint) bool { return described.ExtensionAdvisories })
	return []Rule{
		{
			Name:      "security/vulnerable-extension",
			Title:     vulnerableExtensionTitle,
			Severity:  SeverityWarning,
			Dialects:  dialects,
			Input:     InputExtensionAssessment,
			CheckFile: vulnerableExtensionFindings,
		},
		{
			Name:      "coverage/unassessed-extension",
			Title:     unassessedExtensionTitle,
			Severity:  SeverityInfo,
			Dialects:  slices.Clone(dialects),
			Input:     InputExtensionAssessment,
			CheckFile: unassessedExtensionFindings,
		},
	}
}

// vulnerableExtensionFindings reports each extension a statement installs that
// a published advisory affects on the dev database's server.
func vulnerableExtensionFindings(file *File) []Finding {
	var findings []Finding
	for _, index := range extensionStatements(file) {
		answer, ok := file.extensions.answerFor(extensionRequestAt(file, index))
		if !ok {
			continue
		}
		for position, installed := range answer.Installs {
			if len(installed.Advisories) == 0 {
				continue
			}
			advisories := make([]string, 0, len(installed.Advisories))
			for _, advisory := range installed.Advisories {
				fixed := "no fixed release named"
				if advisory.FixedIn != "" {
					fixed = "fixed in " + advisory.FixedIn
				}
				advisories = append(advisories, fmt.Sprintf("%s (%s): %s, %s", advisory.ID, fixed, advisory.Summary, advisory.URL))
			}
			message := fmt.Sprintf("%s, which this statement installs%s, is affected on the dev database's server%s: %s. "+
				"A server at the fixed release or later is not affected; the dev database's server is what this run assessed, "+
				"so check the release the migration will run on",
				installedName(installed), cascaded(position), serverName(file.extensions.Server), strings.Join(advisories, "; "))
			findings = append(findings, extensionFinding(file, index, "security/vulnerable-extension", vulnerableExtensionTitle, message))
		}
	}
	return findings
}

// unassessedExtensionFindings reports each extension a statement installs that
// the advisory data cannot call clean, and each statement whose installs the
// dev database could not say.
func unassessedExtensionFindings(file *File) []Finding {
	var findings []Finding
	for _, index := range extensionStatements(file) {
		request := extensionRequestAt(file, index)
		answer, ok := file.extensions.answerFor(request)
		if !ok || len(answer.Installs) == 0 {
			message := fmt.Sprintf("the dev database's server%s did not say what this statement installs for extension %s, "+
				"so no advisory was checked for it", serverApposition(file.extensions.Server), request.Extension)
			findings = append(findings, extensionFinding(file, index, "coverage/unassessed-extension", unassessedExtensionTitle, message))
			continue
		}
		for position, installed := range answer.Installs {
			if len(installed.Unassessed) == 0 {
				continue
			}
			message := fmt.Sprintf("%s, which this statement installs%s, was not fully assessed against published advisories "+
				"on the dev database's server%s: %s",
				installedName(installed), cascaded(position), serverName(file.extensions.Server), strings.Join(installed.Unassessed, "; "))
			findings = append(findings, extensionFinding(file, index, "coverage/unassessed-extension", unassessedExtensionTitle, message))
		}
	}
	return findings
}

// extensionFinding is a finding of an extension rule on one statement. The
// run sets its severity, the rule's or the one the configuration names.
func extensionFinding(file *File, statement int, rule, title, message string) Finding {
	return Finding{
		Rule:    rule,
		Title:   title,
		File:    file.Path,
		Line:    file.Statements[statement].Line,
		Message: message,
		Context: statementFindingContext(statement),
	}
}

// installedName names an installed extension and its version.
func installedName(installed InstalledExtension) string {
	return strings.TrimSpace(installed.Extension + " " + installed.Version)
}

// cascaded says how the statement installs the extension at position: the
// first is the one it names, and the rest arrive through CASCADE.
func cascaded(position int) string {
	if position == 0 {
		return ""
	}
	return " through CASCADE"
}

// serverName names the server in a message, or nothing when it reported no
// version.
func serverName(server string) string {
	if server == "" {
		return ""
	}
	return ", " + server
}

// serverApposition names the server inside a sentence, closed by a comma, or
// nothing when it reported no version.
func serverApposition(server string) string {
	if server == "" {
		return ""
	}
	return ", " + server + ","
}

// extensionStatements returns the indexes of the statements of an up file
// that install an extension, on a run that has the dev database's answers
// and is not the compatibility surface's. The reviewed-schema filter leaves
// out what it leaves out of every rule.
func extensionStatements(file *File) []int {
	if file.extensions == nil {
		return nil
	}
	return extensionStatementIndexes(file)
}

// extensionStatementIndexes returns the indexes of the statements of an up
// file that install an extension, whether or not the run read the answers.
func extensionStatementIndexes(file *File) []int {
	if !file.IsUp || file.compatibility == CompatibilityProfileAtlas {
		return nil
	}
	var indexes []int
	for index := range file.Statements {
		if file.scopeExcluded[index] {
			continue
		}
		if _, ok := extensionRequestOf(&file.Statements[index]); ok {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

// extensionRequestAt is the request of the statement at index, which
// [extensionStatementIndexes] found to make one.
func extensionRequestAt(file *File, index int) ExtensionRequest {
	request, _ := extensionRequestOf(&file.Statements[index])
	return request
}

// extensionRequestOf reads the extension a statement creates or updates:
//
//	CREATE EXTENSION [IF NOT EXISTS] name [WITH] [SCHEMA s | VERSION v | CASCADE]...
//	ALTER EXTENSION name UPDATE [TO v]
//
// The options of CREATE EXTENSION may come in any order, as the server reads
// them. A statement whose name or version is not a bare word, a quoted
// identifier or a string literal is not read.
func extensionRequestOf(stmt *Statement) (ExtensionRequest, bool) {
	w := stmt.Words
	switch {
	case lintkit.HasWordPrefix(w, "CREATE", "EXTENSION"):
		at := lintkit.SkipIfNotExists(w, 2)
		name, ok := extensionWord(stmt, at)
		if !ok {
			return ExtensionRequest{}, false
		}
		request := ExtensionRequest{Extension: name}
		for i := at + 1; i < len(w); {
			switch w[i] {
			case "WITH":
				i++
			case "SCHEMA":
				i += 2
			case "VERSION":
				version, ok := extensionWord(stmt, i+1)
				if !ok {
					return ExtensionRequest{}, false
				}
				request.Version = version
				i += 2
			case "CASCADE":
				request.Cascade = true
				i++
			default:
				return ExtensionRequest{}, false
			}
		}
		return request, true
	case lintkit.HasWordPrefix(w, "ALTER", "EXTENSION") && len(w) > 3 && w[3] == "UPDATE":
		name, ok := extensionWord(stmt, 2)
		if !ok {
			return ExtensionRequest{}, false
		}
		request := ExtensionRequest{Extension: name}
		switch {
		case len(w) == 4:
			return request, true
		case len(w) == 6 && w[4] == "TO":
			version, ok := extensionWord(stmt, 5)
			if !ok {
				return ExtensionRequest{}, false
			}
			request.Version = version
			return request, true
		}
	}
	return ExtensionRequest{}, false
}

// extensionWord reads the name or version at index as the server stores it:
// a bare word folded to lower case, a quoted identifier or a string literal
// with its quotes removed and doubled quotes undone.
func extensionWord(stmt *Statement, index int) (string, bool) {
	if index >= len(stmt.Words) {
		return "", false
	}
	word := lintkit.SourceWordAt(stmt.Words, stmt.sourceWords, index)
	switch {
	case len(word) >= 2 && word[0] == '"' && word[len(word)-1] == '"':
		return strings.ReplaceAll(word[1:len(word)-1], `""`, `"`), len(word) > 2
	case len(word) >= 2 && word[0] == '\'' && word[len(word)-1] == '\'':
		return strings.ReplaceAll(word[1:len(word)-1], `''`, `'`), len(word) > 2
	case lintkit.IdentLike(word) && word[0] != '`':
		return strings.ToLower(word), true
	}
	return "", false
}

// extensionRequests returns every request the selected up files make, for a
// rule the run applies that reads the dev database's answers, each once and
// sorted. A run that has none to ask about makes no round trip.
func extensionRequests(files []File, opts Options, rules []Rule) []ExtensionRequest {
	var requests []ExtensionRequest
	for i := range files {
		file := &files[i]
		if !file.Selected || !extensionAnswersWanted(file, opts, rules) {
			continue
		}
		for _, index := range extensionStatementIndexes(file) {
			requests = append(requests, extensionRequestAt(file, index))
		}
	}
	slices.SortFunc(requests, func(a, b ExtensionRequest) int {
		return cmp.Or(strings.Compare(a.Extension, b.Extension), strings.Compare(a.Version, b.Version), boolOrder(a.Cascade, b.Cascade))
	})
	return slices.Compact(requests)
}

// extensionAnswersWanted reports whether a rule the run applies to file reads
// the dev database's answers.
func extensionAnswersWanted(file *File, opts Options, rules []Rule) bool {
	return slices.ContainsFunc(rules, func(rule Rule) bool {
		return rule.Input == InputExtensionAssessment && ruleRunsOnFile(rule, file, opts)
	})
}

// boolOrder orders false before true.
func boolOrder(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}
