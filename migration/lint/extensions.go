package lint

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/pgextensioninstall"
)

// ExtensionRequest is an extension a statement creates or updates, as the
// statement names it: CREATE EXTENSION name [VERSION version] [CASCADE], or
// ALTER EXTENSION name UPDATE [TO version]. The name and the version are
// decoded as the server decodes them, from any spelling it reads.
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
	// Held is, for each version of [Analysis.ExtensionVersions] the run read,
	// the extensions the dev database held when that version started, by name
	// with the version installed.
	//
	// A statement is judged against it and the statements before it in its
	// file: CREATE EXTENSION IF NOT EXISTS of an extension the database holds
	// installs nothing, CASCADE installs only what the database lacks, and an
	// update to the version the database holds installs nothing. A version
	// missing from it is judged as if the database held no extension, which
	// reports an install that may not happen rather than miss one that does.
	Held map[int64]map[string]string
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
// coverage/unassessed-extension. Both read what the statements of a file that
// install an extension install ([fileInstalls]) and the answers
// [Options.Extensions] carries, and neither runs on the compatibility surface:
// the community binary's analyzer for vulnerable extensions has no check code
// in the reviewed Atlas reference, so `ptah-compat migrate lint` reports
// neither.
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
	for _, statement := range fileInstalls(file) {
		for _, installed := range statement.installs {
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
				installedName(installed.InstalledExtension), installed.how(), serverName(file.extensions.Server),
				strings.Join(advisories, "; "))
			findings = append(findings, extensionFinding(file, statement.index, "security/vulnerable-extension", vulnerableExtensionTitle, message))
		}
	}
	return findings
}

// unassessedExtensionFindings reports each extension a statement installs that
// the advisory data cannot call clean, each statement whose installs the dev
// database could not say, and each extension statement the reader could not
// read.
func unassessedExtensionFindings(file *File) []Finding {
	var findings []Finding
	for _, statement := range fileInstalls(file) {
		switch {
		case statement.unread != "":
			message := "this statement creates or updates an extension, but " + statement.unread +
				", so no advisory was checked for what it installs"
			findings = append(findings, extensionFinding(file, statement.index, "coverage/unassessed-extension", unassessedExtensionTitle, message))
			continue
		case !statement.answered:
			message := fmt.Sprintf("the dev database's server%s did not say what this statement installs for extension %s, "+
				"so no advisory was checked for it", serverApposition(file.extensions.Server), statement.request.Extension)
			findings = append(findings, extensionFinding(file, statement.index, "coverage/unassessed-extension", unassessedExtensionTitle, message))
			continue
		}
		for _, installed := range statement.installs {
			if len(installed.Unassessed) == 0 {
				continue
			}
			message := fmt.Sprintf("%s, which this statement installs%s, was not fully assessed against published advisories "+
				"on the dev database's server%s: %s",
				installedName(installed.InstalledExtension), installed.how(), serverName(file.extensions.Server),
				strings.Join(installed.Unassessed, "; "))
			findings = append(findings, extensionFinding(file, statement.index, "coverage/unassessed-extension", unassessedExtensionTitle, message))
		}
	}
	return findings
}

// statementInstalls is what one extension statement of a file installs, as
// the extension rules report it.
type statementInstalls struct {
	// index is the statement's index in the file.
	index int
	// request is what the statement names, and unread why the reader could
	// not read it, empty when it read it.
	request ExtensionRequest
	unread  string
	// answered reports that the dev database said what the statement
	// installs.
	answered bool
	// installs are what the statement installs where the database lacks it,
	// in the order the answer lists them.
	installs []reportedInstall
}

// reportedInstall is one extension a statement installs, and whether it
// arrives through CASCADE.
type reportedInstall struct {
	InstalledExtension
	cascaded bool
}

// fileInstalls walks the statements of an up file in order, from what the
// dev database held when the file's version started, and returns what each
// extension statement installs, for a run that has the dev database's
// answers and is not the compatibility surface's.
//
// A statement the reviewed-schema filter leaves out is not reported, as it is
// left out of every rule, but it still changes what the database holds. A DO
// block is not read, so after one the walk holds nothing, as it does after a
// DROP EXTENSION ... CASCADE or a DROP EXTENSION it cannot read: each may have
// dropped an extension, and a later statement is then judged as installing
// what it names rather than as installing nothing.
func fileInstalls(file *File) []statementInstalls {
	if file.extensions == nil || !file.IsUp || file.compatibility == CompatibilityProfileAtlas {
		return nil
	}
	held := maps.Clone(file.extensions.Held[file.Version])
	if held == nil {
		held = make(map[string]string)
	}
	var statements []statementInstalls
	for index := range file.Statements {
		statement, ok, err := pgextensioninstall.Read(file.Statements[index].SQL)
		switch {
		case !ok:
			if firstWordIs(&file.Statements[index], "DO") {
				clear(held)
			}
			continue
		case statement.Kind == pgextensioninstall.Drop:
			dropped(held, statement, err)
			continue
		case err != nil:
			if !file.scopeExcluded[index] {
				statements = append(statements, statementInstalls{index: index, unread: err.Error()})
			}
			continue
		}
		installs, found := statementAgainst(file.extensions, statement, held)
		if found && !file.scopeExcluded[index] {
			installs.index = index
			statements = append(statements, installs)
		}
	}
	return statements
}

// statementAgainst is what a read CREATE EXTENSION or ALTER EXTENSION ...
// UPDATE installs where the database holds held, which it updates. found is
// false for a statement that installs nothing: CREATE EXTENSION IF NOT EXISTS
// of an extension the database holds, or an update to the version it holds.
func statementAgainst(assessments *ExtensionAssessments, statement pgextensioninstall.Statement, held map[string]string) (statementInstalls, bool) {
	request := requestOf(statement)
	if _, holds := held[request.Extension]; holds && statement.IfNotExists {
		return statementInstalls{}, false
	}
	answer, ok := assessments.answerFor(request)
	if !ok || len(answer.Installs) == 0 {
		if statement.Kind == pgextensioninstall.Create {
			held[request.Extension] = ""
		}
		return statementInstalls{request: request}, true
	}
	if version, holds := held[request.Extension]; holds && statement.Kind == pgextensioninstall.Update && version == answer.Installs[0].Version {
		return statementInstalls{}, false
	}
	installs := statementInstalls{request: request, answered: true}
	for position, installed := range answer.Installs {
		if _, holds := held[installed.Extension]; holds && position > 0 {
			continue
		}
		installs.installs = append(installs.installs, reportedInstall{InstalledExtension: installed, cascaded: position > 0})
		held[installed.Extension] = installed.Version
	}
	return installs, true
}

// dropped removes what a DROP EXTENSION drops from held: the extensions it
// names, and everything when it cascades or could not be read, since what
// depends on them is not known here.
func dropped(held map[string]string, statement pgextensioninstall.Statement, err error) {
	if err != nil || statement.Cascade {
		clear(held)
		return
	}
	for _, name := range statement.Dropped {
		delete(held, name)
	}
}

// firstWordIs reports whether a statement's first word is word.
func firstWordIs(stmt *Statement, word string) bool {
	return len(stmt.Words) != 0 && stmt.Words[0] == word
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

// how says how the statement installs the extension in a message: by naming
// it, which needs no words, or through CASCADE.
func (r reportedInstall) how() string {
	if !r.cascaded {
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

// extensionStatementIndexes returns the indexes of the statements of an up
// file that create or update an extension, read or not, whether or not the
// run read the answers. The reviewed-schema filter leaves out what it leaves
// out of every rule.
func extensionStatementIndexes(file *File) []int {
	if !file.IsUp || file.compatibility == CompatibilityProfileAtlas {
		return nil
	}
	var indexes []int
	for index := range file.Statements {
		if file.scopeExcluded[index] {
			continue
		}
		statement, ok, _ := pgextensioninstall.Read(file.Statements[index].SQL)
		if ok && statement.Kind != pgextensioninstall.Drop {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

// requestOf is the request a read CREATE EXTENSION or ALTER EXTENSION ...
// UPDATE makes.
func requestOf(statement pgextensioninstall.Statement) ExtensionRequest {
	return ExtensionRequest{Extension: statement.Name, Version: statement.Version, Cascade: statement.Cascade}
}

// fileRequests returns the requests every read CREATE EXTENSION and ALTER
// EXTENSION ... UPDATE of an up file makes, the statements the reviewed-schema
// filter leaves out included, since they change what the database holds.
func fileRequests(file *File) []ExtensionRequest {
	var requests []ExtensionRequest
	for index := range file.Statements {
		statement, ok, err := pgextensioninstall.Read(file.Statements[index].SQL)
		if ok && err == nil && statement.Kind != pgextensioninstall.Drop {
			requests = append(requests, requestOf(statement))
		}
	}
	return requests
}

// extensionRequests returns every request the selected up files make, for a
// rule the run applies that reads the dev database's answers, each once and
// sorted, and the versions of the files that hold an extension statement, read
// or not, sorted. A run that has none to ask about makes no round trip.
func extensionRequests(files []File, opts Options, rules []Rule) ([]ExtensionRequest, []int64) {
	var requests []ExtensionRequest
	var versions []int64
	for i := range files {
		file := &files[i]
		if !file.Selected || !file.IsUp || file.compatibility == CompatibilityProfileAtlas || !extensionAnswersWanted(file, opts, rules) {
			continue
		}
		asked := fileRequests(file)
		if len(asked) == 0 && len(extensionStatementIndexes(file)) == 0 {
			continue
		}
		requests = append(requests, asked...)
		versions = append(versions, file.Version)
	}
	slices.SortFunc(requests, func(a, b ExtensionRequest) int {
		return cmp.Or(strings.Compare(a.Extension, b.Extension), strings.Compare(a.Version, b.Version), boolOrder(a.Cascade, b.Cascade))
	})
	slices.Sort(versions)
	return slices.Compact(requests), slices.Compact(versions)
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
