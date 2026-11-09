// Package migrationunits serves units.migration-split of contract 25.12 on the
// fixture's SQLite, with a toy grammar that needs a split the host cannot
// make itself:
//
//   - `DEFINE name = literal;` exists only in the unit that carries it, so
//     every later unit's text repeats it, and `$name` reads it. A unit's text
//     therefore differs from its source.
//   - a run of INSERT, UPDATE and DELETE statements is one unit that changes
//     data only, which the host commits together with its checkpoint;
//   - every other statement is a unit of its own that runs outside a
//     transaction;
//   - `COMMIT` is refused, by its byte range.
//
// SLOW is a statement of the toy grammar that runs until the statement timeout
// stops it. An assertion is read-only when it is one SELECT that reads no
// table named external_*, which the host's own rules would accept.
//
// The environment selects the session and the faults:
// FIXTURE_UNITS_DDL=transactional opens sessions with transactional DDL,
// FIXTURE_UNITS_ABORT_COMMITS=n aborts
// the first n commits of a transaction that ran a unit,
// FIXTURE_UNITS_ABORT_EXECUTIONS=n aborts the first n units run in a caller
// transaction, FIXTURE_UNITS_LOSE_COMMIT=n loses the reply of the nth such
// commit after it lands, FIXTURE_UNITS_LOSE_READS=n fails the first n history
// reads after that, and FIXTURE_UNITS_LOG names a file that records each split,
// transaction, execution and history write the host asked for, one line each.
// The address fault crash-after ends the process after a unit applies, as the
// fixture's own executor does.
package migrationunits

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// Provider is the fixture with migration splits.
type Provider struct {
	backend.Provider
	// TransactionalDDL is the capability a session reports.
	TransactionalDDL bool
	// AbortCommits, AbortExecutions, LoseCommit and LoseReads are the
	// faults the environment selected.
	AbortCommits, AbortExecutions, LoseCommit, LoseReads int
	// Log is the file each call is recorded in, or empty.
	Log string

	mu        sync.Mutex
	unitTx    map[string]bool
	aborted   int
	landed    int
	executes  int
	failReads int
}

// FromEnvironment reads the settings the package comment lists.
func FromEnvironment() (*Provider, error) {
	p := &Provider{Log: os.Getenv("FIXTURE_UNITS_LOG")}
	switch ddl := os.Getenv("FIXTURE_UNITS_DDL"); ddl {
	case "":
	case "transactional":
		p.TransactionalDDL = true
	default:
		return nil, fmt.Errorf("FIXTURE_UNITS_DDL: unknown value %q", ddl)
	}
	for _, setting := range []struct {
		name  string
		value *int
	}{{"FIXTURE_UNITS_ABORT_COMMITS", &p.AbortCommits}, {"FIXTURE_UNITS_ABORT_EXECUTIONS", &p.AbortExecutions}, {"FIXTURE_UNITS_LOSE_COMMIT", &p.LoseCommit},
		{"FIXTURE_UNITS_LOSE_READS", &p.LoseReads}} {
		raw := os.Getenv(setting.name)
		if raw == "" {
			continue
		}
		value, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", setting.name, err)
		}
		*setting.value = value
	}
	return p, nil
}

// Features are what the provider and its engine declare beside the
// fixture's own: transactions, history, locks and metadata, which the host
// runs units and checkpoints with, operator statements for checks, and the
// split. A statement timeout is the provider's alone.
func Features() (providerFeatures, engineFeatures []string) {
	services := []string{provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature, provider.MetadataFeature,
		provider.StatementsFeature, provider.MigrationSplitFeature}
	return append(slices.Clone(services), provider.StatementTimeoutFeature), services
}

// Description is the fixture's description with the migration split.
func Description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity = "fixture-migration-units-1"
	providerFeatures, engineFeatures := Features()
	description.Features = append(description.Features, providerFeatures...)
	engine := description.Engines[0]
	engine.Features = append(slices.DeleteFunc(engine.Features, func(feature string) bool {
		return feature == provider.CommonTableIdentityFeature
	}), engineFeatures...)
	return description
}

// Open reports the transactional DDL the environment selected.
func (p *Provider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Capabilities = map[string]bool{"transactional_ddl": p.TransactionalDDL}
	return session, nil
}

// recordf writes one line of the call log.
func (p *Provider) recordf(format string, args ...any) {
	if p.Log == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	file, err := os.OpenFile(p.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) // #nosec G304 G703 -- the host selects this log file in its private test directory
	if err != nil {
		return
	}
	// A line the log lost makes the test that reads it fail; nothing else
	// reads the log.
	_, _ = fmt.Fprintf(file, format+"\n", args...)
	_ = file.Close()
}

// SplitMigration splits one body by the toy grammar.
func (p *Provider) SplitMigration(_ context.Context, request *protocol.SplitMigrationRequest) (*protocol.MigrationSplit, error) {
	p.recordf("split %s", strconv.Quote(request.GetBody()))
	units, refusal := Split(request.GetBody())
	result := &protocol.MigrationSplit{Units: units, Refusal: refusal}
	for _, assertion := range request.GetAssertions() {
		result.Assertions = append(result.Assertions, Verdict(assertion))
	}
	return result, nil
}

// statement is one statement of a body: its byte range, from its first token
// through its semicolon, and its text without the semicolon.
type statement struct {
	start, end int
	text       string
}

func (s statement) word() string {
	fields := strings.Fields(s.text)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(fields[0])
}

// statements reads body into statements, skipping line comments and reading
// a semicolon inside a quoted literal as part of it.
func statements(body string) []statement {
	var result []statement
	start := -1
	quoted := false
	finish := func(end int) {
		if start < 0 {
			return
		}
		text := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(body[start:end]), ";"))
		if text != "" {
			result = append(result, statement{start: start, end: start + len(strings.TrimRight(body[start:end], " \t\r\n")), text: text})
		}
		start = -1
	}
	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch {
		case quoted:
			quoted = ch != '\''
		case ch == '\'':
			quoted = true
			if start < 0 {
				start = i
			}
		case ch == '-' && i+1 < len(body) && body[i+1] == '-':
			for i < len(body) && body[i] != '\n' {
				i++
			}
		case ch == ';':
			finish(i + 1)
		case ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n' && start < 0:
			start = i
		}
	}
	finish(len(body))
	return result
}

// TransactionControlReason is why COMMIT is refused.
const TransactionControlReason = "the statement controls a transaction, and the host runs each data unit in a transaction of its own"

// Split returns the units of body, or the refusal of its first COMMIT.
func Split(body string) ([]*protocol.MigrationUnit, *protocol.SplitRefusal) {
	var units []*protocol.MigrationUnit
	var defines []string
	var run []statement
	unit := func(own []statement, placement protocol.MigrationUnit_Placement) *protocol.MigrationUnit {
		texts := slices.Clone(defines)
		sources := make([]string, 0, len(own))
		for _, read := range own {
			texts = append(texts, read.text)
			sources = append(sources, body[read.start:read.end])
		}
		return &protocol.MigrationUnit{Sql: strings.Join(texts, ";\n"), Source: strings.Join(sources, "\n"), Placement: placement}
	}
	flush := func() {
		if len(run) > 0 {
			units = append(units, unit(run, protocol.MigrationUnit_WITH_CHECKPOINT))
			run = nil
		}
	}
	for _, read := range statements(body) {
		switch read.word() {
		case "COMMIT":
			return nil, &protocol.SplitRefusal{Start: uint32(read.start), End: uint32(read.end), Reason: TransactionControlReason} // #nosec G115 -- a body is far below 4 GiB
		case "DEFINE":
			flush()
			defines = append(defines, read.text)
		case "INSERT", "UPDATE", "DELETE":
			run = append(run, read)
		default:
			flush()
			units = append(units, unit([]statement{read}, protocol.MigrationUnit_OUTSIDE))
		}
	}
	flush()
	return units, nil
}

// Verdict judges one assertion.
func Verdict(assertion string) *protocol.AssertionVerdict {
	text := strings.ToUpper(strings.TrimSpace(assertion))
	switch {
	case !strings.HasPrefix(text, "SELECT") || strings.Contains(text, ";"):
		return &protocol.AssertionVerdict{Reason: "the assertion is not one SELECT statement"}
	case strings.Contains(text, "EXTERNAL_"):
		return &protocol.AssertionVerdict{Reason: "the assertion reads an external table, whose rows are not in the database"}
	default:
		return &protocol.AssertionVerdict{ReadOnly: true}
	}
}

// slowQuery runs until its context ends.
const slowQuery = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"

// expand reads a unit's text: its DEFINE statements give the literal each
// `$name` after them stands for.
func expand(unit string) ([]string, error) {
	names := make(map[string]string)
	var run []string
	for _, read := range statements(unit) {
		if read.word() == "DEFINE" {
			name, literal, found := strings.Cut(strings.TrimSpace(read.text[len("DEFINE"):]), "=")
			if !found {
				return nil, fmt.Errorf("malformed DEFINE")
			}
			names["$"+strings.TrimSpace(name)] = strings.TrimSpace(literal)
			continue
		}
		text := read.text
		for name, literal := range names {
			text = strings.ReplaceAll(text, name, literal)
		}
		if strings.Contains(text, "$") {
			return nil, fmt.Errorf("unknown name in %q", text)
		}
		run = append(run, text)
	}
	return run, nil
}

// Execute runs each unit whole or not at all, bounded by the statement
// timeout the request carries. A unit that fails, or that the timeout stops,
// applied nothing: it runs under a savepoint its failure rolls back to.
func (p *Provider) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	transaction := request.GetTransactionId()
	for _, unit := range request.GetUnits() {
		p.recordf("execute tx=%t timeout=%s sql=%s", transaction != "", time.Duration(request.GetStatementTimeoutNanoseconds()), strconv.Quote(unit.GetSql()))
	}
	if transaction != "" {
		p.mu.Lock()
		if p.unitTx == nil {
			p.unitTx = make(map[string]bool)
		}
		p.unitTx[transaction] = true
		p.executes++
		aborted := p.executes <= p.AbortExecutions
		p.mu.Unlock()
		if aborted {
			p.recordf("abort execution")
			return &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED, TransactionAborted: true}, nil
		}
	}
	result := &protocol.Execution{Outcome: protocol.Execution_APPLIED}
	err := p.WithSession(ctx, request.GetSessionId(), transaction, func(access backend.Access) error {
		for _, unit := range request.GetUnits() {
			run, err := expand(unit.GetSql())
			if err == nil {
				err = atomically(ctx, access.Querier, run, time.Duration(request.GetStatementTimeoutNanoseconds()))
			}
			if err != nil {
				result.Outcome = protocol.Execution_NOT_APPLIED
				if result.AppliedCount > 0 {
					result.Outcome = protocol.Execution_PARTIAL
				}
				result.StatementTimedOut = errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
				p.recordf("refused %s", strconv.Quote(err.Error()))
				return nil
			}
			result.AppliedCount++
			if access.Fault == "crash-after" {
				os.Exit(23) //revive:disable-line:deep-exit this fixture tests a provider crash after a committed write
			}
		}
		return nil
	})
	if err != nil {
		return &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED}, nil
	}
	return result, nil
}

// atomically runs one unit's statements under a savepoint, and rolls back to
// it when one fails or the timeout stops one.
func atomically(ctx context.Context, querier backend.Querier, run []string, limit time.Duration) error {
	bounded, cancel := ctx, context.CancelFunc(func() {})
	if limit > 0 {
		bounded, cancel = context.WithTimeout(ctx, limit)
	}
	defer cancel()
	cleanup := context.WithoutCancel(ctx)
	savepoint := "unit_" + strings.ToLower(rand.Text())
	if _, err := querier.ExecContext(cleanup, "SAVEPOINT "+savepoint); err != nil {
		return err
	}
	for _, text := range run {
		var err error
		if strings.EqualFold(text, "SLOW") {
			var count int64
			err = querier.QueryRowContext(bounded, slowQuery).Scan(&count)
			if bounded.Err() != nil {
				err = bounded.Err()
			}
		} else {
			_, err = querier.ExecContext(bounded, text)
		}
		if err != nil {
			_, rollbackErr := querier.ExecContext(cleanup, "ROLLBACK TO "+savepoint)
			_, releaseErr := querier.ExecContext(cleanup, "RELEASE "+savepoint)
			return errors.Join(err, rollbackErr, releaseErr)
		}
	}
	_, err := querier.ExecContext(cleanup, "RELEASE "+savepoint)
	return err
}

// BeginTransaction records the transaction the host opened.
func (p *Provider) BeginTransaction(ctx context.Context, request *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	p.recordf("begin isolation=%s", request.GetIsolation())
	return p.Provider.BeginTransaction(ctx, request)
}

// FinishTransaction aborts or loses the commit of a transaction that ran a
// unit, as the environment selected.
func (p *Provider) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	p.mu.Lock()
	commit := request.GetAction() == protocol.FinishTransactionRequest_COMMIT && p.unitTx[request.GetTransactionId()]
	abort := commit && p.aborted < p.AbortCommits
	if abort {
		p.aborted++
	} else if commit {
		p.landed++
	}
	lose := commit && !abort && p.landed == p.LoseCommit
	if lose {
		p.failReads = p.LoseReads
	}
	p.mu.Unlock()
	if abort {
		rollback := proto.CloneOf(request)
		rollback.Action = protocol.FinishTransactionRequest_ROLLBACK
		result, err := p.Provider.FinishTransaction(ctx, rollback)
		if err != nil {
			return nil, err
		}
		p.recordf("abort commit outcome=%s", result.GetOutcome())
		return &protocol.TransactionResult{Outcome: result.GetOutcome(), Aborted: result.GetOutcome() == protocol.TransactionResult_ROLLED_BACK}, nil
	}
	result, err := p.Provider.FinishTransaction(ctx, request)
	p.recordf("finish action=%s outcome=%s", request.GetAction(), result.GetOutcome())
	if lose && err == nil {
		return nil, fmt.Errorf("fixture lost the commit reply")
	}
	return result, err
}

// ReadHistory fails the reads the environment asked for after a lost commit
// reply, so the host cannot settle the commit by reading the checkpoint back.
func (p *Provider) ReadHistory(ctx context.Context, request *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error) {
	p.mu.Lock()
	fail := p.failReads > 0
	p.failReads = max(p.failReads-1, 0)
	p.mu.Unlock()
	if fail {
		p.recordf("lose read")
		return nil, fmt.Errorf("fixture lost the history read")
	}
	return p.Provider.ReadHistory(ctx, request)
}

// MutateHistory records each history write: whether it joined a transaction,
// the progress it writes and its error column.
func (p *Provider) MutateHistory(ctx context.Context, request *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error) {
	replacement := request.GetReplacement()
	p.recordf("history tx=%t applied=%d error=%s", request.GetScope().GetTransactionId() != "", replacement.GetApplied(), strconv.Quote(replacement.GetError()))
	return p.Provider.MutateHistory(ctx, request)
}

// AnalyzeSQL reads each source by the toy grammar for lint. Every statement
// is behavioral, since the host's common assessment does not read the grammar,
// and a statement's span starts where the one before it ended, so a comment
// before it is part of it.
func (p *Provider) AnalyzeSQL(_ context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	result := &protocol.SQLAnalysis{Complete: true}
	for _, source := range request.GetSources() {
		file := &protocol.SQLFile{}
		end := 0
		read := statements(source)
		for i, statement := range read {
			start := len(source) - len(strings.TrimLeft(source[end:], " \t\r\n"))
			stop := statement.end
			if i == len(read)-1 {
				stop = max(stop, len(strings.TrimRight(source, " \t\r\n")))
			}
			words := strings.Fields(statement.text)
			file.Statements = append(file.Statements, &protocol.SQLStatement{
				Start: uint32(start), End: uint32(stop), Line: uint32(1 + strings.Count(source[:start], "\n")), // #nosec G115 -- a source is far below 4 GiB
				Words: words, SourceWords: words, Canonical: strings.Join(words, " "),
				Safety: protocol.SQLStatement_BEHAVIORAL, Reason: "a statement of the fixture's migration grammar",
			})
			end = stop
		}
		result.Files = append(result.Files, file)
	}
	return result, nil
}
