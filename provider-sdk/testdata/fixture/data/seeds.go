package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ledgerDDL is the only ledger layout the fixture owns. A table of that name
// with another definition is refused, never adopted.
const ledgerDDL = `CREATE TABLE "fixture_seed_ledger"` +
	` ("path" TEXT NOT NULL PRIMARY KEY, "checksum" TEXT NOT NULL, "env" TEXT NOT NULL, "applied_at" INTEGER NOT NULL)`

// ListSeedTables lists the tables of the database, the seed ledger left out.
func (p *Provider) ListSeedTables(ctx context.Context, request *protocol.ListSeedTablesRequest) (*protocol.SeedTables, error) {
	if schema := request.GetSchema(); schema != "" && !strings.EqualFold(schema, "main") {
		return nil, fmt.Errorf("the fixture has no schema other than main")
	}
	result := &protocol.SeedTables{}
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		rows, err := access.Querier.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name <> ? ORDER BY name", LedgerTable)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			result.Tables = append(result.Tables, &protocol.Reference{Kind: "table", Schema: request.GetSchema(), Name: name})
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ApplySeed runs a file and records it in one transaction. A statement
// SQLite runs only outside a transaction refuses the file before anything
// runs; a key conflict and every other failure roll the transaction back.
// The fault lost-seed-reply commits and then loses the reply, as a crash
// after the commit would.
func (p *Provider) ApplySeed(ctx context.Context, request *protocol.ApplySeedRequest) (*protocol.SeedResult, error) {
	seed := request.GetSeed()
	result := &protocol.SeedResult{RequestId: request.GetRequestId(), Path: seed.GetPath()}
	statements := splitSeed(seed.GetText())
	for _, statement := range statements {
		if reason := outsideTransaction(statement.text); reason != "" {
			result.Outcome, result.Reason, result.Position = protocol.SeedResult_REFUSED, reason, statement.position
			return result, nil
		}
	}
	lost := false
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		lost = access.Fault == "lost-seed-reply"
		return inTransaction(ctx, access, func(tx *sql.Tx) error {
			for _, statement := range statements {
				if _, err := tx.ExecContext(ctx, statement.text); err != nil {
					if strings.Contains(err.Error(), "UNIQUE constraint failed") {
						return errKeyConflict
					}
					return fmt.Errorf("the statement at line %d failed", statement.position.GetLine())
				}
			}
			return record(ctx, tx, seed)
		})
	})
	switch {
	case errors.Is(err, errKeyConflict):
		result.Outcome = protocol.SeedResult_KEY_CONFLICT
	case errors.Is(err, errUnknown):
		result.Outcome, result.Reason = protocol.SeedResult_UNKNOWN, "the fixture could not finish its transaction"
	case err != nil:
		result.Outcome, result.Reason = protocol.SeedResult_NOT_APPLIED, oneLine(err.Error())
	case lost:
		return nil, fmt.Errorf("fixture lost the seed reply after its commit")
	default:
		result.Outcome = protocol.SeedResult_APPLIED
	}
	return result, nil
}

// RecordSeed writes a ledger entry without running the file.
func (p *Provider) RecordSeed(ctx context.Context, request *protocol.RecordSeedRequest) (*protocol.SeedResult, error) {
	result := &protocol.SeedResult{RequestId: request.GetRequestId(), Path: request.GetSeed().GetPath()}
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		return inTransaction(ctx, access, func(tx *sql.Tx) error { return record(ctx, tx, request.GetSeed()) })
	})
	switch {
	case errors.Is(err, errUnknown):
		result.Outcome, result.Reason = protocol.SeedResult_UNKNOWN, "the fixture could not finish its transaction"
	case err != nil:
		result.Outcome, result.Reason = protocol.SeedResult_NOT_APPLIED, oneLine(err.Error())
	default:
		result.Outcome = protocol.SeedResult_APPLIED
	}
	return result, nil
}

// ReadSeedLedger reads every entry. It never creates the ledger.
func (p *Provider) ReadSeedLedger(ctx context.Context, request *protocol.ReadSeedLedgerRequest) (*protocol.SeedLedger, error) {
	result := &protocol.SeedLedger{Complete: true}
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		exists, err := ledgerExists(ctx, access.Querier)
		if err != nil || !exists {
			return err
		}
		result.Exists = true
		rows, err := access.Querier.QueryContext(ctx, `SELECT "path", "checksum", "env", "applied_at" FROM "fixture_seed_ledger" ORDER BY "path"`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var entry protocol.SeedLedgerEntry
			var applied int64
			if err := rows.Scan(&entry.Path, &entry.Checksum, &entry.Env, &applied); err != nil {
				return err
			}
			moment := time.Unix(0, applied).UTC()
			entry.AppliedAtSeconds, entry.AppliedAtNanos = moment.Unix(), int32(moment.Nanosecond()) // #nosec G115 -- a nanosecond count is below 1e9
			if len(result.Entries) == int(request.GetMaxEntries()) {
				return fmt.Errorf("the seed ledger holds more entries than requested")
			}
			result.Entries = append(result.Entries, &entry)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

var (
	errKeyConflict = errors.New("key conflict")
	errUnknown     = errors.New("unknown transaction outcome")
)

// inTransaction runs use in a transaction of the session's database, which
// it commits when use succeeds. A rollback that fails, or a commit that does,
// leaves the outcome unknown.
func inTransaction(ctx context.Context, access backend.Access, use func(*sql.Tx) error) error {
	if access.DB == nil {
		return fmt.Errorf("a seed runs outside the session's transaction")
	}
	tx, err := access.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := use(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return errUnknown
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return errUnknown
	}
	return nil
}

// record creates a missing ledger, refuses one the fixture does not own and
// replaces the entry for the seed's path.
func record(ctx context.Context, tx *sql.Tx, seed *protocol.Seed) error {
	exists, err := ledgerExists(ctx, tx)
	if err != nil {
		return err
	}
	if !exists {
		if _, err := tx.ExecContext(ctx, ledgerDDL); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO "fixture_seed_ledger" ("path", "checksum", "env", "applied_at") VALUES (?, ?, ?, ?)`,
		seed.GetPath(), seed.GetChecksum(), seed.GetEnv(), time.Now().UTC().UnixNano())
	return err
}

func ledgerExists(ctx context.Context, queries backend.Querier) (bool, error) {
	var stored sql.NullString
	err := queries.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE", LedgerTable).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if stored.String != ledgerDDL {
		return false, fmt.Errorf("the seed ledger has a layout the fixture does not own")
	}
	return true, nil
}

// statement is one statement of a seed file and where it starts.
type statement struct {
	text     string
	position *protocol.SourcePosition
}

// splitSeed cuts a seed file at the semicolons outside quotes and comments.
func splitSeed(text string) []statement {
	var statements []statement
	var scan seedScanner
	start := -1
	var at *protocol.SourcePosition
	for offset, r := range text {
		code := scan.step(text, offset, r)
		if start < 0 && code && !strings.ContainsRune("; \t\r\n", r) {
			start, at = offset, scan.position(offset)
		}
		if r == ';' && code && start >= 0 {
			statements = append(statements, statement{text: text[start : offset+1], position: at})
			start = -1
		}
		scan.advance(r)
	}
	if start >= 0 {
		statements = append(statements, statement{text: text[start:], position: at})
	}
	return statements
}

// seedScanner tracks where a seed file's text is code rather than a quoted
// literal or a comment, and the line and column of each rune.
type seedScanner struct {
	quote        rune
	comment      bool
	line, column uint32
}

// step reads the rune at offset and reports whether it is code.
func (s *seedScanner) step(text string, offset int, r rune) bool {
	switch {
	case s.comment:
		s.comment = r != '\n'
		return false
	case s.quote != 0:
		if r == s.quote {
			s.quote = 0
		}
		return false
	case r == '-' && strings.HasPrefix(text[offset:], "--"):
		s.comment = true
		return false
	case r == '\'' || r == '"':
		s.quote = r
	}
	return true
}

func (s *seedScanner) position(offset int) *protocol.SourcePosition {
	return &protocol.SourcePosition{ByteOffset: uint64(offset), Line: s.line + 1, Column: s.column + 1} // #nosec G115 -- an offset is not negative
}

func (s *seedScanner) advance(r rune) {
	if r == '\n' {
		s.line, s.column = s.line+1, 0
		return
	}
	s.column++
}

// outsideTransaction names a statement SQLite cannot run inside the seed's
// transaction, or returns "".
func outsideTransaction(text string) string {
	fields := strings.Fields(strings.ToUpper(text))
	if len(fields) == 0 {
		return ""
	}
	switch strings.TrimSuffix(fields[0], ";") {
	case "VACUUM":
		return "VACUUM runs only outside a transaction"
	case "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return "a seed file cannot control its own transaction"
	}
	return ""
}

func oneLine(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	for len(text) > 512 {
		_, size := utf8.DecodeLastRuneInString(text)
		text = text[:len(text)-size]
	}
	return text
}
