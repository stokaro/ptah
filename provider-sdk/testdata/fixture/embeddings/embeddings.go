// Package embeddings serves the fixture's embedding store on SQLite, the
// service contract 25.2 adds. It lives apart from package backend, which keeps
// to contract 25.0, and reaches a session only through
// backend.Provider.WithSession.
//
// The run-state records are kept whole, as their deterministic Protobuf
// encoding, in tables of the fixture's own. A generation's vector is the
// little-endian float32 values of a BLOB column, beside four TEXT columns that
// record what wrote it, on the target table or in a table the generation owns.
// The fixture builds no vector index, so every search is an exhaustive scan.
package embeddings

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// The fixture's own tables. Every record is a row of stateTable, and every
// event a row of eventTable; ownerTable records which table a generation owns.
const (
	stateTable = "fixture_embedding_state"
	eventTable = "fixture_embedding_event"
	ownerTable = "fixture_embedding_owner"
)

// layout is the statements that create the store, by table. A table whose
// stored statement differs is not the fixture's, and preparing refuses it.
var layout = []struct{ name, statement string }{
	{stateTable, `CREATE TABLE "` + stateTable + `" ("kind" TEXT NOT NULL, "key" TEXT NOT NULL, "record" BLOB NOT NULL, PRIMARY KEY ("kind", "key"))`},
	{eventTable, `CREATE TABLE "` + eventTable + `" ("run_id" TEXT NOT NULL, "sequence" INTEGER NOT NULL, "record" BLOB NOT NULL, PRIMARY KEY ("run_id", "sequence"))`},
	{ownerTable, `CREATE TABLE "` + ownerTable + `" ("table_name" TEXT NOT NULL PRIMARY KEY, "mark" TEXT NOT NULL)`},
}

// The kinds of record in stateTable.
const (
	generationKind = "generation"
	runKind        = "run"
	pointerKind    = "pointer"
)

// Representation is the element type a fixture generation stores, and
// ColumnType the type of the column that holds it.
const (
	Representation = "float"
	ColumnType     = "BLOB"
)

// CrashAfterCommit is the fault that ends the process after a batch commit
// has committed and before it answers.
const CrashAfterCommit = "embeddings-crash-after-commit"

// Provider serves the base fixture and its embedding store.
type Provider struct {
	backend.Provider
}

// Description is the fixture's description with the embedding store, whose
// engine builds immutable generations.
func Description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity = "fixture-embeddings-1"
	description.Features = append(description.Features, provider.EmbeddingStoreFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.EmbeddingStoreFeature)
	engine.Embeddings = &protocol.EmbeddingEngine{ConsistencyModes: []string{"immutable"}}
	return description
}

// errStoreMissing is a write against a store nobody prepared.
var errStoreMissing = errors.New("the embedding store is not prepared")

// How a call's transaction begins: a read takes a snapshot, and a write takes
// the database's write lock at once, so two sessions on one file serialize
// their writes.
const (
	reading = "BEGIN DEFERRED"
	writing = "BEGIN IMMEDIATE"
)

// withConnection runs use on the session's database connection, inside a
// transaction that begins as begin says. The transaction commits when use
// returns nil and rolls back otherwise.
func (p *Provider) withConnection(ctx context.Context, sessionID, begin string, use func(*sql.Conn, string) error) error {
	return p.WithSession(ctx, sessionID, "", func(access backend.Access) error {
		if access.DB == nil {
			return fmt.Errorf("the fixture session has no database")
		}
		connection, err := access.DB.Conn(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = connection.Close() }()
		if _, err := connection.ExecContext(ctx, "PRAGMA busy_timeout = 10000"); err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, begin); err != nil {
			return err
		}
		if err := use(connection, access.Fault); err != nil {
			_, rollback := connection.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
			return errors.Join(err, rollback)
		}
		_, err = connection.ExecContext(ctx, "COMMIT")
		return err
	})
}

// storeExists reports whether every table of the store is there.
func storeExists(ctx context.Context, connection *sql.Conn) (bool, error) {
	for _, table := range layout {
		exists, err := tableExists(ctx, connection, table.name)
		if err != nil || !exists {
			return false, err
		}
	}
	return true, nil
}

// tableExists reports whether a table of that name is there, compared as
// SQLite compares names.
func tableExists(ctx context.Context, connection *sql.Conn, name string) (bool, error) {
	var count int
	err := connection.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE", name).Scan(&count)
	return count != 0, err
}

// PrepareEmbeddingStore creates the store's tables when they are missing and
// refuses a table of one of their names that the fixture did not create.
func (p *Provider) PrepareEmbeddingStore(ctx context.Context, request *protocol.PrepareEmbeddingStoreRequest) (*protocol.EmbeddingWriteResult, error) {
	var refusal *protocol.EmbeddingRefusal
	err := p.withConnection(ctx, request.GetSessionId(), writing, func(connection *sql.Conn, _ string) error {
		for _, table := range layout {
			var stored string
			err := connection.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE", table.name).Scan(&stored)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				if _, err := connection.ExecContext(ctx, table.statement); err != nil {
					return err
				}
			case err != nil:
				return err
			case stored != table.statement:
				refusal = refused(protocol.EmbeddingRefusal_NOT_OWNED, "table "+table.name+" is not the fixture's embedding store")
				return errRefused
			}
		}
		return nil
	})
	return writeResult(request.GetRequestId(), refusal, err)
}

// errRefused rolls back a write that answers with a refusal.
var errRefused = errors.New("refused")

// refused builds a refusal.
func refused(reason protocol.EmbeddingRefusal_Reason, message string) *protocol.EmbeddingRefusal {
	return &protocol.EmbeddingRefusal{Reason: reason, Message: message}
}

// writeResult answers a write: APPLIED when it committed, REFUSED when it
// rolled back for a refusal, and an error otherwise.
func writeResult(requestID string, refusal *protocol.EmbeddingRefusal, err error) (*protocol.EmbeddingWriteResult, error) {
	switch {
	case refusal != nil && (err == nil || errors.Is(err, errRefused)):
		return &protocol.EmbeddingWriteResult{RequestId: requestID, Outcome: protocol.EmbeddingWriteResult_REFUSED, Refusal: refusal}, nil
	case errors.Is(err, errStoreMissing):
		return &protocol.EmbeddingWriteResult{RequestId: requestID, Outcome: protocol.EmbeddingWriteResult_REFUSED,
			Refusal: refused(protocol.EmbeddingRefusal_ABSENT, "the embedding store is not prepared")}, nil
	case err != nil:
		return nil, err
	}
	return &protocol.EmbeddingWriteResult{RequestId: requestID, Outcome: protocol.EmbeddingWriteResult_APPLIED}, nil
}

// quote renders a name as an SQLite identifier.
func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// tableName returns a reference's table, refusing a schema other than main,
// the only one the fixture has.
func tableName(table *protocol.Reference) (string, error) {
	if schema := table.GetSchema(); schema != "" && !strings.EqualFold(schema, "main") {
		return "", fmt.Errorf("the fixture has no schema other than main")
	}
	return table.GetName(), nil
}
