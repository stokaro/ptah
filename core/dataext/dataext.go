// Package dataext declares the services an engine's owner provides for managed
// data and seeds, where what the engine does cannot be stated as data: writing
// a value as a literal of its column's type, typing the rows a comparison
// pairs, and running a seed file as the engine's own client would.
//
// The literal and row services are pure: migration/datadiff and the
// managed-data comparison receive them from the runtime that loaded the
// owner, through engine.Runtime.DataServices. A seed runs through the
// database driver, which the rendering runtime does not link, so the seed
// service comes with the connection instead
// (dbschema.DatabaseConnection.Seeds). Either way the shared packages name no
// engine. The dialect descriptor says which engines need which service
// (descriptor.Literals.ByOwner, descriptor.ManagedData.TypedRows,
// descriptor.Seeds.ByOwner), and a shared package refuses such an engine when
// it is given no service rather than fall back to the generic spelling.
//
// An engine whose owner spells every row write itself, as an executable
// provider does, gives a [Writes] service instead of literals, and its seed
// service may keep its own ledger ([SeedLedger]).
//
// The package holds contracts only. It links no engine, so a provider in
// another module implements it without linking Ptah's engines.
package dataext

import (
	"context"
	"database/sql"
	"errors"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
)

// Services are the data services one engine's owner provides. A nil member is
// a service the engine does not have, which is what every engine whose
// descriptor asks for none has. The zero value provides nothing.
type Services struct {
	// Literals writes the literals whose form a column's declared type
	// decides.
	Literals Literals
	// Columns answers what a column's declared type decides about the
	// literal it takes, for an engine whose literals are otherwise written
	// from the value.
	Columns Columns
	// Rows types managed rows before they are compared, and the rows a
	// checkpoint bootstraps before they are written.
	Rows Rows
	// Writes renders whole row writes, for an engine whose owner spells
	// every statement that writes a row. With it the caller writes no
	// statement text of its own, and Literals and Columns are not consulted.
	Writes Writes
}

// WriteMode is the operation a [RowWrite] performs.
type WriteMode int

const (
	// WriteInsert inserts a row.
	WriteInsert WriteMode = iota + 1
	// WriteUpsert inserts a row, or writes it over the row with the same key.
	// An engine that has no such statement refuses it.
	WriteUpsert
	// WriteUpdateByKey sets the columns outside the key on the row the key
	// columns identify.
	WriteUpdateByKey
	// WriteDeleteByKey deletes the row the key columns identify.
	WriteDeleteByKey
)

// RowWrite is one row operation a [Writes] service renders.
type RowWrite struct {
	// Mode is the operation.
	Mode WriteMode
	// Schema and Table name the table; Schema is empty for the connection's
	// own schema.
	Schema, Table string
	// Columns are the columns written, key columns included, in the order
	// Values follows. A delete writes its key columns alone.
	Columns []string
	// Keys are the columns that identify the row.
	Keys []string
	// Values holds one value per column, in the form [Rows.TypeRows] gives
	// it: nil is null.
	Values []any
	// ColumnTypes is the engine type of each column, as [Rows.TypeRows]
	// names it. A column it does not name has no type the owner gave.
	ColumnTypes map[string]string
}

// Writes renders row writes for an engine whose owner spells every
// statement that writes a row.
type Writes interface {
	// RenderWrites returns one complete statement per write, in input order,
	// for a target with caps. The same writes give the same statements, so
	// a saved plan and a migration file cannot disagree about a row. An
	// error, and no statement, refuses the whole batch: a write in a mode or
	// with a value the engine cannot take, or a write the owner can perform
	// only as something other than statement text.
	RenderWrites(ctx context.Context, caps capability.Capabilities, writes []RowWrite) ([]string, error)
}

// Literals writes a value as a literal for a column, for an engine whose
// literal depends on the column's declared type rather than on the value's Go
// type alone.
type Literals interface {
	// Literal returns value written as a literal for a column declared as
	// declaredType, which is empty when the caller does not know the type.
	// handled is false where the engine takes the literal the value's Go type
	// gives, and the caller writes that one. An error names why the value has
	// no literal in that column, without naming the column, which the caller
	// adds.
	Literal(declaredType string, value any) (literal string, handled bool, err error)
}

// Columns answers what a column's declared type decides about the literal it
// takes, for an engine whose type map decides which columns refuse a literal
// the value's Go type would give.
type Columns interface {
	// HoldsMoment reports whether a column declared as declaredType holds a
	// moment and refuses a string literal. The caller writes the moment a
	// string names instead, and refuses a string that names none.
	HoldsMoment(declaredType string) bool
}

// Rows types the rows of one managed table before they are compared, for an
// engine that compares and writes each value in its column's own type.
type Rows interface {
	// TypeRows returns the engine type of each column and the declared and
	// live rows with every value in the form the engine compares it in. ctx
	// bounds an owner that types the rows in another process. The returned
	// rows are new; the request's rows are not modified.
	TypeRows(ctx context.Context, request RowsRequest) (TypedRows, error)
}

// RowsRequest is one table's rows for [Rows.TypeRows].
type RowsRequest struct {
	// Capabilities are the target's capabilities, which decide the type a
	// declared column lands on.
	Capabilities capability.Capabilities
	// Schema and Table name the table the rows belong to; Schema is empty for
	// the connection's own schema.
	Schema, Table string
	// LiveTable is the table as the database holds it, nil when it does not
	// exist yet.
	LiveTable *catalog.Table
	// DeclaredTypes is each column's declared type.
	DeclaredTypes map[string]string
	// Desired are the declared rows, and Live the rows the database holds.
	Desired, Live []map[string]any
}

// TypedRows is a table's column types and its declared and live rows in the
// form [Rows.TypeRows] gives them.
type TypedRows struct {
	// ColumnTypes is the engine type of each column a statement writes.
	ColumnTypes map[string]string
	// Desired are the declared rows.
	Desired []map[string]any
	// Live are the rows the database holds.
	Live []map[string]any
}

// Database is the connection a seed service works through.
// *dbschema.DatabaseConnection satisfies it.
type Database interface {
	// BeginTx starts a transaction on the target.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	// Conn reserves one session of the target, which the caller closes.
	Conn(ctx context.Context) (*sql.Conn, error)
}

// Seed is one seed file a [Seeds] service runs.
type Seed struct {
	// Path is the file's path in the seed directory, which the tracker table
	// records it under.
	Path string
	// Filename is the file's name, which errors name it by.
	Filename string
	// Checksum is the hex SHA-256 of the file's bytes, which the tracker
	// table records.
	Checksum string
	// Text is the file's content.
	Text string
	// Env is the environment the run selected seeds for, which the tracker
	// table records.
	Env string
}

// ErrKeyConflict marks a seed file that hit an existing key and left nothing
// of itself applied. A [Seeds] service wraps it into the error Apply returns
// for that case, which is the failure an idempotent run tolerates.
var ErrKeyConflict = errors.New("the seed file hit an existing key")

// Seeds runs seed files for an engine whose seeds do not run as split
// statements in one transaction with savepoints. The caller decides what a
// failure means: an idempotent run records a file whose Apply error wraps
// [ErrKeyConflict] with Record.
type Seeds interface {
	// TableNames lists the tables of schema, the schema the connection reads
	// as current.
	TableNames(ctx context.Context, db Database, schema string) ([]string, error)
	// Apply runs one seed file and records it in the tracker table,
	// schema_seeds, which exists when Apply is called, or in the service's
	// own ledger ([SeedLedger]). The file and its record commit together or
	// not at all, so a file that fails leaves nothing of itself applied.
	Apply(ctx context.Context, db Database, seed Seed) error
	// Record records a seed without running it, as an idempotent run does
	// for a file whose rows already exist.
	Record(ctx context.Context, db Database, seed Seed) error
}

// SeedLedger is a [Seeds] service that keeps the record of applied seeds in a
// ledger whose layout is its own rather than in the schema_seeds table. Apply
// and Record create a missing ledger, so the caller creates no table.
type SeedLedger interface {
	// Ledger returns the checksum recorded for each applied seed path. A
	// ledger that does not exist yet holds no entry; a ledger the service
	// cannot read whole is an error.
	Ledger(ctx context.Context, db Database) (map[string]string, error)
}

// Runtime gives the data services a dialect's owner registered.
// *engine.Runtime implements it: the services belong to the runtime that
// loaded the owner, never to the process.
type Runtime interface {
	// DataServices returns the services of target, the zero Services when
	// its owner registered none, and an error for a target the runtime does
	// not know.
	DataServices(target string) (Services, error)
}

// Row is the result of a query for one row. Scan and Err expose a refusal when
// the selected connection cannot provide raw SQL queries. A database/sql Row
// implements it without copying its data or creating a driver.
type Row interface {
	Scan(...any) error
	Err() error
}
