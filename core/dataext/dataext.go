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
// The package holds contracts only. It links no engine, so a provider in
// another module implements it without linking Ptah's engines.
package dataext

import (
	"context"
	"database/sql"

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
	// Rows types managed rows before they are compared.
	Rows Rows
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
	// live rows with every value in the form the engine compares it in.
	// liveTable is the table as the database holds it, nil when it does not
	// exist yet; declaredTypes is each column's declared type; caps are the
	// target's capabilities, which decide the type a declared column lands
	// on. The returned rows are new; desired and live are not modified.
	TypeRows(
		caps capability.Capabilities,
		liveTable *catalog.Table,
		declaredTypes map[string]string,
		desired, live []map[string]any,
	) (TypedRows, error)
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

// Seeds runs seed files for an engine whose seeds do not run as split
// statements in one transaction with savepoints. The caller decides what a
// failure means: an idempotent run records a file that hit a duplicate key
// with Record.
type Seeds interface {
	// TableNames lists the tables of schema, the schema the connection reads
	// as current.
	TableNames(ctx context.Context, db Database, schema string) ([]string, error)
	// Apply runs one seed file and records it in the tracker table,
	// schema_seeds, which exists when Apply is called. The file and its
	// record commit together or not at all, so a file that fails leaves
	// nothing of itself applied.
	Apply(ctx context.Context, db Database, seed Seed) error
	// Record records a seed without running it, as an idempotent run does
	// for a file whose rows already exist.
	Record(ctx context.Context, db Database, seed Seed) error
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
