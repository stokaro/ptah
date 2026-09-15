//go:build integration

package generator_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/generator"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// A checkpoint carrying a reference table has to hand a fresh database the
// bytes the history wrote, and a binary column is where a character-string
// literal stops being able to do that (stokaro/ptah#3297). Each payload breaks
// a text literal a different way:
//
//   - alpha holds 0xff, which no UTF-8 statement can carry;
//   - beta holds `\\B`, which PostgreSQL's bytea input reads as an escape and
//     stores as two bytes, with no error;
//   - gamma is plain ASCII holding a quote, which SQL Server still refuses to
//     convert from varchar to varbinary;
//   - delta is empty, and the column is NOT NULL, so it must not become NULL.
//
// The amount column is the other side of the rule. Two of these drivers hand a
// DECIMAL back as bytes too, and a decimal rendered as a binary literal would
// be converted to a different number rather than refused.
//
// The history writes the payloads through each engine's own hex decoding
// function rather than the literal the renderer emits, so the fixture does not
// agree with the renderer by construction. Both databases are read back by
// scanning into []byte, not through dbschema.ReadTableRows, so the comparison
// does not depend on the reader the fix changes.
const binaryCheckpointTable = "ptah_3297_payloads"

type binaryCheckpointRow struct {
	Code    string
	Payload string
	Amount  string
}

var wantBinaryCheckpointRows = []binaryCheckpointRow{
	{Code: "alpha", Payload: "5cff41", Amount: "12.3456"},
	{Code: "beta", Payload: "5c5c42", Amount: "-0.0001"},
	{Code: "delta", Payload: "", Amount: "0.0000"},
	{Code: "gamma", Payload: "275c5c", Amount: "100.0000"},
}

func TestCheckpointBinaryColumnReplaysBytewise_PostgreSQLLive(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	shadowURL, shadowDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_3297_shadow")
	defer dropGeneratorTestPostgres(c, admin, shadowDatabase)
	freshURL, freshDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_3297_fresh")
	defer dropGeneratorTestPostgres(c, admin, freshDatabase)

	fromHistory, fromCheckpoint := replayBinaryCheckpoint(c, shadowURL, freshURL,
		"CREATE TABLE ptah_3297_payloads (code VARCHAR(16) PRIMARY KEY, payload BYTEA NOT NULL, amount NUMERIC(12,4) NOT NULL);\n"+
			"INSERT INTO ptah_3297_payloads (code, payload, amount) VALUES "+
			"('alpha', decode('5cff41', 'hex'), 12.3456), "+
			"('beta', decode('5c5c42', 'hex'), -0.0001), "+
			"('gamma', decode('275c5c', 'hex'), 100), "+
			"('delta', decode('', 'hex'), 0);\n",
	)

	c.Assert(fromHistory, qt.DeepEquals, wantBinaryCheckpointRows)
	c.Assert(fromCheckpoint, qt.DeepEquals, wantBinaryCheckpointRows)
}

func TestCheckpointBinaryColumnReplaysBytewise_MySQLLive(t *testing.T) {
	c := qt.New(t)
	shadowURL := mySQLScratchDatabaseURL(c, "ptah_3297_shadow")
	freshURL := mySQLScratchDatabaseURL(c, "ptah_3297_fresh")

	fromHistory, fromCheckpoint := replayBinaryCheckpoint(c, shadowURL, freshURL,
		"CREATE TABLE ptah_3297_payloads (code VARCHAR(16) NOT NULL PRIMARY KEY, payload VARBINARY(16) NOT NULL, amount DECIMAL(12,4) NOT NULL);\n"+
			"INSERT INTO ptah_3297_payloads (code, payload, amount) VALUES "+
			"('alpha', UNHEX('5CFF41'), 12.3456), "+
			"('beta', UNHEX('5C5C42'), -0.0001), "+
			"('gamma', UNHEX('275C5C'), 100), "+
			"('delta', UNHEX(''), 0);\n",
	)

	c.Assert(fromHistory, qt.DeepEquals, wantBinaryCheckpointRows)
	c.Assert(fromCheckpoint, qt.DeepEquals, wantBinaryCheckpointRows)
}

func TestCheckpointBinaryColumnReplaysBytewise_SQLServerLive(t *testing.T) {
	c := qt.New(t)
	shadowURL := sqlServerScratchDatabaseURL(c, "ptah_3297_shadow")
	freshURL := sqlServerScratchDatabaseURL(c, "ptah_3297_fresh")

	fromHistory, fromCheckpoint := replayBinaryCheckpoint(c, shadowURL, freshURL,
		"CREATE TABLE ptah_3297_payloads (code NVARCHAR(16) NOT NULL PRIMARY KEY, payload VARBINARY(16) NOT NULL, amount DECIMAL(12,4) NOT NULL);\n"+
			"INSERT INTO ptah_3297_payloads (code, payload, amount) VALUES "+
			"(N'alpha', CONVERT(VARBINARY(16), '5CFF41', 2), 12.3456), "+
			"(N'beta', CONVERT(VARBINARY(16), '5C5C42', 2), -0.0001), "+
			"(N'gamma', CONVERT(VARBINARY(16), '275C5C', 2), 100), "+
			"(N'delta', CONVERT(VARBINARY(16), '', 2), 0);\n",
	)

	c.Assert(fromHistory, qt.DeepEquals, wantBinaryCheckpointRows)
	c.Assert(fromCheckpoint, qt.DeepEquals, wantBinaryCheckpointRows)
}

// replayBinaryCheckpoint writes a one-migration history, squashes it into a
// checkpoint that carries the reference table's rows, applies the checkpoint
// alone to a fresh database, and reads both databases back.
//
// The shadow database is the history side: the generator replays the whole
// directory into it and only removes the migration bookkeeping afterward.
func replayBinaryCheckpoint(c *qt.C, shadowURL, freshURL, historyUp string) (fromHistory, fromCheckpoint []binaryCheckpointRow) {
	c.Helper()
	ctx := c.Context()

	history := c.TempDir()
	writeBinaryCheckpointFile(c, filepath.Join(history, "0000000001_payloads.up.sql"), historyUp)
	writeBinaryCheckpointFile(c, filepath.Join(history, "0000000001_payloads.down.sql"), "DROP TABLE "+binaryCheckpointTable+";\n")

	upSQL, downSQL, err := generator.GenerateCheckpointFromShadow(ctx, generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: shadowURL,
		MigrationsDir:     history,
		DataTables:        []string{binaryCheckpointTable},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(upSQL, qt.Contains, generator.BootstrapDataMarker+" "+binaryCheckpointTable+" rows=4")

	checkpoint := c.TempDir()
	writeBinaryCheckpointFile(c, filepath.Join(checkpoint, migrationfile.CheckpointFileName(2, "snapshot", "up")), upSQL)
	writeBinaryCheckpointFile(c, filepath.Join(checkpoint, migrationfile.CheckpointFileName(2, "snapshot", "down")), downSQL)

	fresh, err := dbschema.ConnectToDatabase(ctx, freshURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(fresh)
	mig, err := migrator.NewFSMigrator(fresh, os.DirFS(checkpoint))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil, qt.Commentf("checkpoint:\n%s", upSQL))

	shadow, err := dbschema.ConnectToDatabase(ctx, shadowURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(shadow)

	return readBinaryCheckpointRows(c, shadow), readBinaryCheckpointRows(c, fresh)
}

func readBinaryCheckpointRows(c *qt.C, conn *dbschema.DatabaseConnection) []binaryCheckpointRow {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		"SELECT code, payload, amount FROM "+binaryCheckpointTable+" ORDER BY code")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var got []binaryCheckpointRow
	for rows.Next() {
		var (
			code    string
			payload []byte
			amount  string
		)
		c.Assert(rows.Scan(&code, &payload, &amount), qt.IsNil)
		got = append(got, binaryCheckpointRow{Code: code, Payload: hex.EncodeToString(payload), Amount: amount})
	}
	c.Assert(rows.Err(), qt.IsNil)
	return got
}

// mySQLScratchDatabaseURL creates a database of its own on the MySQL server and
// returns its address. Checkpoint generation empties its shadow database, so
// it must never be pointed at a database other tests use.
func mySQLScratchDatabaseURL(c *qt.C, prefix string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.MySQLAdmin)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })

	database := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	quoted := sqlident.Quote(platform.MySQL, database)
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+quoted)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoted)
		c.Check(dropErr, qt.IsNil)
	})

	// CI writes the MySQL address in the driver's tcp() form, which url.Parse
	// cannot read, and a developer usually writes a URL.
	if strings.Contains(adminURL, "@tcp(") {
		scheme, dsn, found := strings.Cut(adminURL, "://")
		c.Assert(found, qt.IsTrue)
		config, parseErr := mysqldriver.ParseDSN(dsn)
		c.Assert(parseErr, qt.IsNil)
		config.DBName = database
		return scheme + "://" + config.FormatDSN()
	}
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.RawPath = ""
	return parsed.String()
}

// sqlServerScratchDatabaseURL creates a database of its own on the SQL Server
// instance and returns its address, for the reason mySQLScratchDatabaseURL
// gives.
func sqlServerScratchDatabaseURL(c *qt.C, prefix string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.SQLServer)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })

	database := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	quoted := sqlident.Quote(platform.SQLServer, database)
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+quoted)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx,
			"ALTER DATABASE "+quoted+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quoted)
		c.Check(dropErr, qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func writeBinaryCheckpointFile(c *qt.C, path, contents string) {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
}
