package ydb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	ydbschema "ptah.run/internal/dbschema/ydb"
)

// recordingConn stands in for a ydb-go-sdk connection: it accepts every value,
// as the SDK's does, and records the arguments a statement reached it with.
type recordingConn struct {
	got *[]driver.NamedValue
}

func (recordingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }
func (recordingConn) Close() error                        { return nil }
func (recordingConn) Begin() (driver.Tx, error)           { return nil, errors.New("not used") }
func (recordingConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New("not used")
}
func (recordingConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, errors.New("not used")
}
func (recordingConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("not used")
}
func (r recordingConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	*r.got = args
	return driver.RowsAffected(1), nil
}
func (recordingConn) Ping(context.Context) error               { return nil }
func (recordingConn) CheckNamedValue(*driver.NamedValue) error { return nil }
func (recordingConn) ResetSession(context.Context) error       { return nil }
func (recordingConn) IsValid() bool                            { return true }
func (recordingConn) Driver() driver.Driver                    { return nil }

// recordingConnector hands out recordingConn.
type recordingConnector struct {
	got    *[]driver.NamedValue
	closed *int
}

func (r recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return recordingConn{got: r.got}, nil
}
func (recordingConnector) Driver() driver.Driver { return nil }
func (r recordingConnector) Close() error {
	*r.closed++
	return nil
}

// myID is a named integer, which the SDK reads by its kind.
type myID int

// A positional argument reaches the SDK named after its position, the name
// sqlutil.Rebind writes for the placeholder, and a Go int or uint reaches it
// widened to 64 bits, which the SDK binds as Int64 and Uint64 rather than as
// a truncated Int32 and Uint32.
func TestBindingConnector_NamesAndWidensArguments(t *testing.T) {
	c := qt.New(t)
	var got []driver.NamedValue
	closed := 0
	db := sql.OpenDB(ydbschema.NewBindingConnector(recordingConnector{got: &got, closed: &closed}, nil))
	c.Cleanup(func() { _ = db.Close() })

	_, err := db.ExecContext(context.Background(), "UPSERT",
		5000000000,
		"text",
		sql.Named("own", 3),
		uint(7),
		(*int)(nil),
		new(9),
		[]int{1, 2},
		sql.Null[int]{V: 4, Valid: true},
		myID(11),
		int32(12),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []driver.NamedValue{
		{Name: "p1", Ordinal: 1, Value: int64(5000000000)},
		{Name: "p2", Ordinal: 2, Value: "text"},
		{Name: "own", Ordinal: 3, Value: int64(3)},
		{Name: "p4", Ordinal: 4, Value: uint64(7)},
		{Name: "p5", Ordinal: 5, Value: (*int64)(nil)},
		{Name: "p6", Ordinal: 6, Value: new(int64(9))},
		{Name: "p7", Ordinal: 7, Value: []int64{1, 2}},
		{Name: "p8", Ordinal: 8, Value: sql.Null[int64]{V: 4, Valid: true}},
		{Name: "p9", Ordinal: 9, Value: int64(11)},
		{Name: "p10", Ordinal: 10, Value: int32(12)},
	})
}

// Closing the pool closes the SDK connector and then calls the hook that
// closes the SDK driver.
func TestBindingConnector_CloseReachesBoth(t *testing.T) {
	c := qt.New(t)
	var got []driver.NamedValue
	closed, hooked := 0, 0
	db := sql.OpenDB(ydbschema.NewBindingConnector(recordingConnector{got: &got, closed: &closed},
		func() error {
			hooked++
			return nil
		}))

	err := db.Close()

	c.Assert(err, qt.IsNil)
	c.Assert(closed, qt.Equals, 1)
	c.Assert(hooked, qt.Equals, 1)
}

// bareConn is a connection that offers only driver.Conn.
type bareConn struct{}

func (bareConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }
func (bareConn) Close() error                        { return nil }
func (bareConn) Begin() (driver.Tx, error)           { return nil, errors.New("not used") }

type bareConnector struct{}

func (bareConnector) Connect(context.Context) (driver.Conn, error) { return bareConn{}, nil }
func (bareConnector) Driver() driver.Driver                        { return nil }

// A connection that does not offer what database/sql uses of the SDK's is
// refused, rather than letting database/sql fall back to another path.
func TestBindingConnector_FailurePath_RefusesAnUnknownConnection(t *testing.T) {
	c := qt.New(t)
	db := sql.OpenDB(ydbschema.NewBindingConnector(bareConnector{}, nil))
	c.Cleanup(func() { _ = db.Close() })

	err := db.PingContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `the YDB driver's connection ydb_test.bareConn no longer offers what Ptah binds through`)
}
