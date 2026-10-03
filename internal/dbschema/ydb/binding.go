package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"

	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

// connector hands database/sql the SDK's connections with Ptah's argument
// binding in front of them, and runs onClose after the SDK connector closes.
// sdk is the driver the connections belong to, which [DriverOf] hands back;
// it is nil for a connector [NewBindingConnector] built.
type connector struct {
	inner   driver.Connector
	sdk     *ydbsdk.Driver
	onClose func() error
}

// NewBindingConnector returns inner with Ptah's argument binding in front of
// every connection it makes; see [conn.CheckNamedValue]. Closing the result
// closes inner when inner is an io.Closer, and then calls onClose, which may
// be nil. A connection inner makes must offer what database/sql uses of a
// ydb-go-sdk connection, or Connect refuses it.
func NewBindingConnector(inner driver.Connector, onClose func() error) driver.Connector {
	return &connector{inner: inner, onClose: onClose}
}

// sdkConn is what database/sql uses of a ydb-go-sdk connection. The SDK's
// connection type is internal, so the set is checked when a connection is
// made: a release that drops one of them fails there, loudly, instead of
// database/sql quietly taking a slower or different path.
type sdkConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.QueryerContext
	driver.ExecerContext
	driver.Pinger
	driver.NamedValueChecker
	driver.SessionResetter
	driver.Validator
}

// Connect opens one SDK connection and wraps it.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	opened, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	sdk, ok := opened.(sdkConn)
	if !ok {
		_ = opened.Close()
		return nil, fmt.Errorf("the YDB driver's connection %T no longer offers what Ptah binds through", opened)
	}
	return conn{sdkConn: sdk, driver: c.sdk}, nil
}

// Driver returns the SDK's driver.
func (c *connector) Driver() driver.Driver {
	return c.inner.Driver()
}

// Close closes the SDK connector and then runs onClose, which closes the SDK
// driver that database/sql's Close does not reach on its own.
func (c *connector) Close() error {
	var errs []error
	if closer, ok := c.inner.(io.Closer); ok {
		errs = append(errs, closer.Close())
	}
	if c.onClose != nil {
		errs = append(errs, c.onClose())
	}
	return errors.Join(errs...)
}

// conn is an SDK connection whose arguments are named and widened before the
// SDK binds them.
type conn struct {
	sdkConn
	driver *ydbsdk.Driver
}

// DriverOf returns the SDK driver behind session, a connection from a pool
// [Open] made. The scheme, table and coordination services are reached
// through the driver, not through SQL, and a caller that holds only a pooled
// connection reaches them this way. A connection from any other pool is an
// error.
func DriverOf(session *sql.Conn) (*ydbsdk.Driver, error) {
	var sdk *ydbsdk.Driver
	err := session.Raw(func(raw any) error {
		bound, ok := raw.(conn)
		if !ok || bound.driver == nil {
			return fmt.Errorf("%T is not a connection to a YDB database Ptah opened", raw)
		}
		sdk = bound.driver
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sdk, nil
}

// CheckNamedValue names a positional argument after its position, and widens a
// Go int or uint to 64 bits, before the SDK sees it.
//
// YQL has no positional parameter, and sqlutil.Rebind writes `$p1`, `$p2`, ...
// for Ptah's `?` placeholders, so the first positional argument is bound to
// `$p1`. Without a name the SDK answers `Unknown name: $p1`.
//
// The SDK binds a Go int as Int32 and truncates it: measured on YDB 26.2.1.14
// with ydb-go-sdk v3.153.2, 5000000000 bound to an Int64 key was stored as
// 705032704 and reported success. Widened to Int64, a value bound to an Int32
// or a smaller column is refused instead (`Failed to convert 'i32': Int64 to
// Optional<Int32>`), and one bound to an Int64 column is stored whole.
func (c conn) CheckNamedValue(value *driver.NamedValue) error {
	if value.Name == "" {
		value.Name = "p" + strconv.Itoa(value.Ordinal)
	}
	value.Value = widenInteger(value.Value)
	return c.sdkConn.CheckNamedValue(value)
}

// widenInteger returns a Go int or uint, or a value built on one, as its 64-bit
// counterpart, and any other value unchanged. A pointer keeps its nil, which
// the SDK binds as a typed NULL. A value with a Value method keeps it, since
// the method decides what is bound.
func widenInteger(value any) any {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case uint:
		return uint64(typed)
	case *int:
		if typed == nil {
			return (*int64)(nil)
		}
		return new(int64(*typed))
	case *uint:
		if typed == nil {
			return (*uint64)(nil)
		}
		return new(uint64(*typed))
	case []int:
		widened := make([]int64, len(typed))
		for i, item := range typed {
			widened[i] = int64(item)
		}
		return widened
	case []uint:
		widened := make([]uint64, len(typed))
		for i, item := range typed {
			widened[i] = uint64(item)
		}
		return widened
	case sql.Null[int]:
		return sql.Null[int64]{V: int64(typed.V), Valid: typed.Valid}
	case sql.Null[uint]:
		return sql.Null[uint64]{V: uint64(typed.V), Valid: typed.Valid}
	case driver.Valuer:
		return value
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int:
		return reflected.Int()
	case reflect.Uint:
		return reflected.Uint()
	default:
		return value
	}
}
