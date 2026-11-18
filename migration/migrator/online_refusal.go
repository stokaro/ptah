package migrator

import (
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// OnlineAlterRefusedError reports a statement the server declined to apply the
// way it was asked to.
//
// A migration generated with the online form carries `ALGORITHM=INPLACE,
// LOCK=NONE`, and the server refuses the whole statement rather than falling
// back to a table copy. That refusal is the feature, so it reaches the
// operator as itself: the ordinary message is a syntax-shaped 1845 or 1846
// that reads as a bad statement, when what happened is that this change cannot
// be applied without blocking writes on this server.
type OnlineAlterRefusedError struct {
	Version int64
	Err     error
	// versionLabel names Version as its file name spells it, for an Atlas
	// file whose version is not a number; empty otherwise.
	versionLabel string
}

func (e *OnlineAlterRefusedError) Error() string {
	return fmt.Sprintf(
		"migration %d asked the server to apply a change without blocking writes and the server refused: %v "+
			"(the change cannot run online here; apply it in a window where a table copy is acceptable, "+
			"or split it into changes that can)",
		versionText{number: e.Version, label: e.versionLabel},
		e.Err,
	)
}

func (e *OnlineAlterRefusedError) Unwrap() error {
	return e.Err
}

// onlineAlterRefusal names a MySQL-family refusal of the ALGORITHM or LOCK
// clause, and returns err unchanged otherwise.
//
// 1845 is "not supported for this operation" and 1846 "not supported" with a
// reason, and the server answers one of the two for every form it cannot apply
// as asked -- measured on MySQL 8.4.6 and MariaDB 12.3.3 over eighteen ALTER
// TABLE forms. Neither number means anything else, so the mapping needs no
// second signal.
func onlineAlterRefusal(version versionText, err error) error {
	if err == nil {
		return nil
	}
	mysqlErr, ok := errors.AsType[*mysqldriver.MySQLError](err)
	if !ok {
		return err
	}
	switch mysqlErr.Number {
	case 1845, 1846:
		return &OnlineAlterRefusedError{Version: version.number, versionLabel: version.label, Err: err}
	default:
		return err
	}
}
