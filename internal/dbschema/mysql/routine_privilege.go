package mysql

import (
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// errBinlogCreateRoutineNeedSuper is MySQL's ER_BINLOG_CREATE_ROUTINE_NEED_SUPER.
//
// The server raises it when binary logging is enabled, the connected user has
// no SUPER privilege, and log_bin_trust_function_creators is off. It is a
// deployment gate, not a statement defect: no CREATE FUNCTION spelling gets
// past it.
const errBinlogCreateRoutineNeedSuper = 1419

// describeRoutinePrivilegeRefusal replaces MySQL's own wording for
// ER_BINLOG_CREATE_ROUTINE_NEED_SUPER with a sentence that says what this
// deployment can do, and returns err unchanged for anything else.
//
// Measured on MySQL 26.7.0 with the pinned image's defaults (log_bin = 1,
// log_bin_trust_function_creators = 0) and a user holding ALL PRIVILEGES on its
// own database but only USAGE globally, the two binary-logging gates fire in
// sequence rather than as alternatives:
//
//	CREATE FUNCTION f() RETURNS int RETURN 1                -> Error 1418
//	CREATE FUNCTION f() RETURNS int DETERMINISTIC RETURN 1  -> Error 1419
//	CREATE FUNCTION f() RETURNS int READS SQL DATA RETURN 1 -> Error 1419
//
// The renderer always emits a characteristic, so 1418 is answered before the
// server ever sees the statement; 1419 is the one left, and it is not
// answerable by rendering. The same user's CREATE TABLE succeeded throughout,
// so the account is not simply unprivileged -- routine creation specifically is
// gated.
//
// The server's own text points at log_bin_trust_function_creators and calls it
// "the less safe" option in the same breath. Repeating that as advice would be
// telling an operator to disable a safety check on their server -- and it
// disables both gates, so a function carrying no characteristic at all is
// accepted too, which is measurably weaker than what it replaces. This message
// names it only to connect what Ptah says to what the server said, and does not
// suggest it.
// It returns nil when err is any other failure, so callers keep their own
// generic wrapping for everything this function does not recognize.
func describeRoutinePrivilegeRefusal(err error, sqlExpr string) error {
	var mysqlErr *mysqldriver.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != errBinlogCreateRoutineNeedSuper {
		return nil
	}
	return fmt.Errorf(
		"this MySQL deployment does not permit Ptah to manage stored functions: "+
			"binary logging is enabled and the connected user lacks the SUPER privilege, "+
			"so the server refuses CREATE FUNCTION whatever Ptah renders. "+
			"Grant SUPER to the migrating user, run the migration as a user that holds it, "+
			"or remove the function declaration for this target. "+
			"The server also names log_bin_trust_function_creators, which MySQL itself calls "+
			"the less safe option; it switches off the characteristic check as well, and Ptah does not suggest it: %w\nSQL: %s",
		err, sqlExpr)
}
