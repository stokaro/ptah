package dbschema

import (
	"fmt"
	"slices"
	"sync"

	"ptah.run/internal/connector"
	"ptah.run/internal/connector/clickhouseconnect"
	"ptah.run/internal/connector/mssqlconnect"
	"ptah.run/internal/connector/mysqlconnect"
	"ptah.run/internal/connector/oracleconnect"
	"ptah.run/internal/connector/pgconnect"
	"ptah.run/internal/connector/sqliteconnect"
	"ptah.run/internal/connector/ydbconnect"
)

// services is the registry a connection chooses its engine's connector from:
// one per built-in dialect Ptah connects to. Each engine names the dialects it
// serves, so this list names none.
var services = sync.OnceValue(func() *connector.Registry {
	registry, err := connector.NewRegistry(slices.Concat(
		pgconnect.Registrations(),
		mysqlconnect.Registrations(),
		clickhouseconnect.Registrations(),
		sqliteconnect.Registrations(),
		mssqlconnect.Registrations(),
		oracleconnect.Registrations(),
		ydbconnect.Registrations(),
	)...)
	if err != nil {
		// The registrations are compiled into the binary and their own tests
		// build them; a conflict is a defect in this module, not an input.
		panic(fmt.Sprintf("dbschema: the built-in connectors are invalid: %v", err))
	}
	return registry
})
