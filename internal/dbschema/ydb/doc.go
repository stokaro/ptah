// Package ydb connects to a YDB database, describes its row tables and
// applies DDL to it. dbschema opens a ydb:// or ydbs:// URL through [Open]
// and builds the schema reader and writer this package defines.
//
// YDB has no information_schema and no SQL that lists tables, so the reader
// asks YDB's scheme and table services through the SDK's gRPC connection and
// decodes their answers itself; see [Reader]. The writer runs each statement
// as a query of its own, and drops directories through the scheme service,
// because no SQL drops one; see [Writer].
//
// Queries and DDL run through database/sql over ydb-go-sdk's query service.
// The connector carries no SDK bind option: sqlutil.Rebind writes YQL's named
// parameters, and the connection names each positional argument and widens a
// Go int or uint to 64 bits before the SDK binds it; see
// [NewBindingConnector].
package ydb
