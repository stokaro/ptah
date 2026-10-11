// Package ydbsql is the YDB owner's side of the SQL frontend. YQL is not
// written in the shared grammar, so the owner reads every YQL document of a
// load itself, in one call, and claims what a YQL document declares, less the
// objects its `ptah:not-described` header leaves unmanaged. The built-in
// runtime registers [SQL] on the YDB target.
package ydbsql

import (
	"context"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsource"
)

// SQL is the YDB owner's SQL extension: the grammar that reads YQL, the
// header kinds that leave a YDB object unmanaged, and the claim a YQL
// document makes.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:    ydbschema.Owner,
		Targets:  []string{platform.YDB},
		Grammar:  grammar{},
		Header:   ydbsource.LimitKinds(),
		Coverage: claim,
	}
}

// grammar reads a load of YQL documents.
type grammar struct{}

// ReadSQL reads the documents of one load as one script. The YQL conversion
// still lives in the shared SQL reader, so it is reached through
// [sqlschema.ReadScript]; moving it here is the owner's remaining work under
// stokaro/ptah#4140.
func (grammar) ReadSQL(ctx context.Context, request sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
	return sqlschema.ReadScript(ctx, request)
}

// claim enrolls the namespaces a YQL document can declare, less the objects
// limits leave unmanaged.
func claim(limits []coverage.Object) (schemaext.Coverage, error) {
	var recorded ydbsource.Limits
	for _, limit := range limits {
		recorded.Add(string(limit.Kind), limit.Name)
	}
	return ydbsource.Coverage(recorded)
}
