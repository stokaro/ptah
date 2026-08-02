// Package onlineddl routes ALTER TABLE migration statements through external
// online-DDL tools such as gh-ost and pt-online-schema-change.
package onlineddl

import "go.5x5.cz/ptah/config/projectconfig"

// Canonical tool names accepted in configuration and directives.
const (
	// ToolGhost routes ALTERs through GitHub's gh-ost.
	ToolGhost = projectconfig.OnlineDDLToolGhost
	// ToolPTOSC routes ALTERs through Percona's pt-online-schema-change.
	ToolPTOSC = projectconfig.OnlineDDLToolPTOSC
)

// Fallback policies accepted in configuration and directives.
const (
	// FallbackError aborts instead of degrading to a plain ALTER TABLE.
	FallbackError = projectconfig.OnlineDDLFallbackError
	// FallbackPlain lets the migrator execute the plain ALTER TABLE.
	FallbackPlain = projectconfig.OnlineDDLFallbackPlain
)

// Config aliases the canonical project configuration IR. Keeping one type
// prevents migration execution policy from drifting from the project settings
// loaded at command startup.
type Config = projectconfig.OnlineDDLConfig
