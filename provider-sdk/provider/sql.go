package provider

import (
	"context"
	"fmt"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// SQLAnalyzer owns SQL lexing and native safety facts. It must refuse syntax it
// cannot analyze completely. Common DDL retains the host's common assessment.
type SQLAnalyzer interface {
	AnalyzeSQL(context.Context, *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error)
}

// ValidateSQLAnalysis refuses missing sources, gaps, overlapping spans, missing
// token data, unknown dispositions, or false line numbers. It checks framing;
// only the provider can establish that its tokenization matches its database.
func ValidateSQLAnalysis(request *protocol.AnalyzeSQLRequest, result *protocol.SQLAnalysis) error {
	if request == nil || request.GetEngine() == "" || result == nil || !result.GetComplete() || len(result.GetFiles()) != len(request.GetSources()) {
		return fmt.Errorf("%w: incomplete SQL analysis", ErrProtocol)
	}
	for i, file := range result.GetFiles() {
		if file == nil {
			return fmt.Errorf("%w: missing SQL source analysis", ErrProtocol)
		}
		if err := validateSQLFile(request.GetSources()[i], file); err != nil {
			return err
		}
	}
	return nil
}

func validateSQLSafety(statement *protocol.SQLStatement) error {
	switch statement.GetSafety() {
	case protocol.SQLStatement_COMMON_DDL:
		if statement.GetReason() != "" {
			return fmt.Errorf("%w: common DDL carries a native safety reason", ErrProtocol)
		}
	case protocol.SQLStatement_BEHAVIORAL, protocol.SQLStatement_DESTRUCTIVE, protocol.SQLStatement_UNKNOWN:
		if strings.TrimSpace(statement.GetReason()) == "" {
			return fmt.Errorf("%w: SQL safety disposition requires a reason", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: unknown SQL safety disposition", ErrProtocol)
	}
	return nil
}

func validateSQLFile(source string, file *protocol.SQLFile) error {
	end := uint32(0)
	for _, statement := range file.GetStatements() {
		if statement == nil || statement.GetStart() < end || statement.GetEnd() <= statement.GetStart() || uint64(statement.GetEnd()) > uint64(len(source)) {
			return fmt.Errorf("%w: invalid SQL statement span", ErrProtocol)
		}
		start, stop := statement.GetStart(), statement.GetEnd()
		if strings.TrimSpace(source[end:start]) != "" || strings.TrimSpace(source[start:stop]) == "" || int64(statement.GetLine()) != 1+int64(strings.Count(source[:start], "\n")) {
			return fmt.Errorf("%w: SQL analysis omitted text or changed line numbers", ErrProtocol)
		}
		if len(statement.GetWords()) == 0 || len(statement.GetWords()) != len(statement.GetSourceWords()) || strings.TrimSpace(statement.GetCanonical()) == "" {
			return fmt.Errorf("%w: SQL analysis omitted tokens", ErrProtocol)
		}
		if err := validateSQLSafety(statement); err != nil {
			return err
		}
		end = stop
	}
	if strings.TrimSpace(source[end:]) != "" {
		return fmt.Errorf("%w: SQL analysis omitted trailing text", ErrProtocol)
	}
	return nil
}
