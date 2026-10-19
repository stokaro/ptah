package descriptor

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Reach states the constructs of the engine's grammar that reach outside the
// database a statement is sent to. internal/sqlreach refuses each one in a
// statement it scans, naming the construct the entry names.
//
// The call families below hold names that are ordinary calls on another
// engine: url(...) is a remote read on ClickHouse and a user function anywhere
// else, so a family is refused on the engine that states it alone. Everywhere
// holds constructs whose spelling no engine gives another meaning, such as
// xp_cmdshell, and those are refused on every engine.
//
// The zero value states none.
type Reach struct {
	// TableFunctions are table functions that read something other than the
	// server's own storage: another server, object storage, or a file or a
	// program on the host.
	TableFunctions ReachCalls `json:"table_functions,omitzero"`
	// NetworkPackages are built-in packages whose procedures send a request
	// from the server. A name matches where it qualifies a call, as in
	// UTL_HTTP.REQUEST(...), and where it is called itself.
	NetworkPackages ReachCalls `json:"network_packages,omitzero"`
	// ControlPackages are built-in packages whose procedures change server
	// state that outlives the statement, matched as NetworkPackages are.
	ControlPackages ReachCalls `json:"control_packages,omitzero"`
	// ControlFunctions are functions that act on the server rather than read
	// from it, such as one that terminates a backend.
	ControlFunctions ReachCalls `json:"control_functions,omitzero"`
	// ReplicationFunctions are functions that change the server's
	// replication state, such as one that creates a replication slot.
	ReplicationFunctions ReachCalls `json:"replication_functions,omitzero"`
	// Everywhere are the constructs of the engine's grammar whose spelling
	// means nothing ordinary on any engine, such as SQLite's ATTACH or SQL
	// Server's xp_cmdshell. A scan refuses each one in a statement of every
	// engine, whichever engine states it, so a statement that names one is
	// refused whatever dialect it was written for. The scan asks the
	// constructs of all engines in registry order, each engine's in the order
	// it states them, and names the first one a statement matches.
	Everywhere []ReachConstruct `json:"everywhere,omitempty"`
}

// ReachConstruct is one construct of [Reach.Everywhere]: the words a refusal
// prints and the shape a statement has when it uses the construct. A statement
// matches when it has every part the construct states; a part that lists
// several names matches any one of them. Keywords and names are written in
// upper case and match a token in any letter case.
//
// A keyword is a bare identifier token. A name in a call position may also be
// quoted, with double quotes, backticks or brackets, since quoting a name does
// not change what it calls.
type ReachConstruct struct {
	// Construct names the construct in a refusal, such as "ATTACH".
	Construct string `json:"construct"`
	// Reach says what the construct can touch, as a refusal prints it after
	// the construct.
	Reach string `json:"reach"`
	// StartsWith is a keyword sequence a statement begins with: at the start
	// of the text, after a semicolon, or, inside a routine body, after BEGIN,
	// THEN, ELSE, LOOP or DO.
	StartsWith []string `json:"starts_with,omitempty"`
	// Keyword is a keyword the statement holds anywhere, such as the INTO of
	// VACUUM INTO.
	Keyword string `json:"keyword,omitempty"`
	// StringAfter are keywords one of which a string literal directly
	// follows, as a file path follows the FROM or TO of COPY.
	StringAfter []string `json:"string_after,omitempty"`
	// Calls are names called as a function: a name directly followed by an
	// opening parenthesis, except directly after a keyword that names an
	// object in DDL, such as TABLE, INDEX, FUNCTION or ON.
	Calls []string `json:"calls,omitempty"`
	// CallPrefixes match a call, as Calls does, to a name that begins with
	// one of them, as every pg_ls_ function does.
	CallPrefixes []string `json:"call_prefixes,omitempty"`
	// Procedures are names called as Calls are, or written directly after
	// EXEC, EXECUTE or CALL.
	Procedures []string `json:"procedures,omitempty"`
	// Pragmas are settings named after PRAGMA: within the three tokens that
	// follow it, with only names and dots in front of the setting, as in
	// PRAGMA temp_store_directory and PRAGMA main.temp_store_directory.
	Pragmas []string `json:"pragmas,omitempty"`
	// WordsThenString is a keyword sequence directly followed by a string
	// literal, as INTO OUTFILE '/tmp/x' is.
	WordsThenString []string `json:"words_then_string,omitempty"`
	// Engines are storage engines named directly after ENGINE, or after
	// ENGINE =, as ENGINE = FEDERATED and ENGINE = MySQL(...) are.
	Engines []string `json:"engines,omitempty"`
	// PathOption is a table option of two keywords followed by a string
	// literal, with an optional = between, as DATA DIRECTORY = '/x' is.
	PathOption []string `json:"path_option,omitempty"`
}

// ReachCalls is one family of calls in [Reach] and the words a refusal of one
// prints. The zero value states no calls.
type ReachCalls struct {
	// Construct names the family in a refusal, such as "ClickHouse remote
	// table function".
	Construct string `json:"construct,omitempty"`
	// Reach says what a call can touch, as a refusal prints it after the
	// construct, such as "reads over the network or from the host filesystem
	// inside a query".
	Reach string `json:"reach,omitempty"`
	// Names are the called names, in upper case. A call matches whatever
	// case or quoting it is written in.
	Names []string `json:"names,omitempty"`
}

// validateReach checks the fields declared in this file.
func (d Dialect) validateReach() error {
	for _, family := range []struct {
		name  string
		calls ReachCalls
	}{
		{"table functions", d.Reach.TableFunctions},
		{"network packages", d.Reach.NetworkPackages},
		{"control packages", d.Reach.ControlPackages},
		{"control functions", d.Reach.ControlFunctions},
		{"replication functions", d.Reach.ReplicationFunctions},
	} {
		calls := family.calls
		worded := calls.Construct != "" && calls.Reach != ""
		unworded := calls.Construct == "" && calls.Reach == ""
		if len(calls.Names) > 0 && !worded || len(calls.Names) == 0 && !unworded {
			return fmt.Errorf("%w: dialect %q: reach %s states names without a construct and its reach, or the words without names",
				ErrInvalidDescriptor, d.Name, family.name)
		}
		for _, name := range calls.Names {
			if name == "" || strings.ToUpper(name) != name {
				return fmt.Errorf("%w: dialect %q: reach %s name %q is empty or not in upper case",
					ErrInvalidDescriptor, d.Name, family.name, name)
			}
		}
	}
	for _, construct := range d.Reach.Everywhere {
		if err := construct.validate(); err != nil {
			return fmt.Errorf("%w: dialect %q: reach construct %q %w", ErrInvalidDescriptor, d.Name, construct.Construct, err)
		}
	}
	return nil
}

// validate checks one construct: its words, at least one part, and the
// spelling of every keyword and name.
func (c ReachConstruct) validate() error {
	if c.Construct == "" || c.Reach == "" {
		return errors.New("states no construct or no reach")
	}
	words := slices.Concat(c.StartsWith, c.StringAfter, c.Calls, c.CallPrefixes, c.Procedures,
		c.Pragmas, c.WordsThenString, c.Engines, c.PathOption)
	if c.Keyword != "" {
		words = append(words, c.Keyword)
	}
	if len(words) == 0 {
		return errors.New("states no part a statement could match")
	}
	if len(c.PathOption) != 0 && len(c.PathOption) != 2 {
		return fmt.Errorf("states a path option of %d keywords rather than two", len(c.PathOption))
	}
	for _, word := range words {
		if word == "" || strings.ToUpper(word) != word {
			return fmt.Errorf("states %q, which is empty or not in upper case", word)
		}
	}
	return nil
}
