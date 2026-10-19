package descriptor

import (
	"fmt"
	"strings"
)

// Reach states the calls of the engine's grammar that reach outside the
// database a statement is sent to, where the same name is an ordinary call on
// another engine: url(...) is a remote read on ClickHouse and a user function
// anywhere else. internal/sqlreach refuses each one in a statement it scans,
// naming the construct the entry names. A construct no engine gives another
// meaning, such as xp_cmdshell, is that package's own and is not stated here.
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
	return nil
}
