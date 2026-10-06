package ydbreplication

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// Refusal says why a replication or a transfer cannot be written or changed
// on a target: Key is the capability it needs and the target lacks, or empty
// when YDB refuses it whatever the target, and Reason then says why.
type Refusal struct {
	// Subject names what is refused.
	Subject string
	// Key is the capability the declaration needs, empty for a refusal YDB
	// makes on every line.
	Key capability.Capability
	// Reason is why YDB refuses it, for a refusal without a key.
	Reason string
}

// CheckReplication reports why the replication name, declared as spec, cannot
// be created on a target holding caps, or nil when it can.
//
// It holds what a declaration's parse cannot know -- the target, through the
// async_replication key and the key of a secret path -- and holds a spec built
// by hand to the parse's rules, because nothing else checks it.
func CheckReplication(name string, spec ast.AsyncReplicationSpec, caps capability.Capabilities) *Refusal {
	subject := "async replication " + name
	if !caps.Has(capability.AsyncReplication) {
		return &Refusal{Subject: subject, Key: capability.AsyncReplication}
	}
	if strings.TrimSpace(name) == "" {
		return &Refusal{Subject: "an async replication", Reason: "a replication needs a name"}
	}
	if _, err := ParseReplication(replicationValues(spec)); err != nil {
		return &Refusal{Subject: subject, Reason: err.Error()}
	}
	if err := ValidateReplicationItems(spec.Items); err != nil {
		return &Refusal{Subject: subject, Reason: err.Error()}
	}
	if UsesSecretPath(spec.Connection) && !caps.Has(capability.ReplicationSecretPaths) {
		return &Refusal{Subject: subject + " names a secret by its path", Key: capability.ReplicationSecretPaths}
	}
	return nil
}

// CheckTransfer reports why the transfer name, declared as spec, cannot be
// created on a target holding caps, or nil when it can.
func CheckTransfer(name string, spec ast.TransferSpec, caps capability.Capabilities) *Refusal {
	subject := "transfer " + name
	if !caps.Has(capability.Transfers) {
		return &Refusal{Subject: subject, Key: capability.Transfers}
	}
	if strings.TrimSpace(name) == "" {
		return &Refusal{Subject: "a transfer", Reason: "a transfer needs a name"}
	}
	if _, err := ParseTransfer(transferValues(spec)); err != nil {
		return &Refusal{Subject: subject, Reason: err.Error()}
	}
	if UsesSecretPath(spec.Connection) && !caps.Has(capability.ReplicationSecretPaths) {
		return &Refusal{Subject: subject + " names a secret by its path", Key: capability.ReplicationSecretPaths}
	}
	return nil
}

// ReplicationChangeRefusal reports why the replication name cannot move from
// current, in state, to desired in place, or nil when it can.
//
// No statement changes a replication's items, consistency level or commit
// interval, and dropping it to create it again drops or freezes its replica
// tables, so that change is refused rather than planned. A connection or a
// credential changes only while the replication is paused, and a replication
// that was failed over replicates nothing more; Ptah never pauses, resumes or
// fails over a replication by itself, because each is an operation on the
// data path rather than a setting.
func ReplicationChangeRefusal(name string, desired, current ast.AsyncReplicationSpec, state string) *Refusal {
	changes := CompareReplication(desired, current)
	subject := "async replication " + name
	switch {
	case len(changes.CreateOnly) > 0:
		return &Refusal{Subject: subject, Reason: fmt.Sprintf("its %s differ from the database's, and YDB changes "+
			"none of them in place (`%s is not supported in ALTER`, and ALTER takes no FOR clause); drop it with "+
			"DROP ASYNC REPLICATION ... CASCADE, which drops its replica tables, and plan again",
			strings.Join(changes.CreateOnly, " and "), strings.ToUpper(createOnlyExample(changes.CreateOnly)))}
	case !changes.ConnectionString && !changes.Credentials:
		return nil
	case state == catalog.ReplicationDone:
		return &Refusal{Subject: subject, Reason: "it was failed over (STATE = 'DONE') and replicates nothing more, " +
			"so its connection no longer applies; remove it from the schema, which drops it and keeps its tables"}
	case HasCredentials(current.Connection) && !HasCredentials(desired.Connection):
		return &Refusal{Subject: subject, Reason: "it is declared with no credential and holds one, and YDB has no " +
			"statement that takes a credential away"}
	case state != catalog.ReplicationPaused:
		return &Refusal{Subject: subject, Reason: fmt.Sprintf("its connection or credential differs, and YDB "+
			"changes them only while the replication is paused (`Modifications are not allowed in StandBy state`), "+
			"which it is not (%s); pause it with ALTER ASYNC REPLICATION %s SET (STATE = 'PAUSED'), apply, and "+
			"resume it with SET (STATE = 'StandBy')", stateWord(state), Path(name))}
	default:
		return nil
	}
}

// TransferChangeRefusal reports why the transfer name cannot move from
// current, in state, to desired in place, or nil when it can. The lambda and
// the batch settings change in any state; the source, the target and the
// consumer never; the connection and the credentials only while the transfer
// is paused.
func TransferChangeRefusal(name string, desired, current ast.TransferSpec, state string) *Refusal {
	changes := CompareTransfer(desired, current)
	subject := "transfer " + name
	switch {
	case len(changes.CreateOnly) > 0:
		return &Refusal{Subject: subject, Reason: fmt.Sprintf("its %s differ from the database's, and YDB changes "+
			"none of them in place (`CONSUMER is not supported in ALTER`, and ALTER takes no FROM or TO); drop it "+
			"with DROP TRANSFER, which loses the position of a consumer YDB created for it, and plan again",
			strings.Join(changes.CreateOnly, " and "))}
	case !changes.ConnectionString && !changes.Credentials:
		return nil
	case HasCredentials(current.Connection) && !HasCredentials(desired.Connection):
		return &Refusal{Subject: subject, Reason: "it is declared with no credential and holds one, and YDB has no " +
			"statement that takes a credential away"}
	case state != catalog.ReplicationPaused:
		return &Refusal{Subject: subject, Reason: fmt.Sprintf("its connection or credential differs, and YDB "+
			"changes them only while the transfer is paused (`Modifications are not allowed in StandBy state`), "+
			"which it is not (%s); pause it with ALTER TRANSFER %s SET (STATE = 'PAUSED'), apply, and resume it "+
			"with SET (STATE = 'StandBy')", stateWord(state), Path(name))}
	default:
		return nil
	}
}

// createOnlyExample is the setting a create-only refusal quotes YDB's answer
// for.
func createOnlyExample(settings []string) string {
	for _, setting := range settings {
		if setting != "items" {
			return setting
		}
	}
	return AttributeConsistencyLevel
}

// stateWord says what state a replication or transfer reported.
func stateWord(state string) string {
	if state == "" {
		return "its state was not read"
	}
	return "it is " + state
}

// replicationValues writes a spec's connection and consistency back as the
// attribute values [ParseReplication] reads.
func replicationValues(spec ast.AsyncReplicationSpec) map[string]string {
	values := connectionValues(spec.Connection)
	if spec.ConsistencyLevel != "" {
		values[AttributeConsistencyLevel] = spec.ConsistencyLevel
	}
	if spec.CommitInterval != "" {
		values[AttributeCommitInterval] = spec.CommitInterval
	}
	return values
}

// transferValues writes a transfer back as the attribute values
// [ParseTransfer] reads.
func transferValues(spec ast.TransferSpec) map[string]string {
	values := connectionValues(spec.Connection)
	values[AttributeSource] = spec.Source
	values[AttributeTarget] = spec.Target
	values[AttributeUsing] = spec.Lambda
	if spec.Consumer != "" {
		values[AttributeConsumer] = spec.Consumer
	}
	if spec.BatchSizeBytes != 0 {
		values[AttributeBatchSizeBytes] = strconv.FormatUint(spec.BatchSizeBytes, 10)
	}
	if spec.FlushInterval != "" {
		values[AttributeFlushInterval] = spec.FlushInterval
	}
	return values
}

// connectionValues writes a connection back as attribute values.
func connectionValues(connection ast.ReplicationConnectionSpec) map[string]string {
	values := make(map[string]string)
	for attribute, value := range map[string]string{
		AttributeConnectionString:   connection.ConnectionString,
		AttributeTokenSecretName:    connection.TokenSecretName,
		AttributeTokenSecretPath:    connection.TokenSecretPath,
		AttributeUser:               connection.User,
		AttributePasswordSecretName: connection.PasswordSecretName,
		AttributePasswordSecretPath: connection.PasswordSecretPath,
	} {
		if value != "" {
			values[attribute] = value
		}
	}
	return values
}

// ValidateReplicationItems checks source and target paths and rejects repeated
// replica targets. The source reader and renderer use the same rule.
func ValidateReplicationItems(items []ast.AsyncReplicationItem) error {
	if len(items) == 0 {
		return fmt.Errorf("it replicates no table; declare an item naming a source and a " +
			"target, since YDB takes no replication without one (`expecting {',', WITH}`)")
	}
	targets := make(map[string]bool, len(items))
	for _, item := range items {
		if _, err := ParseItem(map[string]string{AttributeSource: item.Source, AttributeTarget: item.Target}); err != nil {
			return err
		}
		target := cleanRelative(item.Target)
		if targets[target] {
			return fmt.Errorf("two of its items create a replica at %q, where one table can stand", target)
		}
		targets[target] = true
	}

	return nil
}
