package ydbreplication

import (
	"fmt"
	"maps"
	"strings"

	"ptah.run/core/ast"
)

// SourceKind identifies the declaration whose YQL settings are being read.
type SourceKind string

const (
	// ReplicationSource selects async replication settings.
	ReplicationSource SourceKind = "replication"
	// TransferSource selects transfer settings.
	TransferSource SourceKind = "transfer"
)

// SourceSettingType names the YQL value form of a modeled replication or
// transfer setting. An empty result means the setting has no desired model.
func SourceSettingType(name string, kind SourceKind) string {
	switch name {
	case AttributeConnectionString, "endpoint", "database", AttributeTokenSecretName,
		AttributeTokenSecretPath, AttributeUser, AttributePasswordSecretName, AttributePasswordSecretPath:
		return "String"
	case AttributeConsistencyLevel:
		if kind == ReplicationSource {
			return "String"
		}
	case AttributeCommitInterval:
		if kind == ReplicationSource {
			return "Interval"
		}
	case AttributeConsumer:
		if kind == TransferSource {
			return "String"
		}
	case AttributeBatchSizeBytes:
		if kind == TransferSource {
			return "Uint"
		}
	case AttributeFlushInterval:
		if kind == TransferSource {
			return "Interval"
		}
	}
	return ""
}

// ParseReplicationSource reads YQL settings, including ENDPOINT and DATABASE,
// through the same model rules as Go and YAML. Items belong to the caller.
func ParseReplicationSource(settings map[string]string) (ast.AsyncReplicationSpec, error) {
	values, err := sourceValues(nil, settings)
	if err != nil {
		return ast.AsyncReplicationSpec{}, err
	}
	return ParseReplication(values)
}

// ParseTransferSource reads a transfer's YQL settings and its FROM, TO and
// USING clauses through the shared declaration rules.
func ParseTransferSource(settings map[string]string) (ast.TransferSpec, error) {
	values, err := sourceValues(nil, settings)
	if err != nil {
		return ast.TransferSpec{}, err
	}
	return ParseTransfer(values)
}

// ApplyReplicationSettings folds a source ALTER into its earlier declaration.
// Lifecycle commands and create-only settings are not desired-state changes.
func ApplyReplicationSettings(previous ast.AsyncReplicationSpec, settings map[string]string) (ast.AsyncReplicationSpec, error) {
	if err := mutableSourceSettings(settings, ReplicationSource); err != nil {
		return ast.AsyncReplicationSpec{}, err
	}
	values, err := sourceValues(replicationValues(previous), settings)
	if err != nil {
		return ast.AsyncReplicationSpec{}, err
	}
	spec, err := ParseReplication(values)
	spec.Items = previous.Clone().Items
	return spec, err
}

// ApplyTransferSettings folds mutable YQL settings and an optional inline
// lambda into a previously declared transfer.
func ApplyTransferSettings(previous ast.TransferSpec, settings map[string]string) (ast.TransferSpec, error) {
	if err := mutableSourceSettings(settings, TransferSource); err != nil {
		return ast.TransferSpec{}, err
	}
	values, err := sourceValues(transferValues(previous), settings)
	if err != nil {
		return ast.TransferSpec{}, err
	}
	return ParseTransfer(values)
}

func mutableSourceSettings(settings map[string]string, kind SourceKind) error {
	if len(settings) == 0 {
		return fmt.Errorf("ALTER needs a modeled setting")
	}
	for name := range settings {
		if (kind == TransferSource && name == AttributeUsing) ||
			(SourceSettingType(name, kind) != "" && name != AttributeConsistencyLevel && name != AttributeCommitInterval && name != AttributeConsumer) {
			continue
		}
		return fmt.Errorf("setting %q cannot change in a desired ALTER declaration", name)
	}
	return nil
}

// YDB replaces the credential variant when switching between a token and a
// password; settings within the password variant retain unspecified fields.
func sourceValues(previous, settings map[string]string) (map[string]string, error) {
	values := maps.Clone(previous)
	if values == nil {
		values = make(map[string]string)
	}
	if err := sourceEndpoint(values, settings); err != nil {
		return nil, err
	}
	// Preserve presence even for a value that validation will refuse as empty.
	if _, ok := settings[AttributeTokenSecretName]; ok {
		delete(values, AttributeTokenSecretPath)
		clearPassword(values)
	}
	if _, ok := settings[AttributeTokenSecretPath]; ok {
		delete(values, AttributeTokenSecretName)
		clearPassword(values)
	}
	if _, ok := settings[AttributePasswordSecretName]; ok {
		delete(values, AttributePasswordSecretPath)
		clearToken(values)
	}
	if _, ok := settings[AttributePasswordSecretPath]; ok {
		delete(values, AttributePasswordSecretName)
		clearToken(values)
	}
	if _, ok := settings[AttributeUser]; ok {
		clearToken(values)
	}
	maps.Copy(values, settings)
	delete(values, "endpoint")
	delete(values, "database")
	return values, nil
}

func clearPassword(values map[string]string) {
	delete(values, AttributeUser)
	delete(values, AttributePasswordSecretName)
	delete(values, AttributePasswordSecretPath)
}

func clearToken(values map[string]string) {
	delete(values, AttributeTokenSecretName)
	delete(values, AttributeTokenSecretPath)
}

func sourceEndpoint(values, settings map[string]string) error {
	_, endpoint := settings["endpoint"]
	_, database := settings["database"]
	if !endpoint && !database {
		return nil
	}
	if _, connection := settings[AttributeConnectionString]; connection {
		return fmt.Errorf("CONNECTION_STRING cannot be combined with ENDPOINT or DATABASE")
	}
	var parsed Endpoint
	if old := values[AttributeConnectionString]; old != "" {
		var err error
		parsed, err = ParseConnectionString(old)
		if err != nil {
			return err
		}
	}
	if endpoint {
		raw := settings["endpoint"]
		parsed.Secure = strings.HasPrefix(raw, "grpcs://")
		scheme := "grpc://"
		if parsed.Secure {
			scheme = "grpcs://"
		}
		parsed.Address = strings.TrimPrefix(raw, scheme)
	}
	if database, exists := settings["database"]; exists {
		parsed.Database = database
	}
	values[AttributeConnectionString] = parsed.String()

	return nil
}
