package provider

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func validateColumnParent(subject *protocol.ComparisonIdentity, name, namespace string, identifiers *protocol.ComparisonIdentifiers) error {
	if err := validatePreparationIdentity(subject, name, namespace, identifiers.GetDefaultSchema()); err != nil {
		return err
	}
	if subject.GetParent() != nil || len(subject.GetSignature()) != 0 ||
		!columnPartMatches(subject.GetName(), name, identifiers.GetRules().GetTableNames()) {
		return columnsError("a parent has an invalid normalized name or child coordinates")
	}
	if subject.GetSchema().GetDefaulted() && namespace == "" {
		namespace = identifiers.GetDefaultSchema()
	}
	if !columnPartMatches(subject.GetSchema(), namespace, identifiers.GetRules().GetTableNames()) {
		return columnsError("a parent has an invalid normalized schema")
	}
	return nil
}

func columnPartMatches(part *protocol.ComparisonPart, source string, rule protocol.IdentifierRules_Comparison) bool {
	return part.GetSource() == source && part.GetNormalized() == sqlFold(rule, source)
}

func validateColumnIdentity(subject *protocol.ComparisonIdentity, name string, identifiers *protocol.ComparisonIdentifiers) error {
	if err := ValidateComparisonIdentity(subject); err != nil {
		return err
	}
	if subject.GetKind() != "column" || subject.GetParent().GetSource() == "" || len(subject.GetSignature()) != 0 ||
		!columnPartMatches(subject.GetName(), name, identifiers.GetRules().GetColumnNames()) {
		return columnsError("a column requires its exact name, normalized spelling and parent")
	}
	if !columnPartMatches(subject.GetParent(), subject.GetParent().GetSource(), identifiers.GetRules().GetTableNames()) ||
		!columnPartMatches(subject.GetSchema(), subject.GetSchema().GetSource(), identifiers.GetRules().GetTableNames()) {
		return columnsError("a column has an invalid normalized namespace")
	}
	return nil
}

func validateBoundColumn(subject, parent *protocol.ComparisonIdentity, name string, identifiers *protocol.ComparisonIdentifiers) error {
	if err := validateColumnIdentity(subject, name, identifiers); err != nil {
		return err
	}
	expected := proto.CloneOf(parent)
	expected.Kind, expected.Parent, expected.Name = "column", proto.CloneOf(parent.Name), proto.CloneOf(subject.Name)
	if !proto.Equal(subject, expected) {
		return columnsError("a column identity crosses its captured parent boundary")
	}
	return nil
}
