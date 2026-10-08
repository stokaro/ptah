package ydbplan

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbchangefeed"
)

// validateChangefeedRecords checks the entire input before lowering. A record
// cannot relocate a stream or contradict the table state a rebuild would use.
func validateChangefeedRecords(table tableChanges) error {
	subject := fmt.Sprintf("table %q", table.TableName)
	if !table.Desired.HasTable() || !table.Current.HasTable() {
		return refuseFact(subject, "feature changes require captured desired and observed tables")
	}
	parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(table.Desired.Table.Schema, table.Desired.Table.Name)
	currentParent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(table.Current.Table.Schema, table.Current.Table.Name)
	if parent.Key() != currentParent.Key() {
		return refuseFact(subject, "the captured feature operands name different tables")
	}
	seen := make(map[objectidentity.Key]bool)
	for _, record := range table.FeatureChanges {
		if seen[record.Subject.Key()] {
			return refuseFact(subject, "duplicate changefeed change")
		}
		seen[record.Subject.Key()] = true
		if err := validateChangefeedRecord(table, record); err != nil {
			return err
		}
	}
	return nil
}

func validateChangefeedRecord(table tableChanges, record schemaext.ChangeRecord) error {
	subject := fmt.Sprintf("table %q", table.TableName)
	cloned, err := record.Clone()
	if err != nil {
		return err
	}
	change, ok := cloned.Value.(*ydbdiff.Changefeed)
	if !ok {
		return refuseFact(subject, fmt.Sprintf("no YDB planning handler for feature change %q", cloned.Value.Kind()))
	}
	if change.Before == nil && change.After == nil {
		return refuseFact(subject, "a changefeed change has no operands")
	}
	if change.Before != nil {
		if err := validateChangefeedSubject(record.Subject, table.Current.Table.Schema, table.Current.Table.Name, change.Before.Spec); err != nil {
			return err
		}
	}
	if change.After != nil {
		if err := validateChangefeedSubject(record.Subject, table.Desired.Table.Schema, table.Desired.Table.Name, change.After.Spec); err != nil {
			return err
		}
	}
	var before, after schemaext.Value
	if change.Before != nil {
		before = change.Before
	}
	if change.After != nil {
		after = change.After
	}
	if err := ydbchangefeed.ValidateOperand(record.Subject, before, table.Current.OwnedObjects, table.Current.FeatureCoverage); err != nil {
		return err
	}
	if err := ydbchangefeed.ValidateOperand(record.Subject, after, table.Desired.OwnedObjects, table.Desired.FeatureCoverage); err != nil {
		return err
	}
	return nil
}

func validateChangefeedSubject(ref objectidentity.ID, schema, table string, spec ydbschema.ChangefeedSpec) error {
	if ref.Key() != ydbschema.ChangefeedRef(schema, table, spec.Name).Key() {
		return refuseFact(ref.String(), "the changefeed payload disagrees with its subject or table")
	}
	return ydbschema.ValidateChangefeed(spec)
}

func validateTableChanges(tableDiff tableChanges, caps capability.Capabilities) error {
	if err := validateChangefeedRecords(tableDiff); err != nil {
		return err
	}
	desired, err := ydbschema.DesiredChangefeeds(tableDiff.Desired.OwnedObjects, tableDiff.Desired.Table.Schema, tableDiff.Desired.Table.Name)
	if err != nil {
		return err
	}
	subject := fmt.Sprintf("table %q", tableDiff.TableName)
	if !caps.Has(capability.Changefeeds) {
		return refuseKey(capability.Changefeeds, "changing the changefeeds of "+subject)
	}
	indexes := make([]string, 0, len(tableDiff.Desired.Indexes))
	for _, index := range tableDiff.Desired.Indexes {
		indexes = append(indexes, index.Name)
	}
	if reason := ydbchangefeed.NameRefusal(desired, indexes); reason != "" {
		return refuseFact(subject, reason)
	}
	if err := checkPlannedChangefeeds(tableDiff, caps); err != nil {
		return err
	}
	return nil
}

// checkPlannedChangefeeds validates only operations this plan will perform.
// An unchanged disabled sibling stays on the table; it is not recreated.
func checkPlannedChangefeeds(table tableChanges, caps capability.Capabilities) error {
	keyType := ydbchangefeed.FirstKeyType(table.Desired, caps)
	for _, record := range table.FeatureChanges {
		change, ok := record.Value.(*ydbdiff.Changefeed)
		if !ok || change == nil {
			return refuseFact(table.TableName, "no valid changefeed operands to validate")
		}
		if change.After == nil {
			continue
		}
		spec := change.After.Spec.Clone()
		if change.Before != nil && !ydbchangefeed.Recreated(spec, change.Before.Spec) {
			// ALTER TOPIC leaves the stream's enabled state untouched. The
			// creation validator must not interpret it as a request to disable.
			spec.Disabled = false
		}
		if err := ydbchangefeed.CheckPlanned(table.TableName, spec, keyType, caps); err != nil {
			return err
		}
	}
	return nil
}

func refuseKey(key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: string(key), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target", subject, key, platform.YDB)}
}
func refuseFact(subject, reason string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: subject, Err: ptaherr.ErrUnsupportedFeature, Message: fmt.Sprintf("%s: %s", subject, reason)}
}
