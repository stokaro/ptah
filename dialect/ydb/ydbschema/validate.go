package ydbschema

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// ValidateChangefeed checks the local wire model without probing target capabilities.
func ValidateChangefeed(feed ChangefeedSpec) error {
	if err := ValidateChangefeedName(feed.Name); err != nil {
		return err
	}
	if feed.Mode == "" || feed.Format == "" {
		return fmt.Errorf("%w: a changefeed requires mode and format", schemaext.ErrInvalidValue)
	}
	if err := validText(feed.Mode, feed.Format, feed.ResolvedTimestamps, feed.RetentionPeriod); err != nil {
		return err
	}
	seen := make(map[string]bool, len(feed.Consumers))
	for _, consumer := range feed.Consumers {
		if strings.TrimSpace(consumer.Name) == "" || seen[consumer.Name] {
			return fmt.Errorf("%w: a consumer requires a unique nonempty name", schemaext.ErrInvalidValue)
		}
		if err := validText(consumer.Name, consumer.ReadFrom, consumer.AvailabilityPeriod); err != nil {
			return err
		}
		if err := validText(consumer.SupportedCodecs...); err != nil {
			return err
		}
		seen[consumer.Name] = true
	}
	return nil
}

// encoding/json replaces invalid string bytes. Refuse them before encoding so
// an artifact cannot silently describe a different value from the input.
func validText(values ...string) error {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return fmt.Errorf("%w: a changefeed contains invalid UTF-8", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

// ValidateChangefeedName requires one nonempty stream path component.
func ValidateChangefeedName(name string) error {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '/') {
		return fmt.Errorf("%w: a changefeed needs a name without a slash", schemaext.ErrInvalidValue)
	}
	return validText(name)
}

// CanonicalChangefeed sorts consumer and codec sets. All other fields retain their
// declaration spelling; applying server defaults is comparison, not encoding.
func CanonicalChangefeed(feed *ChangefeedSpec) {
	for i := range feed.Consumers {
		slices.Sort(feed.Consumers[i].SupportedCodecs)
	}
	slices.SortFunc(feed.Consumers, func(a, b ast.TopicConsumerSpec) int { return strings.Compare(a.Name, b.Name) })
}
