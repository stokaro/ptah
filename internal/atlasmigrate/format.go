package atlasmigrate

import (
	"fmt"
	"maps"
	"net/url"
	"slices"
)

const atlasApplyDirFormat = "atlas"

var externalApplyDirFormats = []string{
	"dbmate",
	"flyway",
	"golang-migrate",
	"goose",
	"liquibase",
}

// ValidateApplyDirFormat rejects migration directory formats that the Atlas
// apply path cannot execute without silently changing their semantics.
func ValidateApplyDirFormat(configured string, query url.Values) error {
	format, found, err := applyDirURLFormat(query)
	if err != nil {
		return err
	}
	if found {
		return validateApplyDirFormat(format)
	}
	return validateApplyDirFormat(configured)
}

func validateApplyDirFormat(value string) error {
	if value == "" || value == atlasApplyDirFormat {
		return nil
	}
	if slices.Contains(externalApplyDirFormats, value) {
		return fmt.Errorf(
			"migration directory format %q is not executable by ptah atlas migrate apply yet; convert it with ptah atlas migrate import",
			value,
		)
	}
	return fmt.Errorf(
		"unknown Atlas migration directory format %q: expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate",
		value,
	)
}

func applyDirURLFormat(query url.Values) (string, bool, error) {
	for _, key := range slices.Sorted(maps.Keys(query)) {
		if key != "format" {
			return "", false, fmt.Errorf("unsupported migration directory URL query parameter %q", key)
		}
	}
	formats := query["format"]
	if len(formats) > 1 {
		return "", false, fmt.Errorf("migration directory URL contains multiple format parameters")
	}
	if len(formats) == 1 {
		return formats[0], true, nil
	}
	return "", false, nil
}
