package atlasmigrate_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasmigrate"
)

func TestValidateApplyDirFormat_HappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name       string
		configured string
		query      url.Values
	}{
		{
			name: "default",
		},
		{
			name:       "configured Atlas",
			configured: "atlas",
		},
		{
			name:       "Atlas URL query",
			configured: "goose",
			query:      url.Values{"format": []string{"atlas"}},
		},
		{
			name:       "empty URL format selects Atlas",
			configured: "goose",
			query:      url.Values{"format": []string{""}},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			err := atlasmigrate.ValidateApplyDirFormat(tt.configured, tt.query)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateApplyDirFormat_FailurePathExternalFormats(t *testing.T) {
	c := qt.New(t)
	formats := []string{
		"dbmate",
		"flyway",
		"golang-migrate",
		"goose",
		"liquibase",
	}

	for _, format := range formats {
		c.Run(format, func(c *qt.C) {
			err := atlasmigrate.ValidateApplyDirFormat(format, nil)

			c.Assert(
				err,
				qt.ErrorMatches,
				`migration directory format "`+format+`" is not executable by ptah atlas migrate apply yet; convert it with ptah atlas migrate import`,
			)
		})
	}
}

func TestValidateApplyDirFormat_FailurePathInvalidFormats(t *testing.T) {
	c := qt.New(t)

	c.Run("unknown configured format", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat("custom", nil)

		c.Assert(err, qt.ErrorMatches, `unknown Atlas migration directory format "custom": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`)
	})

	c.Run("case-sensitive format", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat("ATLAS", nil)

		c.Assert(err, qt.ErrorMatches, `unknown Atlas migration directory format "ATLAS": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`)
	})

	c.Run("configured format whitespace is significant", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat(" atlas ", nil)

		c.Assert(err, qt.ErrorMatches, `unknown Atlas migration directory format " atlas ": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`)
	})

	c.Run("URL format overrides configured format", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat("atlas", url.Values{"format": []string{"goose"}})

		c.Assert(err, qt.ErrorMatches, `migration directory format "goose" is not executable by ptah atlas migrate apply yet; convert it with ptah atlas migrate import`)
	})

	c.Run("unknown query parameter", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat("atlas", url.Values{"version": []string{"1"}})

		c.Assert(err, qt.ErrorMatches, `unsupported migration directory URL query parameter "version"`)
	})

	c.Run("multiple format query parameters", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat("atlas", url.Values{"format": []string{"atlas", "goose"}})

		c.Assert(err, qt.ErrorMatches, "migration directory URL contains multiple format parameters")
	})

	c.Run("URL format whitespace is significant", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat("atlas", url.Values{"format": []string{" atlas "}})

		c.Assert(err, qt.ErrorMatches, `unknown Atlas migration directory format " atlas ": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`)
	})

	c.Run("unknown query parameters are reported deterministically", func(c *qt.C) {
		err := atlasmigrate.ValidateApplyDirFormat(
			"atlas",
			url.Values{
				"version":  []string{"1"},
				"checksum": []string{"required"},
			},
		)

		c.Assert(err, qt.ErrorMatches, `unsupported migration directory URL query parameter "checksum"`)
	})
}
