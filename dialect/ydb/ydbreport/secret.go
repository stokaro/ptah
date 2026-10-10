package ydbreport

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretService reports YDB secrets. A secret counts once; nothing about its
// value is reported, because nothing about it is known.
type SecretService struct{}

// SecretDefinitions declares the inventory metric for secrets.
func SecretDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbsecret.Kind, DisplayName: "secrets",
		Metrics: []schemaext.MetricDefinition{{Name: "secrets", Help: "Secret objects, without their values"}}}}
}

// ReportValues validates representation and counts each secret once.
func (SecretService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportStandalone(ctx, request, "secret", ydbsecret.Kind, "secrets", ydbsecret.Codecs())
}
