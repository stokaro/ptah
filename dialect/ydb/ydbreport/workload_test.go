package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbworkload"
)

func TestWorkloadReportsCountCapturedValuesInBothRepresentations(t *testing.T) {
	for _, test := range []struct {
		name           string
		service        schemaext.ReportingService
		representation schemaext.Representation
		value          schemaext.Value
		metric         string
	}{
		{"desired pool", ydbreport.PoolService{}, schemaext.Desired, &ydbworkload.DesiredPool{}, "resource_pools"},
		{"observed pool", ydbreport.PoolService{}, schemaext.Observed, &ydbworkload.ObservedPool{}, "resource_pools"},
		{"desired classifier", ydbreport.ClassifierService{}, schemaext.Desired, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 0}}, "resource_pool_classifiers"},
		{"observed classifier", ydbreport.ClassifierService{}, schemaext.Observed, &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 20}}, "resource_pool_classifiers"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := test.service.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []schemaext.ValueReport{{Kind: test.value.Kind(), Counts: []schemaext.MetricCount{{Name: test.metric, Value: 1}}}})
		})
	}
}

func TestWorkloadReportsRefuseOtherFamiliesAndRepresentations(t *testing.T) {
	for _, invalid := range []schemaext.Value{&ydbworkload.DesiredPool{}, &ydbworkload.ObservedClassifier{}, (*ydbworkload.ObservedPool)(nil)} {
		t.Run(string(invalid.Kind()), func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.PoolService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed,
				Values: []schemaext.Value{&ydbworkload.ObservedPool{}, invalid}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}
