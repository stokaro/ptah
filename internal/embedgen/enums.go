package embedgen

// The enumerated values this build acts on.
//
// These lists are what a specification file is checked against, and they are
// separate from the constant declarations on purpose: a value that exists as a
// constant and is absent from its list is a value nothing accepts, and a value
// in a list with no constant behind it does not compile. What neither of those
// catches is a constant added and never listed, which would be a policy an
// operator can write and Ptah silently never applies -- so
// TestEnums_ListEveryDeclaredConstant reads the declarations themselves and
// requires each to appear (stokaro/ptah#2068).

// VersionStrategies are the source-version strategies.
func VersionStrategies() []VersionStrategy {
	return []VersionStrategy{
		VersionUnset, VersionMonotonic, VersionOutboxSequence, VersionUpdatedAt, VersionInputHash,
	}
}

// NullPolicies are the ways a NULL input field is treated.
func NullPolicies() []NullPolicy {
	return []NullPolicy{NullAsEmpty, NullSkipField, NullRefuseRow}
}

// EmptyPolicies are the ways an empty canonical input is treated.
func EmptyPolicies() []EmptyPolicy {
	return []EmptyPolicy{EmptyRefuseRow, EmptySkipRow}
}

// UnicodeForms are the Unicode normalization forms.
func UnicodeForms() []UnicodeForm {
	return []UnicodeForm{UnicodeNone, UnicodeNFC, UnicodeNFD, UnicodeNFKC, UnicodeNFKD}
}

// TruncatePolicies are the ways an over-long input is treated.
func TruncatePolicies() []TruncatePolicy {
	return []TruncatePolicy{TruncateRefuse, TruncateBytes, TruncateChunk}
}

// EndpointClasses separate a local endpoint from a hosted one.
func EndpointClasses() []EndpointClass {
	return []EndpointClass{EndpointLocal, EndpointHosted, EndpointGateway}
}

// VectorNormalizations are what a provider says about its vectors' magnitude.
func VectorNormalizations() []VectorNormalization {
	return []VectorNormalization{NormalizationNone, NormalizationL2}
}

// DistanceMetrics are the distances a generation can be compared under.
func DistanceMetrics() []DistanceMetric {
	return []DistanceMetric{MetricCosine, MetricL2, MetricInnerProduct}
}
