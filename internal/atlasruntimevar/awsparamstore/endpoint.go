package awsparamstore

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// partition is the endpoint metadata of one AWS partition, as the SDK's
// partition table (internal/endpoints/awsrulesfn/partitions.json) states it.
type partition struct {
	name        string
	regionRegex string
	// namedRegions are the region names the partition lists that its
	// regionRegex does not match, such as aws-global. Every other listed region
	// matches the expression, and no listed region carries an override.
	namedRegions       []string
	dnsSuffix          string
	dualStackDNSSuffix string
	supportsFIPS       bool
	supportsDualStack  bool
}

// partitions is the SDK's table in its order, which decides which expression
// a region is tried against first. TestEndpointsMatchTheSDK reads the SDK's
// partitions.json and drives every region it lists through both clients, so
// a partition or region an SDK update adds fails the test.
var partitions = []partition{
	{
		name: "aws", regionRegex: `^(us|eu|ap|sa|ca|me|af|il|mx)\-\w+\-\d+$`, namedRegions: []string{"aws-global"},
		dnsSuffix: "amazonaws.com", dualStackDNSSuffix: "api.aws", supportsFIPS: true, supportsDualStack: true,
	},
	{
		name: "aws-cn", regionRegex: `^cn\-\w+\-\d+$`, namedRegions: []string{"aws-cn-global"},
		dnsSuffix: "amazonaws.com.cn", dualStackDNSSuffix: "api.amazonwebservices.com.cn", supportsFIPS: true, supportsDualStack: true,
	},
	{
		name: "aws-eusc", regionRegex: `^eusc\-(de)\-\w+\-\d+$`,
		dnsSuffix: "amazonaws.eu", dualStackDNSSuffix: "api.amazonwebservices.eu", supportsFIPS: true, supportsDualStack: true,
	},
	{
		name: "aws-iso", regionRegex: `^us\-iso\-\w+\-\d+$`, namedRegions: []string{"aws-iso-global"},
		dnsSuffix: "c2s.ic.gov", dualStackDNSSuffix: "api.aws.ic.gov", supportsFIPS: true, supportsDualStack: true,
	},
	{
		name: "aws-iso-b", regionRegex: `^us\-isob\-\w+\-\d+$`, namedRegions: []string{"aws-iso-b-global"},
		dnsSuffix: "sc2s.sgov.gov", dualStackDNSSuffix: "api.aws.scloud", supportsFIPS: true, supportsDualStack: true,
	},
	{
		name: "aws-iso-e", regionRegex: `^eu\-isoe\-\w+\-\d+$`, namedRegions: []string{"aws-iso-e-global"},
		dnsSuffix: "cloud.adc-e.uk", dualStackDNSSuffix: "api.cloud-aws.adc-e.uk", supportsFIPS: true, supportsDualStack: true,
	},
	{
		name: "aws-iso-f", regionRegex: `^us\-isof\-\w+\-\d+$`, namedRegions: []string{"aws-iso-f-global"},
		dnsSuffix: "csp.hci.ic.gov", dualStackDNSSuffix: "api.aws.hci.ic.gov", supportsFIPS: true, supportsDualStack: true,
	},
	{
		name: "aws-us-gov", regionRegex: `^us\-gov\-\w+\-\d+$`, namedRegions: []string{"aws-us-gov-global"},
		dnsSuffix: "amazonaws.com", dualStackDNSSuffix: "api.aws", supportsFIPS: true, supportsDualStack: true,
	},
}

const defaultPartition = "aws"

var partitionExpressions = sync.OnceValue(func() []*regexp.Regexp {
	compiled := make([]*regexp.Regexp, len(partitions))
	for i, p := range partitions {
		compiled[i] = regexp.MustCompile(p.regionRegex)
	}
	return compiled
})

// partitionFor finds the partition of region the way the SDK does: a listed
// name first, then the first expression that matches, then the aws partition.
func partitionFor(region string) partition {
	for _, p := range partitions {
		if slices.Contains(p.namedRegions, region) {
			return p
		}
	}
	for i, expression := range partitionExpressions() {
		if expression.MatchString(region) {
			return partitions[i]
		}
	}
	for _, p := range partitions {
		if p.name == defaultPartition {
			return p
		}
	}
	return partition{}
}

// endpointParameters are the inputs of the SSM endpoint rule set.
type endpointParameters struct {
	region       *string
	useFIPS      bool
	useDualStack bool
	endpoint     *string
}

// The SDK capitalizes this message; staticcheck refuses a capitalized format
// string, so the text is passed as an argument.
const failedToParseURI = "Failed to parse uri"

func ruleError(message string) error {
	return fmt.Errorf("endpoint rule error, %s", message)
}

// resolveEndpoint applies the SSM endpoint rule set: a configured endpoint
// as it is, refused beside FIPS or dual stack; otherwise ssm or ssm-fips under
// the partition's DNS suffix, with the FIPS endpoint of aws-us-gov at the plain
// regional host.
func resolveEndpoint(params endpointParameters) (url.URL, error) {
	if params.endpoint != nil {
		switch {
		case params.useFIPS:
			return url.URL{}, ruleError("Invalid Configuration: FIPS and custom endpoint are not supported")
		case params.useDualStack:
			return url.URL{}, ruleError("Invalid Configuration: Dualstack and custom endpoint are not supported")
		}
		return parseEndpoint(*params.endpoint)
	}
	if params.region == nil {
		return url.URL{}, ruleError("Invalid Configuration: Missing Region")
	}
	region := *params.region
	p := partitionFor(region)
	switch {
	case params.useFIPS && params.useDualStack:
		if p.supportsFIPS && p.supportsDualStack {
			return parseEndpoint("https://ssm-fips." + region + "." + p.dualStackDNSSuffix)
		}
		return url.URL{}, ruleError("FIPS and DualStack are enabled, but this partition does not support one or both")
	case params.useFIPS:
		if !p.supportsFIPS {
			return url.URL{}, ruleError("FIPS is enabled but this partition does not support FIPS")
		}
		if p.name == "aws-us-gov" {
			return parseEndpoint("https://ssm." + region + ".amazonaws.com")
		}
		return parseEndpoint("https://ssm-fips." + region + "." + p.dnsSuffix)
	case params.useDualStack:
		if !p.supportsDualStack {
			return url.URL{}, ruleError("DualStack is enabled but this partition does not support DualStack")
		}
		return parseEndpoint("https://ssm." + region + "." + p.dualStackDNSSuffix)
	}
	return parseEndpoint("https://ssm." + region + "." + p.dnsSuffix)
}

func parseEndpoint(raw string) (url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return url.URL{}, fmt.Errorf("%s: %s", failedToParseURI, raw)
	}
	return *parsed, nil
}

func validHostName(name string) bool {
	for label := range strings.SplitSeq(name, ".") {
		if !smithyhttp.ValidHostLabel(label) || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}

func hasFIPSMarker(region string) bool {
	return strings.Contains(region, "-fips-") || strings.Contains(region, "fips-") || strings.Contains(region, "-fips")
}

func stripFIPSMarker(region string) string {
	if !hasFIPSMarker(region) {
		return region
	}
	region = strings.ReplaceAll(region, "-fips-", "-")
	region = strings.ReplaceAll(region, "fips-", "")
	return strings.ReplaceAll(region, "-fips", "")
}

// The configuration sources an aws.Config carries answer these questions; the
// SDK's service clients ask them through the same method sets.
type (
	useFIPSEndpointProvider interface {
		GetUseFIPSEndpoint(context.Context) (aws.FIPSEndpointState, bool, error)
	}
	useDualStackEndpointProvider interface {
		GetUseDualStackEndpoint(context.Context) (aws.DualStackEndpointState, bool, error)
	}
	serviceBaseEndpointProvider interface {
		GetServiceBaseEndpoint(ctx context.Context, sdkID string) (string, bool, error)
	}
	ignoreConfiguredEndpointsProvider interface {
		GetIgnoreConfiguredEndpoints(context.Context) (bool, bool, error)
	}
)

// firstFound asks each source that implements T, in order, and stops at the
// first that answers or fails.
func firstFound[T, V any](sources []any, ask func(T) (V, bool, error)) (V, bool, error) {
	var zero V
	for _, source := range sources {
		provider, ok := source.(T)
		if !ok {
			continue
		}
		value, found, err := ask(provider)
		if err != nil || found {
			return value, found, err
		}
	}
	return zero, false, nil
}

// endpointOptions is the endpoint configuration an aws.Config resolves to for
// SSM, the way the SDK's ssm.NewFromConfig reads it.
type endpointOptions struct {
	useFIPS      bool
	useDualStack bool
	baseEndpoint *string
	legacy       legacyResolver
}

// legacyResolver is an endpoint resolver the configuration carries.
type legacyResolver func(service, region string) (configuredEndpoint, error)

// configuredEndpoint is what such a resolver answers.
type configuredEndpoint struct {
	url           string
	signingName   string
	signingRegion string
}

func newEndpointOptions(cfg aws.Config) endpointOptions {
	ctx := context.Background()
	options := endpointOptions{baseEndpoint: cfg.BaseEndpoint}
	// The SDK ignores a failure to read these two flags.
	if state, found, err := firstFound(cfg.ConfigSources, func(p useFIPSEndpointProvider) (aws.FIPSEndpointState, bool, error) {
		return p.GetUseFIPSEndpoint(ctx)
	}); err == nil && found {
		options.useFIPS = state == aws.FIPSEndpointStateEnabled
	}
	if state, found, err := firstFound(cfg.ConfigSources, func(p useDualStackEndpointProvider) (aws.DualStackEndpointState, bool, error) {
		return p.GetUseDualStackEndpoint(ctx)
	}); err == nil && found {
		options.useDualStack = state == aws.DualStackEndpointStateEnabled
	}
	if endpoint, found := serviceBaseEndpoint(ctx, cfg.ConfigSources); found {
		options.baseEndpoint = &endpoint
	}
	options.legacy = configuredResolver(cfg)
	return options
}

// configuredResolver is the endpoint resolver cfg carries, or nil. The SDK
// deprecates both fields in favor of BaseEndpoint, but its clients still ask
// them first, and gocloud.dev's endpoint URL parameter sets the first one.
func configuredResolver(cfg aws.Config) legacyResolver {
	if resolver := cfg.EndpointResolverWithOptions; resolver != nil { //nolint:staticcheck // SA1019: gocloud.dev's endpoint URL parameter sets this field.
		return func(service, region string) (configuredEndpoint, error) {
			endpoint, err := resolver.ResolveEndpoint(service, region)
			return configuredEndpoint{url: endpoint.URL, signingName: endpoint.SigningName, signingRegion: endpoint.SigningRegion}, err
		}
	}
	if resolver := cfg.EndpointResolver; resolver != nil { //nolint:staticcheck // SA1019: the SDK's clients honor this field after the one above.
		return func(service, region string) (configuredEndpoint, error) {
			endpoint, err := resolver.ResolveEndpoint(service, region)
			return configuredEndpoint{url: endpoint.URL, signingName: endpoint.SigningName, signingRegion: endpoint.SigningRegion}, err
		}
	}
	return nil
}

// serviceBaseEndpoint is the SSM-specific endpoint of the shared configuration
// and the environment. AWS_ENDPOINT_URL alone already reached cfg.BaseEndpoint,
// so the sources are asked only when AWS_ENDPOINT_URL_SSM is set or
// AWS_ENDPOINT_URL is not.
func serviceBaseEndpoint(ctx context.Context, sources []any) (string, bool) {
	_, global := os.LookupEnv("AWS_ENDPOINT_URL")
	_, service := os.LookupEnv("AWS_ENDPOINT_URL_SSM")
	if global && !service {
		return "", false
	}
	if ignore, found, _ := firstFound(sources, func(p ignoreConfiguredEndpointsProvider) (bool, bool, error) {
		return p.GetIgnoreConfiguredEndpoints(ctx)
	}); found && ignore {
		return "", false
	}
	endpoint, found, err := firstFound(sources, func(p serviceBaseEndpointProvider) (string, bool, error) {
		return p.GetServiceBaseEndpoint(context.Background(), serviceID)
	})
	return endpoint, found && err == nil
}

// parameters binds the rule set's inputs for one call. A region that is not a
// host name is refused. A FIPS pseudo-region such as fips-us-east-1 loses its
// marker and turns FIPS on, whatever the configuration said.
func (o endpointOptions) parameters(region string) (endpointParameters, error) {
	params := endpointParameters{
		useFIPS:      o.useFIPS || hasFIPSMarker(region),
		useDualStack: o.useDualStack,
		endpoint:     o.baseEndpoint,
	}
	if region == "" {
		return params, nil
	}
	if !validHostName(region) {
		return endpointParameters{}, fmt.Errorf("invalid input region %s", region)
	}
	params.region = aws.String(stripFIPSMarker(region))
	return params, nil
}
