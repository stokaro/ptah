package awsparamstore_test

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/smithy-go"
	qt "github.com/frankban/quicktest"
	"gocloud.dev/runtimevar"
)

// sdkModuleDir is the source directory of the aws-sdk-go-v2 module this build
// uses.
func sdkModuleDir(c *qt.C) string {
	output, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/aws/aws-sdk-go-v2").Output()
	c.Assert(err, qt.IsNil)
	dir := strings.TrimSpace(string(output))
	c.Assert(dir, qt.Not(qt.Equals), "")
	return dir
}

// sdkPartitions is the SDK's partition table, the data its endpoint rules
// read.
type sdkPartitions struct {
	Partitions []struct {
		ID          string                     `json:"id"`
		RegionRegex string                     `json:"regionRegex"`
		Regions     map[string]json.RawMessage `json:"regions"`
	} `json:"partitions"`
}

func readSDKPartitions(c *qt.C) sdkPartitions {
	raw, err := os.ReadFile(filepath.Join(sdkModuleDir(c), "internal", "endpoints", "awsrulesfn", "partitions.json"))
	c.Assert(err, qt.IsNil)
	var table sdkPartitions
	c.Assert(json.Unmarshal(raw, &table), qt.IsNil)
	return table
}

// endpointRegions are every region the SDK lists, and the names the rules
// treat specially: FIPS pseudo-regions, a region no partition matches, and a
// region that is not a host name.
func endpointRegions(c *qt.C) []string {
	regions := []string{
		"fips-us-east-1", "us-east-1-fips", "us-fips-east-1", "fips-us-gov-west-1", "us-gov-west-1-fips",
		"xx-unknown-9", "US-EAST-1", "us-east-1.internal", "bad region", "-us-east-1",
	}
	for _, p := range readSDKPartitions(c).Partitions {
		for region := range p.Regions {
			regions = append(regions, region)
		}
	}
	slices.Sort(regions)
	return regions
}

// endpointFlags are the FIPS and dual-stack combinations, each set both ways
// the configuration can carry it: load options and the environment.
var endpointFlags = []struct {
	name    string
	options []func(*config.LoadOptions) error
	env     map[string]string
}{
	{name: "neither"},
	{name: "fips", options: []func(*config.LoadOptions) error{config.WithUseFIPSEndpoint(aws.FIPSEndpointStateEnabled)}},
	{name: "dual stack", options: []func(*config.LoadOptions) error{config.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled)}},
	{name: "both", options: []func(*config.LoadOptions) error{
		config.WithUseFIPSEndpoint(aws.FIPSEndpointStateEnabled), config.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled),
	}},
	{name: "fips disabled explicitly", options: []func(*config.LoadOptions) error{config.WithUseFIPSEndpoint(aws.FIPSEndpointStateDisabled)}},
	{name: "both from the environment", env: map[string]string{"AWS_USE_FIPS_ENDPOINT": "true", "AWS_USE_DUALSTACK_ENDPOINT": "true"}},
}

// resolvedEndpoint is where a driver sent its one request, the error it
// failed with instead, and how it signed.
type resolvedEndpoint struct {
	URL             string
	CredentialScope string
	Error           string
}

func resolveThrough(c *qt.C, d driver, region string, flag int) resolvedEndpoint {
	isolateAWSEnvironment(c)
	setEnvironment(c, staticCredentials, endpointFlags[flag].env)
	fake := newFakeSSM(found("ptah-db", "String", value, 1))
	options := append([]func(*config.LoadOptions) error{config.WithRegion(region)}, endpointFlags[flag].options...)
	cfg := testConfig(c, fake, options...)
	state := firstState(c, func() (*runtimevar.Variable, error) {
		return d.openConfig(cfg, "ptah-db", runtimevar.StringDecoder, 0)
	})
	resolved := resolvedEndpoint{}
	if state.Error != nil {
		resolved.Error = state.Error.Text
	}
	for _, sent := range fake.recorded() {
		resolved.URL, resolved.CredentialScope = sent.URL, sent.CredentialScope
	}
	return resolved
}

// TestEndpointsMatchTheSDK drives every region of the SDK's partition table,
// and the special names above, through both drivers with every flag
// combination, and requires the same host, signing scope or error. The region
// list is read from the SDK module, so a partition or region an SDK update
// adds is compared without an edit here.
func TestEndpointsMatchTheSDK(t *testing.T) {
	c := qt.New(t)
	regions := endpointRegions(c)
	c.Assert(len(regions) > 40, qt.IsTrue, qt.Commentf("the SDK partition table listed %d regions", len(regions)))
	for _, region := range regions {
		for flag, flags := range endpointFlags {
			t.Run(region+"/"+flags.name, func(t *testing.T) {
				c := qt.New(t)

				reference := resolveThrough(c, drivers[0], region, flag)
				candidate := resolveThrough(c, drivers[1], region, flag)

				c.Assert(candidate, qt.DeepEquals, reference)
			})
		}
	}
}

// TestEndpointsCoverEveryRuleOutcome pins that the comparison above reaches
// each outcome of the rule set, so a region list that lost a partition or a
// flag combination cannot leave a branch uncompared.
func TestEndpointsCoverEveryRuleOutcome(t *testing.T) {
	tests := []struct {
		region  string
		flag    int
		wantURL string
		wantErr string
	}{
		{region: "us-east-1", flag: 0, wantURL: "https://ssm.us-east-1.amazonaws.com/"},
		{region: "us-east-1", flag: 1, wantURL: "https://ssm-fips.us-east-1.amazonaws.com/"},
		{region: "us-east-1", flag: 2, wantURL: "https://ssm.us-east-1.api.aws/"},
		{region: "us-east-1", flag: 3, wantURL: "https://ssm-fips.us-east-1.api.aws/"},
		{region: "us-gov-west-1", flag: 1, wantURL: "https://ssm.us-gov-west-1.amazonaws.com/"},
		{region: "cn-north-1", flag: 2, wantURL: "https://ssm.cn-north-1.api.amazonwebservices.com.cn/"},
		{region: "fips-us-east-1", flag: 4, wantURL: "https://ssm-fips.us-east-1.amazonaws.com/"},
		{region: "aws-cn-global", flag: 0, wantURL: "https://ssm.aws-cn-global.amazonaws.com.cn/"},
		{region: "xx-unknown-9", flag: 0, wantURL: "https://ssm.xx-unknown-9.amazonaws.com/"},
		{region: "bad region", flag: 0, wantErr: `.*failed to bind endpoint params, invalid input region bad region`},
	}
	for _, test := range tests {
		t.Run(test.region, func(t *testing.T) {
			c := qt.New(t)

			resolved := resolveThrough(c, drivers[1], test.region, test.flag)

			c.Assert(resolved.URL, qt.Equals, test.wantURL)
			c.Assert(resolved.Error, qt.Matches, test.wantErr)
		})
	}
}

// TestModeledErrorsReadLikeTheSDK answers with every error shape of the SDK's
// SSM registry, carrying a message under both spellings, and requires the same
// error from both drivers: which spelling the shape reads, its fault, and
// whether the code is modeled at all.
func TestModeledErrorsReadLikeTheSDK(t *testing.T) {
	c := qt.New(t)
	codes := make([]string, 0, len(ssm.TypeRegistry.Entries))
	for id := range ssm.TypeRegistry.Entries {
		_, code, _ := strings.Cut(id, "#")
		codes = append(codes, code)
	}
	codes = append(codes, "PtahUnmodeledException")
	slices.Sort(codes)
	c.Assert(len(codes) > 100, qt.IsTrue, qt.Commentf("the SDK registry listed %d codes", len(codes)))
	for _, code := range codes {
		t.Run(code, func(t *testing.T) {
			c := qt.New(t)
			row := configScenario{replies: []reply{{
				status: http.StatusBadRequest,
				body:   `{"__type":"` + code + `","Message":"capitalized member","message":"lowercase member"}`,
			}}}
			fake := newFakeSSM(row.replies...)

			reference, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
				return []observation{firstState(c, openConfigScenario(c, row, fake, "", d))}
			})

			c.Assert(candidate, qt.DeepEquals, reference)
			c.Assert(reference.States[0].Error, qt.IsNotNil)
			c.Assert(reference.States[0].Error.APICode, qt.Equals, code)
		})
	}
}

// TestModeledErrorsKeepTheirFault pins the one shape with a server fault,
// which the comparison above checks only against the reference.
func TestModeledErrorsKeepTheirFault(t *testing.T) {
	c := qt.New(t)
	row := configScenario{replies: []reply{failedUpper(http.StatusBadRequest, "InternalServerError", "boom")}}
	fake := newFakeSSM(row.replies...)

	_, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
		return []observation{firstState(c, openConfigScenario(c, row, fake, "", d))}
	})

	c.Assert(candidate.States[0].Error, qt.IsNotNil)
	c.Assert(candidate.States[0].Error.APIFault, qt.Equals, smithy.FaultServer)
	c.Assert(candidate.States[0].Error.Text, qt.Matches, `.*, InternalServerError: boom`)
}
