package awsparamstore

// White-box testing required: the error-shape list and the partition table
// are package data copied from the SDK. The black-box differential tests drive
// every SDK entry through both drivers, but cannot see an entry this package
// carries and the SDK does not; comparing the tables reads both directions.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/smithy-go"
	smithytraits "github.com/aws/smithy-go/traits"
	qt "github.com/frankban/quicktest"
)

// TestModeledErrorsMatchTheSDKRegistry compares the code, fault and message
// member of every modeled error with the SDK's SSM type registry.
func TestModeledErrorsMatchTheSDKRegistry(t *testing.T) {
	c := qt.New(t)

	c.Assert(describeModeledErrors(), qt.DeepEquals, describeSDKRegistry())
}

func describeModeledErrors() map[string]string {
	described := make(map[string]string, len(modeledErrors))
	for _, shape := range modeledErrors {
		described[shape.code] = faultName(shape.fault) + " " + shape.messageMember
	}
	return described
}

func describeSDKRegistry() map[string]string {
	described := make(map[string]string, len(ssm.TypeRegistry.Entries))
	for id, entry := range ssm.TypeRegistry.Entries {
		_, code, _ := strings.Cut(id, "#")
		fault := smithy.FaultClient
		if trait, ok := smithy.SchemaTrait[*smithytraits.Error](entry.Schema); ok && trait.Type == "server" {
			fault = smithy.FaultServer
		}
		member := ""
		for name := range entry.Schema.Members() {
			if strings.EqualFold(name, "message") {
				member = name
			}
		}
		described[code] = faultName(fault) + " " + member
	}
	return described
}

// partitionView is a partition with its region list reduced to the names the
// expression does not match, the only ones a lookup by name decides.
type partitionView struct {
	Name, RegionRegex, DNSSuffix, DualStackDNSSuffix string
	NamedRegions                                     []string
	SupportsFIPS, SupportsDualStack                  bool
}

// TestPartitionsMatchTheSDKTable compares the partition table with the SDK's
// partitions.json, in order, and requires that no listed region carries an
// override, since the table has no place for one.
func TestPartitionsMatchTheSDKTable(t *testing.T) {
	c := qt.New(t)

	sdk, overrides := readSDKPartitionViews(c)

	c.Assert(describePartitions(), qt.DeepEquals, sdk)
	c.Assert(overrides, qt.HasLen, 0)
}

func describePartitions() []partitionView {
	views := make([]partitionView, 0, len(partitions))
	for _, p := range partitions {
		views = append(views, partitionView{
			Name: p.name, RegionRegex: p.regionRegex, DNSSuffix: p.dnsSuffix, DualStackDNSSuffix: p.dualStackDNSSuffix,
			NamedRegions: slices.Clone(p.namedRegions), SupportsFIPS: p.supportsFIPS, SupportsDualStack: p.supportsDualStack,
		})
	}
	return views
}

func readSDKPartitionViews(c *qt.C) (views []partitionView, overrides []string) {
	output, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/aws/aws-sdk-go-v2").Output()
	c.Assert(err, qt.IsNil)
	raw, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(output)), "internal", "endpoints", "awsrulesfn", "partitions.json"))
	c.Assert(err, qt.IsNil)
	var table struct {
		Partitions []struct {
			ID          string                    `json:"id"`
			RegionRegex string                    `json:"regionRegex"`
			Regions     map[string]map[string]any `json:"regions"`
			Outputs     struct {
				Name               string `json:"name"`
				DNSSuffix          string `json:"dnsSuffix"`
				DualStackDNSSuffix string `json:"dualStackDnsSuffix"`
				SupportsFIPS       bool   `json:"supportsFIPS"`
				SupportsDualStack  bool   `json:"supportsDualStack"`
			} `json:"outputs"`
		} `json:"partitions"`
	}
	c.Assert(json.Unmarshal(raw, &table), qt.IsNil)
	for _, p := range table.Partitions {
		expression := regexp.MustCompile(p.RegionRegex)
		var named []string
		for region, fields := range p.Regions {
			if !expression.MatchString(region) {
				named = append(named, region)
			}
			for field := range fields {
				if field != "description" {
					overrides = append(overrides, region+"."+field)
				}
			}
		}
		slices.Sort(named)
		views = append(views, partitionView{
			Name: p.Outputs.Name, RegionRegex: p.RegionRegex, DNSSuffix: p.Outputs.DNSSuffix,
			DualStackDNSSuffix: p.Outputs.DualStackDNSSuffix, NamedRegions: named,
			SupportsFIPS: p.Outputs.SupportsFIPS, SupportsDualStack: p.Outputs.SupportsDualStack,
		})
		c.Assert(p.ID, qt.Equals, p.Outputs.Name)
	}
	return views, overrides
}
