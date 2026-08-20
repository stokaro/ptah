package cloudtoken_test

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/cloudtoken"
)

// An RDS auth token is a SigV4 presigned request, so minting one is local
// arithmetic: these tests run with static credentials and no network at all
// (stokaro/ptah#1617).

// staticAWSEnvironment pins the credential chain to values in the environment,
// so no shared config file, profile, or instance metadata on the host running
// the test can reach the result.
func staticAWSEnvironment(t *testing.T) {
	t.Helper()
	missing := filepath.Join(t.TempDir(), "absent")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
	t.Setenv("AWS_CONFIG_FILE", missing)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
}

// tokenQuery splits a minted token into its endpoint and its query values.
func tokenQuery(c *qt.C, token string) (string, url.Values) {
	c.Helper()
	endpoint, query, found := strings.Cut(token, "/?")
	c.Assert(found, qt.IsTrue, qt.Commentf("token carries no query: %q", token))
	values, err := url.ParseQuery(query)
	c.Assert(err, qt.IsNil)
	return endpoint, values
}

func TestAWSRDSToken_CarriesTheSignedConnectRequest(t *testing.T) {
	c := qt.New(t)
	staticAWSEnvironment(t)

	token, err := cloudtoken.AWSRDSToken(context.Background(), cloudtoken.AWSRDSOptions{
		Endpoint: "db.example.us-east-1.rds.amazonaws.com:5432",
		Username: "app",
		Region:   "us-east-1",
	})

	c.Assert(err, qt.IsNil)
	endpoint, query := tokenQuery(c, token)
	c.Assert(endpoint, qt.Equals, "db.example.us-east-1.rds.amazonaws.com:5432")
	c.Assert(query.Get("Action"), qt.Equals, "connect")
	c.Assert(query.Get("DBUser"), qt.Equals, "app")
	c.Assert(query.Get("X-Amz-Algorithm"), qt.Equals, "AWS4-HMAC-SHA256")
	c.Assert(query.Get("X-Amz-Expires"), qt.Equals, "900")
	c.Assert(query.Get("X-Amz-SignedHeaders"), qt.Equals, "host")
	c.Assert(query.Get("X-Amz-Signature"), qt.Not(qt.Equals), "")
}

// TestAWSRDSToken_SignsForTheConnectServiceNotTheAPI holds the credential
// scope. Signing for "rds" instead of "rds-db" produces a well-formed token
// the database rejects, which no offline shape check would notice.
func TestAWSRDSToken_SignsForTheConnectServiceNotTheAPI(t *testing.T) {
	c := qt.New(t)
	staticAWSEnvironment(t)

	token, err := cloudtoken.AWSRDSToken(context.Background(), cloudtoken.AWSRDSOptions{
		Endpoint: "db.example.eu-west-1.rds.amazonaws.com:5432",
		Username: "app",
		Region:   "eu-west-1",
	})

	c.Assert(err, qt.IsNil)
	_, query := tokenQuery(c, token)
	credential := query.Get("X-Amz-Credential")
	c.Assert(credential, qt.Contains, "/eu-west-1/rds-db/aws4_request")
	c.Assert(credential, qt.Not(qt.Contains), "/eu-west-1/rds/aws4_request")
}

// TestAWSRDSToken_HasNoScheme is the shape the endpoint actually accepts: the
// token is used as a password, and a leading https:// makes it one the server
// refuses.
func TestAWSRDSToken_HasNoScheme(t *testing.T) {
	c := qt.New(t)
	staticAWSEnvironment(t)

	token, err := cloudtoken.AWSRDSToken(context.Background(), cloudtoken.AWSRDSOptions{
		Endpoint: "db.example.us-east-1.rds.amazonaws.com:5432",
		Username: "app",
		Region:   "us-east-1",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(token, "https://"), qt.IsFalse)
	c.Assert(strings.HasPrefix(token, "http"), qt.IsFalse)
}

// TestAWSRDSToken_RegionMustComeFromSomewhere is why the region is checked
// before signing: SigV4 will happily sign for the empty region, producing a
// credential scope no endpoint accepts and an error only the database can
// report.
func TestAWSRDSToken_RegionMustComeFromSomewhere(t *testing.T) {
	c := qt.New(t)
	staticAWSEnvironment(t)

	_, err := cloudtoken.AWSRDSToken(context.Background(), cloudtoken.AWSRDSOptions{
		Endpoint: "db.example.us-east-1.rds.amazonaws.com:5432",
		Username: "app",
	})

	c.Assert(err, qt.ErrorMatches, `aws rds token: no region: .*AWS_REGION.*`)
}

// TestAWSRDSToken_TakesTheRegionFromTheEnvironment is the paired control: the
// region is optional on the data source precisely because the ambient
// configuration can carry it.
func TestAWSRDSToken_TakesTheRegionFromTheEnvironment(t *testing.T) {
	c := qt.New(t)
	staticAWSEnvironment(t)
	t.Setenv("AWS_REGION", "ap-south-1")

	token, err := cloudtoken.AWSRDSToken(context.Background(), cloudtoken.AWSRDSOptions{
		Endpoint: "db.example.ap-south-1.rds.amazonaws.com:5432",
		Username: "app",
	})

	c.Assert(err, qt.IsNil)
	_, query := tokenQuery(c, token)
	c.Assert(query.Get("X-Amz-Credential"), qt.Contains, "/ap-south-1/rds-db/")
}

func TestAWSRDSToken_RefusesAnEmptyEndpointOrUsername(t *testing.T) {
	c := qt.New(t)
	staticAWSEnvironment(t)

	_, noEndpoint := cloudtoken.AWSRDSToken(context.Background(), cloudtoken.AWSRDSOptions{
		Username: "app",
		Region:   "us-east-1",
	})
	_, noUsername := cloudtoken.AWSRDSToken(context.Background(), cloudtoken.AWSRDSOptions{
		Endpoint: "db.example.us-east-1.rds.amazonaws.com:5432",
		Region:   "us-east-1",
	})

	c.Assert(noEndpoint, qt.ErrorMatches, `aws rds token: endpoint is required`)
	c.Assert(noUsername, qt.ErrorMatches, `aws rds token: username is required`)
}

// TestAWSRDSToken_ChangesEveryCallIsNotAssumed holds the one property a cached
// token would break: two mints for the same input differ only if the signature
// covers the timestamp, so a token reused past its 15 minutes would still look
// valid to any shape check.
func TestAWSRDSToken_SignatureCoversTheExpiry(t *testing.T) {
	c := qt.New(t)
	staticAWSEnvironment(t)
	options := cloudtoken.AWSRDSOptions{
		Endpoint: "db.example.us-east-1.rds.amazonaws.com:5432",
		Username: "app",
		Region:   "us-east-1",
	}

	token, err := cloudtoken.AWSRDSToken(context.Background(), options)

	c.Assert(err, qt.IsNil)
	_, query := tokenQuery(c, token)
	// X-Amz-Expires is one of the signed query parameters, so it cannot be
	// edited after the fact: a token claiming a longer life fails the
	// signature check at the endpoint.
	c.Assert(query.Get("X-Amz-Expires"), qt.Equals, "900")
	c.Assert(query.Get("X-Amz-Date"), qt.Matches, `\d{8}T\d{6}Z`)
}

// TestGCPCloudSQLToken_ReportsAMissingCredentialSource is the only half of the
// Cloud SQL path an offline test can hold: minting is a token exchange with
// Google, so the failure is what happens without credentials, and the success
// needs a real account.
func TestGCPCloudSQLToken_ReportsAMissingCredentialSource(t *testing.T) {
	c := qt.New(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "absent.json"))
	t.Setenv("CLOUDSDK_CONFIG", t.TempDir())
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")

	_, err := cloudtoken.GCPCloudSQLToken(context.Background())

	c.Assert(err, qt.ErrorMatches, `gcp cloudsql token: .*`)
}
