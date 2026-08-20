// Package cloudtoken mints the short-lived IAM credentials an atlas.hcl
// project can name as a database password.
//
// It sits below the CLI because nothing here is Atlas-shaped: an RDS auth
// token is an AWS SigV4 presigned request and a Cloud SQL token is an OAuth2
// access token, both defined by the cloud provider and usable by any client.
// The project-config layer only decides where the value is spelled
// (stokaro/ptah#1617).
package cloudtoken

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sigv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"golang.org/x/oauth2/google"
)

// rdsConnectService is the SigV4 service name an RDS auth token is signed
// for. It is not the "rds" management service: the token authorizes the
// `rds-db:connect` action against a database user, not an API call.
const rdsConnectService = "rds-db"

// rdsTokenLifetime is how long a minted token stays valid. AWS caps it at 15
// minutes and rejects anything longer, so this is the maximum rather than a
// preference.
const rdsTokenLifetime = 15 * time.Minute

// emptyPayloadHash is the SHA-256 of the empty body every presigned GET
// carries. Signing with the literal "UNSIGNED-PAYLOAD" marker instead produces
// a token the RDS endpoint rejects.
var emptyPayloadHash = func() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}()

// AWSRDSOptions names one RDS auth token.
type AWSRDSOptions struct {
	// Endpoint is the database endpoint the token authorizes, host and port.
	Endpoint string
	// Username is the database user the token authenticates as.
	Username string
	// Region is the AWS region the token is signed for. Empty takes the region
	// from the ambient AWS configuration.
	Region string
	// Profile names a shared-config profile. Empty takes the default chain.
	Profile string
}

// AWSRDSToken mints an RDS IAM authentication token.
//
// The token is a SigV4-presigned `GET https://<endpoint>/?Action=connect` with
// the scheme stripped, which is the form the RDS endpoint accepts as a
// password. Signing is local: this reaches the network only if the ambient
// credential chain does, so a run with static credentials in the environment
// mints a token offline.
func AWSRDSToken(ctx context.Context, opts AWSRDSOptions) (string, error) {
	endpoint := strings.TrimSpace(opts.Endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("aws rds token: endpoint is required")
	}
	username := strings.TrimSpace(opts.Username)
	if username == "" {
		return "", fmt.Errorf("aws rds token: username is required")
	}
	cfg, err := loadAWSConfig(ctx, opts)
	if err != nil {
		return "", fmt.Errorf("aws rds token: loading aws config: %w", err)
	}
	region := strings.TrimSpace(opts.Region)
	if region == "" {
		region = cfg.Region
	}
	if region == "" {
		return "", fmt.Errorf(
			"aws rds token: no region: set region on the data source, or AWS_REGION in the environment")
	}
	credentials, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return "", fmt.Errorf("aws rds token: retrieving credentials: %w", err)
	}
	request, err := rdsConnectRequest(endpoint, username)
	if err != nil {
		return "", err
	}
	signed, _, err := sigv4.NewSigner().PresignHTTP(
		ctx,
		credentials,
		request,
		emptyPayloadHash,
		rdsConnectService,
		region,
		time.Now().UTC(),
	)
	if err != nil {
		return "", fmt.Errorf("aws rds token: signing: %w", err)
	}
	return strings.TrimPrefix(signed, "https://"), nil
}

// loadAWSConfig resolves the credential chain, honoring an explicit profile.
func loadAWSConfig(ctx context.Context, opts AWSRDSOptions) (aws.Config, error) {
	loadOptions := make([]func(*awsconfig.LoadOptions) error, 0, 2)
	if profile := strings.TrimSpace(opts.Profile); profile != "" {
		loadOptions = append(loadOptions, awsconfig.WithSharedConfigProfile(profile))
	}
	if region := strings.TrimSpace(opts.Region); region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(region))
	}
	return awsconfig.LoadDefaultConfig(ctx, loadOptions...)
}

// rdsConnectRequest builds the request the token presigns.
//
// The expiry rides in the query rather than in a signer option because the
// presigner signs whatever query it is given: setting it afterwards would
// produce a token whose signature does not cover its own lifetime.
func rdsConnectRequest(endpoint, username string) (*http.Request, error) {
	values := url.Values{}
	values.Set("Action", "connect")
	values.Set("DBUser", username)
	values.Set("X-Amz-Expires", fmt.Sprintf("%d", int(rdsTokenLifetime.Seconds())))
	target := "https://" + endpoint + "/?" + values.Encode()
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("aws rds token: building request for %q: %w", endpoint, err)
	}
	return request, nil
}

// cloudSQLScope is the OAuth2 scope a Cloud SQL IAM database login is granted
// under.
const cloudSQLScope = "https://www.googleapis.com/auth/sqlservice.admin"

// GCPCloudSQLToken mints a Cloud SQL IAM access token from the ambient
// application default credentials.
//
// Unlike the RDS token this is an exchange, not a signature: it contacts
// Google's token endpoint, so it fails on a host with no credentials
// configured rather than producing anything offline.
func GCPCloudSQLToken(ctx context.Context) (string, error) {
	credentials, err := google.FindDefaultCredentials(ctx, cloudSQLScope)
	if err != nil {
		return "", fmt.Errorf("gcp cloudsql token: finding default credentials: %w", err)
	}
	token, err := credentials.TokenSource.Token()
	if err != nil {
		return "", fmt.Errorf("gcp cloudsql token: getting token: %w", err)
	}
	if token.AccessToken == "" {
		return "", fmt.Errorf("gcp cloudsql token: the credential source returned an empty access token")
	}
	return token.AccessToken, nil
}
