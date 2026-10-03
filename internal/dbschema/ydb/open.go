package ydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"

	"ptah.run/internal/ydburl"
)

// Connection is an open YDB database: the database/sql pool that queries and
// DDL run through, and the SDK driver the reader and the writer reach the
// scheme and table services with. Closing DB closes the driver too.
type Connection struct {
	DB     *sql.DB
	Driver *ydbsdk.Driver
}

// The parameters a YDB URL may carry besides database. Each is read by Ptah
// or passed to ydb-go-sdk, which documents it; any other parameter is refused,
// because the SDK ignores a parameter it does not know without a word.
const (
	paramToken             = "token"
	paramUseEnvCredentials = "use_env_credentials"
	paramBalancer          = "go_balancer"
	paramLegacyBalancer    = "balancer"
	paramQueryMode         = "go_query_mode"
	paramLegacyQueryMode   = "query_mode"
	paramDefaultIdempotent = "go_default_idempotent"
	paramPrefetchParts     = "prefetch_query_result_parts"
	paramFakeTx            = "go_fake_tx"
	paramQueryBind         = "go_query_bind"
)

// acceptedParameters lists, in the order a refusal names them, the parameters
// a YDB URL may carry.
var acceptedParameters = []string{
	ydburl.DatabaseParameter, paramToken, paramUseEnvCredentials, paramBalancer, paramLegacyBalancer,
	paramQueryMode, paramLegacyQueryMode, paramDefaultIdempotent, paramPrefetchParts,
}

// Open connects to the YDB database a ydb:// or ydbs:// URL names.
//
// The URL is translated to the SDK's grpc:// or grpcs:// form. Credentials
// come from one place: the URL's user and password (static credentials), a
// token parameter (an access token), or use_env_credentials, which reads the
// YDB_* variables ydb-go-sdk-auth-environ defines. With none of them the
// connection is anonymous.
//
// The database/sql connector takes no bind option. ydb-go-sdk's binders read
// YQL literals by other rules than YQL's, so Ptah writes `$p1`, `$p2`, ... itself
// (sqlutil.Rebind) and the connection names each positional argument; see
// [NewBindingConnector]. It is pinned to the query service, which the SDK otherwise
// lets YDB_DATABASE_SQL_OVER_QUERY_SERVICE switch.
func Open(ctx context.Context, rawURL string) (*Connection, error) {
	parsed, err := ydburl.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid YDB URL: %w", err)
	}
	if parsed.Database == "" {
		return nil, errors.New("invalid YDB URL: name the database in the path (ydb://host:2136/local) " +
			"or in the database parameter")
	}
	passed, err := sdkParameters(parsed.Query)
	if err != nil {
		return nil, fmt.Errorf("invalid YDB URL: %w", err)
	}
	credentials, err := credentialOption(parsed, os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("invalid YDB URL: %w", err)
	}

	options := []ydbsdk.Option{ydbsdk.WithApplicationName("ptah")}
	if credentials != nil {
		options = append(options, credentials)
	}
	driver, err := ydbsdk.Open(ctx, dataSourceName(parsed, passed), options...)
	if err != nil {
		return nil, fmt.Errorf("open YDB driver: %w", err)
	}
	inner, err := ydbsdk.Connector(driver, ydbsdk.WithQueryService(true))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create YDB connector: %w", err), closeDriver(driver))
	}
	return &Connection{
		DB:     sql.OpenDB(NewBindingConnector(inner, func() error { return closeDriver(driver) })),
		Driver: driver,
	}, nil
}

// dataSourceName writes the SDK's form of the URL: grpc:// or grpcs://, the
// endpoint, the database, and the parameters the SDK reads. Credentials are
// passed as options rather than in this string, which the SDK may log.
func dataSourceName(parsed ydburl.URL, passed url.Values) string {
	scheme := "grpc"
	if parsed.Secure {
		scheme = "grpcs"
	}
	dsn := url.URL{Scheme: scheme, Host: parsed.Endpoint(), Path: parsed.Database, RawQuery: passed.Encode()}
	return dsn.String()
}

// sdkParameters checks every parameter and returns the ones the SDK reads
// itself.
func sdkParameters(query url.Values) (url.Values, error) {
	passed := url.Values{}
	for _, key := range slices.Sorted(maps.Keys(query)) {
		values := query[key]
		if len(values) != 1 {
			return nil, fmt.Errorf("parameter %s is given %d times", key, len(values))
		}
		value := values[0]
		if err := checkParameter(key, value); err != nil {
			return nil, err
		}
		switch key {
		case paramBalancer, paramLegacyBalancer, paramDefaultIdempotent, paramPrefetchParts:
			passed.Set(key, value)
		}
	}
	if passed.Has(paramBalancer) && passed.Has(paramLegacyBalancer) {
		return nil, fmt.Errorf("parameters %s and %s both choose a balancer; give one", paramBalancer, paramLegacyBalancer)
	}
	return passed, nil
}

// checkParameter refuses a parameter Ptah does not read or a value the SDK
// would read differently from what was written.
func checkParameter(key, value string) error {
	switch key {
	case paramToken, paramUseEnvCredentials:
		// Read by credentialOption.
		return nil
	case paramBalancer, paramLegacyBalancer:
		// The SDK falls back to its default balancer on a value it cannot
		// parse and says nothing, which a caller who disabled the balancer
		// would learn only from a connection that never comes up.
		if _, err := balancers.CreateFromConfig(value); err != nil {
			return fmt.Errorf("parameter %s=%q is not a balancer ydb-go-sdk knows: "+
				"use disable, single, random_choice, round_robin or a JSON balancer config", key, value)
		}
		return nil
	case paramQueryMode, paramLegacyQueryMode:
		if value != "query" {
			return fmt.Errorf("parameter %s=%q is refused: Ptah runs its reads and DDL through the query service, "+
				"and the table-service modes run one kind of statement each; use %s=query or leave it out",
				key, value, key)
		}
		return nil
	case paramDefaultIdempotent:
		if _, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("parameter %s=%q is not a boolean", key, value)
		}
		return nil
	case paramPrefetchParts:
		if parts, err := strconv.Atoi(value); err != nil || parts < 0 {
			return fmt.Errorf("parameter %s=%q is not a non-negative integer", key, value)
		}
		return nil
	case paramFakeTx:
		return fmt.Errorf("parameter %s is refused: Ptah reads a schema in a snapshot read-only transaction, "+
			"and a faked transaction would read outside it", key)
	case paramQueryBind:
		return fmt.Errorf("parameter %s is refused: Ptah binds parameters itself, and ydb-go-sdk's binders "+
			"rewrite a ? inside a YQL literal", key)
	default:
		return fmt.Errorf("parameter %q is not one Ptah reads on a YDB URL; accepted: %s",
			key, strings.Join(acceptedParameters, ", "))
	}
}

// environmentCredentials are the variables ydb-go-sdk-auth-environ reads, in
// the order it reads them.
var environmentCredentials = []string{
	"YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS",
	"YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS",
	"YDB_METADATA_CREDENTIALS",
	"YDB_ACCESS_TOKEN_CREDENTIALS",
	"YDB_STATIC_CREDENTIALS_USER",
	"YDB_OAUTH2_KEY_FILE",
	"YDB_ANONYMOUS_CREDENTIALS",
}

// credentialOption reads the one credential source a URL names. Two sources
// are refused rather than ranked, since a URL carrying both a password and a
// token says two different things about who connects.
func credentialOption(parsed ydburl.URL, lookup func(string) (string, bool)) (ydbsdk.Option, error) {
	var sources []string
	if parsed.User != nil {
		sources = append(sources, "the URL's user")
	}
	token, hasToken := parsed.Query[paramToken]
	if hasToken {
		sources = append(sources, "the token parameter")
	}
	var useEnvironment bool
	if values, ok := parsed.Query[paramUseEnvCredentials]; ok {
		// Present with no value is on, the way ydb-go-sdk-auth-environ reads
		// it; a value is read as a strict boolean.
		raw := strings.TrimSpace(values[0])
		useEnvironment = true
		if raw != "" {
			enabled, err := strconv.ParseBool(raw)
			if err != nil {
				return nil, fmt.Errorf("parameter %s=%q is not a boolean", paramUseEnvCredentials, values[0])
			}
			useEnvironment = enabled
		}
		if useEnvironment {
			sources = append(sources, paramUseEnvCredentials)
		}
	}
	if len(sources) > 1 {
		return nil, fmt.Errorf("the URL names more than one credential source (%s); give one",
			strings.Join(sources, ", "))
	}

	switch {
	case parsed.User != nil:
		password, _ := parsed.User.Password()
		return ydbsdk.WithStaticCredentials(parsed.User.Username(), password), nil
	case hasToken:
		if strings.TrimSpace(token[0]) == "" {
			return nil, fmt.Errorf("parameter %s is empty", paramToken)
		}
		return ydbsdk.WithAccessTokenCredentials(token[0]), nil
	case useEnvironment:
		if err := checkEnvironmentCredentials(lookup); err != nil {
			return nil, err
		}
		return environ.WithEnvironCredentials(), nil
	default:
		return nil, nil
	}
}

// checkEnvironmentCredentials refuses the environments ydb-go-sdk-auth-environ
// would read as anonymous without saying so: none of its variables set, a
// static user without the password or the endpoint it needs, and a metadata
// switch set to anything but the 1 it reads.
func checkEnvironmentCredentials(lookup func(string) (string, bool)) error {
	set := slices.ContainsFunc(environmentCredentials, func(name string) bool {
		_, ok := lookup(name)
		return ok
	})
	if !set {
		return fmt.Errorf("use_env_credentials is set and no credential variable is: set one of %s",
			strings.Join(environmentCredentials, ", "))
	}
	if value, ok := lookup("YDB_METADATA_CREDENTIALS"); ok && value != "1" {
		return fmt.Errorf("YDB_METADATA_CREDENTIALS=%q is ignored by the YDB SDK, which reads only 1", value)
	}
	if _, ok := lookup("YDB_STATIC_CREDENTIALS_USER"); ok {
		var missing []string
		for _, name := range []string{"YDB_STATIC_CREDENTIALS_PASSWORD", "YDB_STATIC_CREDENTIALS_ENDPOINT"} {
			if _, ok := lookup(name); !ok {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("YDB_STATIC_CREDENTIALS_USER is set without %s, and the YDB SDK would connect "+
				"anonymously instead", strings.Join(missing, " and "))
		}
	}
	return nil
}

// closeDriver closes a driver whose connector could not be built.
func closeDriver(driver *ydbsdk.Driver) error {
	ctx, cancel := context.WithTimeout(context.Background(), driverCloseTimeout)
	defer cancel()
	if err := driver.Close(ctx); err != nil {
		return fmt.Errorf("close YDB driver: %w", err)
	}
	return nil
}

// driverCloseTimeout bounds how long closing waits for the driver's sessions
// and gRPC connections to wind down.
const driverCloseTimeout = 30 * time.Second

// TransactionNotFound reports YDB's answer to a statement in a transaction the
// server no longer has, as a rollback after a failed statement gets.
func TransactionNotFound(err error) bool {
	return ydbsdk.IsOperationError(err, Ydb.StatusIds_NOT_FOUND)
}
