// Command ptah-provider-conformance runs the provider conformance suite
// against a provider executable and prints its report: one line per check,
// or the machine-readable report with -json. It exits 0 when every check
// that applies passed, 1 when one failed, and 2 when it could not run.
//
//	ptah-provider-conformance -executable ./bin/provider -provider-id example.com/provider
//
// The live part runs when PTAH_CONFORMANCE_ADDRESS names a database the
// suite may change. The address comes from the environment so that a
// credential in it never appears in a process list. A provider written in
// any language runs the same suite: the command speaks to it only through
// the protocol. The models subcommand prints the provider's model
// identities, for -fixtures of a later release to record.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/stokaro/ptah/provider-sdk/conformance"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

const (
	// addressVariable names the live database.
	addressVariable = "PTAH_CONFORMANCE_ADDRESS"
	// credentialVariable names an address that carries conformance.Canary
	// where a credential goes.
	credentialVariable = "PTAH_CONFORMANCE_CREDENTIAL_ADDRESS" // #nosec G101 -- the name of a variable; the address it names carries the canary, not a credential
	exitConforming     = 0
	exitNonconforming  = 1
	exitUsage          = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

// options are the command's flags.
type options struct {
	executable, digest, providerID, engine, fixtures, require string
	environment                                               stringList
	jsonReport, list                                          bool
	timeout                                                   time.Duration
}

// stringList is a flag that may repeat.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func parse(args []string, stderr io.Writer) (options, error) {
	var o options
	set := flag.NewFlagSet("ptah-provider-conformance", flag.ContinueOnError)
	set.SetOutput(stderr)
	set.StringVar(&o.executable, "executable", "", "the provider executable")
	set.StringVar(&o.digest, "sha256", "", "the executable's SHA-256 digest; computed from the file when empty")
	set.StringVar(&o.providerID, "provider-id", "", "the provider identity the executable must describe")
	set.StringVar(&o.require, "require", strings.Join([]string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.AnalyzeSQLFeature}, ","),
		"the features the host requires of every provider, comma-separated; Ptah requires these")
	set.StringVar(&o.engine, "engine", "", "the engine of the live part; empty selects the provider's only engine")
	set.StringVar(&o.fixtures, "fixtures", "", "a JSON file of calls, recorded model identities and live changes")
	set.Var(&o.environment, "env", "an environment entry NAME=value for the provider; may repeat")
	set.BoolVar(&o.jsonReport, "json", false, "print the machine-readable report")
	set.BoolVar(&o.list, "list", false, "print the checks of the suite as JSON and exit")
	set.DurationVar(&o.timeout, "timeout", 0, "the bound of each call; zero means two minutes")
	if err := set.Parse(args); err != nil {
		return o, err
	}
	if o.list {
		return o, nil
	}
	if o.executable == "" || o.providerID == "" {
		return o, errors.New("-executable and -provider-id are required")
	}
	return o, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	models := len(args) > 0 && args[0] == "models"
	if models {
		args = args[1:]
	}
	o, err := parse(args, stderr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if o.list {
		return printJSON(stdout, stderr, conformance.Checks())
	}
	if o.executable, err = filepath.Abs(o.executable); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if o.digest == "" {
		if o.digest, err = digest(o.executable); err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}
	if models {
		return printModels(ctx, o, stdout, stderr)
	}
	config, err := configure(o, getenv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	report, err := conformance.Check(ctx, config)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if o.jsonReport {
		if code := printJSON(stdout, stderr, report); code != exitConforming {
			return code
		}
	} else {
		printText(stdout, report)
	}
	if len(report.Failures()) != 0 {
		return exitNonconforming
	}
	return exitConforming
}

func digest(path string) (string, error) {
	content, err := os.ReadFile(path) // #nosec G304 -- the operator names the executable the command checks
	if err != nil {
		return "", fmt.Errorf("read the executable: %w", err)
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), nil
}

func configure(o options, getenv func(string) string) (conformance.Config, error) {
	config := conformance.Config{
		Target:            conformance.Executable(o.executable, o.digest, o.environment...),
		ProviderID:        o.providerID,
		Engine:            o.engine,
		CredentialAddress: getenv(credentialVariable),
		CallTimeout:       o.timeout,
	}
	if o.require != "" {
		config.RequiredFeatures = strings.Split(o.require, ",")
	}
	var loaded *fixtures
	if o.fixtures != "" {
		var err error
		if loaded, err = readFixtures(o.fixtures); err != nil {
			return config, err
		}
		config.Calls, config.Models = loaded.calls, loaded.models
	}
	if address := getenv(addressVariable); address != "" {
		config.Live = &conformance.Live{Address: address}
		if loaded != nil {
			config.Live.Schemas, config.Live.LockName = loaded.schemas, loaded.lockName
			config.Live.Changes, config.Live.Refused = loaded.changes, loaded.refused
		}
	}
	return config, nil
}

// printModels starts the provider as a host that lists every two-way
// feature and prints the identities of the models it describes.
func printModels(ctx context.Context, o options, stdout, stderr io.Writer) int {
	client, err := runtime.Start(ctx, runtime.Options{Executable: o.executable, SHA256: o.digest, ProviderID: o.providerID,
		Environment: o.environment, HostFeatures: provider.TwoWayFeatures()})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	defer client.Close()
	return printJSON(stdout, stderr, conformance.ModelIdentities(client.Description()))
}

func printJSON(stdout, stderr io.Writer, value any) int {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	return exitConforming
}

func printText(stdout io.Writer, report *conformance.Report) {
	labels := map[conformance.Status]string{conformance.Passed: "PASS", conformance.Failed: "FAIL", conformance.NotApplicable: "N/A "}
	for _, result := range report.Results {
		line := labels[result.Status] + " " + result.ID
		if result.Detail != "" {
			line += ": " + strings.ReplaceAll(result.Detail, "\n", "\n      ")
		}
		fmt.Fprintln(stdout, line)
	}
	if report.Provider != nil {
		fmt.Fprintf(stdout, "%s %s against contract %s: %d failed\n", report.Provider.ID, report.Provider.Version, report.Contract, len(report.Failures()))
	}
}
