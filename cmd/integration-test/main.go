package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/integrationharness"
)

// Root command flag constants
const (
	reportFormatFlag = "report"
	outputDirFlag    = "output"
	databasesFlag    = "databases"
	scenariosFlag    = "scenarios"
	verboseFlag      = "verbose"
)

// List command flag constants
const (
	showStaticFlag  = "static"
	showDynamicFlag = "dynamic"
	showAllFlag     = "all"
)

type rootOptions struct {
	reportFormat string
	outputDir    string
	databases    []string
	scenarios    []string
	verbose      bool
}

type listOptions struct {
	showStatic  bool
	showDynamic bool
	showAll     bool
}

type databaseTarget struct {
	connectionURL string
	cleanupURL    string
}

func newRootCommand() *cobra.Command {
	opts := rootOptions{}
	cmd := &cobra.Command{
		Use:   "ptah-integration-test",
		Short: "Run Ptah migration library integration tests",
		Long: `Run comprehensive integration tests for the Ptah migration library.

This tool tests migration functionality across multiple database backends
including PostgreSQL, MySQL, MariaDB, and opt-in SQL Server. It validates basic
functionality, idempotency, concurrency, failure recovery, and more.

The tests use Docker containers for database backends and generate detailed
reports in multiple formats.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runIntegrationTests(cmd, args, &opts)
		},
	}
	registerRootFlags(cmd, &opts)
	cmd.AddCommand(newListCommand())
	return cmd
}

func registerRootFlags(cmd *cobra.Command, opts *rootOptions) {
	flags := cmd.Flags()
	flags.StringVar(&opts.reportFormat, reportFormatFlag, "stdout", "Report format: stdout, txt, json, or html (can be multiple separated by comma)")
	flags.StringVar(&opts.outputDir, outputDirFlag, "/app/reports", "Output directory for reports")
	flags.StringSliceVar(&opts.databases, databasesFlag, []string{"postgres", "mysql", "mariadb", "cockroachdb", "yugabytedb"}, "Databases to test against; SQL Server is opt-in via sqlserver")
	flags.StringSliceVar(&opts.scenarios, scenariosFlag, []string{}, "Specific scenarios to run (empty = all)")
	flags.BoolVar(&opts.verbose, verboseFlag, false, "Enable verbose output")
}

func newListCommand() *cobra.Command {
	opts := listOptions{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all available test scenarios",
		Long: `List all available integration test scenarios with their descriptions.

This command displays all static and dynamic test scenarios that can be run
with the integration test suite. Use this to see what scenarios are available
before running specific tests with the --scenarios flag.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return listScenarios(cmd, args, &opts)
		},
	}
	registerListFlags(cmd, &opts)
	return cmd
}

func registerListFlags(cmd *cobra.Command, opts *listOptions) {
	flags := cmd.Flags()
	flags.BoolVar(&opts.showStatic, showStaticFlag, false, "Show only static scenarios")
	flags.BoolVar(&opts.showDynamic, showDynamicFlag, false, "Show only dynamic scenarios")
	flags.BoolVar(&opts.showAll, showAllFlag, true, "Show all scenarios (default)")
}

func main() {
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runIntegrationTests(_ *cobra.Command, _ []string, opts *rootOptions) error {
	ctx := context.Background()

	reportFormats := strings.Split(opts.reportFormat, ",")

	// Validate report formats
	for _, reportFormat := range reportFormats {
		format := integrationharness.ReportFormat(reportFormat)
		switch format {
		case integrationharness.FormatStdout, integrationharness.FormatTXT, integrationharness.FormatJSON, integrationharness.FormatHTML:
			// Valid formats
		default:
			return fmt.Errorf("invalid report format: %s (must be txt, json, or html)", reportFormat)
		}
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(opts.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Load test fixtures
	// Try Docker path first, then local development path
	fixturesPath := "/app/fixtures"
	if _, err := os.Stat(fixturesPath); os.IsNotExist(err) {
		// Fallback to local development path
		fixturesPath = "integration/fixtures"
	}
	fixturesFS := os.DirFS(fixturesPath)

	// Create test runner
	runner := integrationharness.NewTestRunner(fixturesFS)

	// Add database connections from environment variables
	dbConnections := configuredDatabaseConnections()

	// Filter databases based on command line arguments
	selectedConnections, err := requestedDatabaseConnections(opts.databases, dbConnections)
	if err != nil {
		return err
	}
	for dbName, target := range selectedConnections {
		if err := runner.AddDatabaseWithCleanup(
			dbName,
			target.connectionURL,
			target.cleanupURL,
		); err != nil {
			return fmt.Errorf("configure %s integration database: %w", dbName, err)
		}
		if opts.verbose {
			fmt.Printf("Added database: %s\n", dbName)
		}
	}

	// Get all scenarios
	allScenarios := integrationharness.GetAllScenarios()

	// Filter scenarios if specific ones were requested
	var scenariosToRun []integrationharness.TestScenario
	if len(opts.scenarios) > 0 {
		scenarioMap := make(map[string]integrationharness.TestScenario)
		for _, scenario := range allScenarios {
			scenarioMap[scenario.Name] = scenario
		}

		for _, scenarioName := range opts.scenarios {
			scenario, exists := scenarioMap[scenarioName]
			if !exists {
				return fmt.Errorf("unknown scenario: %s", scenarioName)
			}
			scenariosToRun = append(scenariosToRun, scenario)
		}
	} else {
		scenariosToRun = allScenarios
	}

	// Add scenarios to runner
	for _, scenario := range scenariosToRun {
		runner.AddScenario(scenario)
		if opts.verbose {
			fmt.Printf("Added scenario: %s\n", scenario.Name)
		}
	}

	fmt.Printf("🏛️  Ptah Migration Library Integration Test Suite\n")
	fmt.Printf("================================================\n\n")
	fmt.Printf("Databases: %s\n", strings.Join(opts.databases, ", "))
	fmt.Printf("Scenarios: %d\n", len(scenariosToRun))
	fmt.Printf("Report Format: %s\n", opts.reportFormat)
	fmt.Printf("Output Directory: %s\n\n", opts.outputDir)

	// Run all tests
	fmt.Printf("🚀 Starting integration tests...\n\n")
	start := time.Now()

	if err := runner.RunAll(ctx); err != nil {
		return fmt.Errorf("failed to run integration tests: %w", err)
	}

	duration := time.Since(start)
	fmt.Printf("✅ Integration tests completed in %v\n\n", duration.Round(time.Millisecond))

	// Generate report
	report := runner.GetReport()
	reporter := integrationharness.NewReporter(report)

	for _, reportFormat := range reportFormats {
		if err := reporter.GenerateReport(integrationharness.ReportFormat(reportFormat), opts.outputDir); err != nil {
			return fmt.Errorf("failed to generate report: %w", err)
		}
	}

	// Print summary
	fmt.Printf("📊 Test Summary:\n")
	fmt.Printf("   Total Tests: %d\n", report.TotalTests)
	fmt.Printf("   Passed: %d\n", report.PassedTests)
	fmt.Printf("   Failed: %d\n", report.FailedTests)
	fmt.Printf("   Skipped: %d\n", report.SkippedTests)

	executedTests := report.PassedTests + report.FailedTests
	if executedTests > 0 {
		successRate := float64(report.PassedTests) / float64(executedTests) * 100
		fmt.Printf("   Success Rate: %.1f%%\n", successRate)
	}

	fmt.Printf("\n📄 Report saved to: %s\n", opts.outputDir)

	// Exit with error code if any tests failed
	if report.FailedTests > 0 {
		fmt.Printf("\n❌ Some tests failed. Check the report for details.\n")
		os.Exit(1)
	}

	switch {
	case executedTests == 0 && report.SkippedTests > 0:
		fmt.Printf("\n⏭️  All requested tests were skipped.\n")
	case report.SkippedTests > 0:
		fmt.Printf("\n🎉 All executed tests passed; %d skipped.\n", report.SkippedTests)
	default:
		fmt.Printf("\n🎉 All tests passed!\n")
	}
	return nil
}

func configuredDatabaseConnections() map[string]databaseTarget {
	return map[string]databaseTarget{
		"postgres": databaseTargetFromEnvironment("POSTGRES_URL", "POSTGRES_CLEANUP_URL"),
		"mysql": databaseTargetFromEnvironment(
			"MYSQL_URL",
			"MYSQL_CLEANUP_URL",
		),
		"mariadb": databaseTargetFromEnvironment(
			"MARIADB_URL",
			"MARIADB_CLEANUP_URL",
		),
		"clickhouse": databaseTargetFromEnvironment(
			"CLICKHOUSE_URL",
			"CLICKHOUSE_CLEANUP_URL",
		),
		"cockroachdb": databaseTargetFromEnvironment(
			"COCKROACHDB_URL",
			"COCKROACHDB_CLEANUP_URL",
		),
		"yugabytedb": databaseTargetFromEnvironment(
			"YUGABYTEDB_URL",
			"YUGABYTEDB_CLEANUP_URL",
		),
		"sqlserver": databaseTargetFromEnvironment(
			"SQLSERVER_URL",
			"SQLSERVER_CLEANUP_URL",
		),
	}
}

func databaseTargetFromEnvironment(connectionEnv, cleanupEnv string) databaseTarget {
	connectionURL := os.Getenv(connectionEnv)
	cleanupURL := os.Getenv(cleanupEnv)
	if cleanupURL == "" {
		cleanupURL = connectionURL
	}
	return databaseTarget{
		connectionURL: connectionURL,
		cleanupURL:    cleanupURL,
	}
}

func requestedDatabaseConnections(
	databases []string,
	dbConnections map[string]databaseTarget,
) (map[string]databaseTarget, error) {
	selected := make(map[string]databaseTarget, len(databases))
	var missing []string
	for _, dbName := range databases {
		canonicalName := platform.NormalizeDialect(dbName)
		if canonicalName == "" {
			canonicalName = dbName
		}
		target, exists := dbConnections[canonicalName]
		if !exists || target.connectionURL == "" {
			missing = append(missing, dbName)
			continue
		}
		selected[canonicalName] = target
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing database URL for requested database(s): %s", strings.Join(missing, ", "))
	}
	return selected, nil
}

func listScenarios(_ *cobra.Command, _ []string, opts *listOptions) error {
	// Get all scenarios
	allScenarios := integrationharness.GetAllScenarios()
	staticScenarios := getStaticScenarios()
	dynamicScenarios := integrationharness.GetDynamicScenarios()

	// Determine which scenarios to show based on flags
	var scenariosToShow []integrationharness.TestScenario
	var title string

	// Handle flag combinations
	switch {
	case opts.showStatic && opts.showDynamic:
		// Both flags set - show all
		scenariosToShow = allScenarios
		title = "All Test Scenarios"
	case opts.showStatic:
		// Only static
		scenariosToShow = staticScenarios
		title = "Static Test Scenarios"
	case opts.showDynamic:
		// Only dynamic
		scenariosToShow = dynamicScenarios
		title = "Dynamic Test Scenarios"
	default:
		// Default - show all
		scenariosToShow = allScenarios
		title = "All Test Scenarios"
	}

	// Print header
	fmt.Printf("🏛️  Ptah Migration Library - %s\n", title)
	fmt.Printf("%s\n\n", strings.Repeat("=", len(title)+35))

	// Group scenarios by type for better organization
	if !opts.showStatic && !opts.showDynamic {
		// Show both types with grouping
		fmt.Printf("📋 Static Scenarios (%d):\n", len(staticScenarios))
		printScenarios(staticScenarios, "  ")

		fmt.Printf("\n🔄 Dynamic Scenarios (%d):\n", len(dynamicScenarios))
		printScenarios(dynamicScenarios, "  ")

		fmt.Printf("\n📊 Summary:\n")
		fmt.Printf("  Total Scenarios: %d\n", len(allScenarios))
		fmt.Printf("  Static: %d\n", len(staticScenarios))
		fmt.Printf("  Dynamic: %d\n", len(dynamicScenarios))
	} else {
		// Show filtered scenarios
		printScenarios(scenariosToShow, "")
		fmt.Printf("\n📊 Total: %d scenarios\n", len(scenariosToShow))
	}

	fmt.Printf("\n💡 Usage:\n")
	fmt.Printf("  Run all scenarios:     ptah-integration-test\n")
	fmt.Printf("  Run specific scenario: ptah-integration-test --scenarios scenario_name\n")
	fmt.Printf("  Run multiple:          ptah-integration-test --scenarios scenario1,scenario2\n")

	return nil
}

// getStaticScenarios returns only the static scenarios (non-dynamic ones)
func getStaticScenarios() []integrationharness.TestScenario {
	allScenarios := integrationharness.GetAllScenarios()
	dynamicScenarios := integrationharness.GetDynamicScenarios()

	// Create a map of dynamic scenario names for quick lookup
	dynamicNames := make(map[string]bool)
	for _, scenario := range dynamicScenarios {
		dynamicNames[scenario.Name] = true
	}

	// Filter out dynamic scenarios
	var staticScenarios []integrationharness.TestScenario
	for _, scenario := range allScenarios {
		if !dynamicNames[scenario.Name] {
			staticScenarios = append(staticScenarios, scenario)
		}
	}

	return staticScenarios
}

// printScenarios prints a list of scenarios with formatting
func printScenarios(scenarios []integrationharness.TestScenario, indent string) {
	for i, scenario := range scenarios {
		// Determine scenario type indicator
		typeIndicator := "📋"
		if strings.HasPrefix(scenario.Name, "dynamic_") {
			typeIndicator = "🔄"
		}

		// Determine if it has enhanced functionality (step recording)
		enhancedIndicator := ""
		if scenario.EnhancedTestFunc != nil {
			enhancedIndicator = " ✨"
		}

		fmt.Printf("%s%s %s%s\n", indent, typeIndicator, scenario.Name, enhancedIndicator)
		fmt.Printf("%s   %s\n", indent, scenario.Description)

		// Add spacing between scenarios except for the last one
		if i < len(scenarios)-1 {
			fmt.Printf("\n")
		}
	}
}
