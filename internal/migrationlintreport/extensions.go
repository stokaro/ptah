package migrationlintreport

import (
	"context"
	"io"
	"slices"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/vulnerability"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrator"
)

// extensionCollector reads, at the start of each version the analysis asked
// about, which extensions the dev database holds, and once the replay has run
// every migration, what it offers to install for the extensions the analysis
// asked about and the server it runs on. The advisory data is matched
// afterwards, in [extensionCollector.assess], so a slow advisory source does
// not hold the dev database.
//
// A nil collector asks for nothing, so a directory that installs no extension
// pays no read and asks the advisory source nothing.
type extensionCollector struct {
	requests []lint.ExtensionRequest
	// versions are the versions whose starting state held is read for, and
	// held what the database held at each.
	versions []int64
	held     map[int64]map[string]string
	// read reports that the replay reached the observer.
	read bool
	// identity is the dev database's server, with its version, and catalog
	// what it can install; both are zero on a server whose product has no
	// reviewed advisory matcher.
	identity vulnerability.Inventory
	catalog  vulnerability.ExtensionCatalog
}

// newExtensionCollector returns the collector for the requests the files of
// versions make, or nil when no file holds an extension statement. A file
// whose extension statements lint cannot read makes no request and still
// needs the collector: its findings name the dev database's server.
func newExtensionCollector(requests []lint.ExtensionRequest, versions []int64) *extensionCollector {
	if len(versions) == 0 {
		return nil
	}
	return &extensionCollector{requests: requests, versions: versions, held: make(map[int64]map[string]string)}
}

// versionObserver returns the replay hook that reads, before each version the
// collector asks about, which extensions the database holds, or nil for a nil
// collector. A product without a reviewed advisory matcher is not read: its
// requests are reported as unassessed whatever it holds.
func (c *extensionCollector) versionObserver() func(context.Context, *migrator.Migration, *dbschema.DatabaseConnection) error {
	if c == nil {
		return nil
	}
	return func(ctx context.Context, migration *migrator.Migration, conn *dbschema.DatabaseConnection) error {
		if migration == nil || !slices.Contains(c.versions, migration.Version) {
			return nil
		}
		if identity := vulnerability.CollectIdentity(conn); identity.Product != "postgres" || identity.ScanCapability != "supported" {
			return nil
		}
		held, err := vulnerability.ReadInstalledExtensions(ctx, conn)
		if err != nil {
			return err
		}
		c.held[migration.Version] = held
		return nil
	}
}

// observer returns the replay hook, or nil for a nil collector.
func (c *extensionCollector) observer(ctx context.Context) func(*dbschema.DatabaseConnection) error {
	if c == nil {
		return nil
	}
	return func(conn *dbschema.DatabaseConnection) error {
		return c.observe(ctx, conn)
	}
}

// observe reads the server and its extension catalog. A product that has no
// reviewed advisory matcher, or a build that does not declare vulnerability
// scanning, is recorded by identity alone: the matcher reports it as
// unassessed.
func (c *extensionCollector) observe(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	c.read = true
	identity, err := vulnerability.Collect(ctx, conn)
	if err != nil {
		return err
	}
	c.identity = identity
	if identity.Product != "postgres" || identity.ScanCapability != "supported" {
		return nil
	}
	c.catalog, err = vulnerability.ReadExtensionCatalog(ctx, conn)
	return err
}

// assess matches what each request installs against the advisory data opts
// selects, with where that data came from, and returns nil when the replay
// never reached the observer. With no request it reads no advisory data.
func (c *extensionCollector) assess(ctx context.Context, opts Options) (*lint.ExtensionAssessments, vulnerability.Origin, error) {
	if c == nil || !c.read {
		return nil, vulnerability.Origin{}, nil
	}
	assessments := &lint.ExtensionAssessments{Server: vulnerability.ServerName(c.identity), Held: c.held}
	if len(c.requests) == 0 {
		return assessments, vulnerability.Origin{}, nil
	}
	matcher, err := vulnerability.NewPostgresMatcher(ctx, vulnerability.ScanInput{
		Snapshots:     opts.Advisories,
		RequireSource: opts.RequireAdvisorySource,
		Now:           time.Now().UTC(),
		Deployment:    "self-managed",
	})
	if err != nil {
		return nil, vulnerability.Origin{}, err
	}
	for _, request := range c.requests {
		answer := lint.ExtensionAnswer{Request: request}
		installs, _ := c.catalog.Installs(vulnerability.ExtensionStatement{Name: request.Extension, Version: request.Version, Cascade: request.Cascade})
		for _, install := range installs {
			inventory := c.identity
			inventory.Extensions = map[string]string{install.Name: install.Version}
			found := matcher.Match(inventory).ForExtension(install.Name)
			installed := lint.InstalledExtension{Extension: install.Name, Version: install.Version, Unassessed: found.Unassessed}
			for _, assessment := range found.Affected {
				installed.Advisories = append(installed.Advisories, lint.ExtensionAdvisory{
					ID: assessment.ID, Summary: assessment.Description, FixedIn: assessment.FixedVersion, URL: assessment.SourceURL,
				})
			}
			answer.Installs = append(answer.Installs, installed)
		}
		assessments.Answers = append(assessments.Answers, answer)
	}
	return assessments, matcher.Origin(), nil
}

// versionObservers runs each version observer that is set, in order, as the
// one hook the replay takes, or returns nil when none is.
func versionObservers(
	observers ...func(context.Context, *migrator.Migration, *dbschema.DatabaseConnection) error,
) func(context.Context, *migrator.Migration, *dbschema.DatabaseConnection) error {
	var set []func(context.Context, *migrator.Migration, *dbschema.DatabaseConnection) error
	for _, observe := range observers {
		if observe != nil {
			set = append(set, observe)
		}
	}
	if len(set) == 0 {
		return nil
	}
	return func(ctx context.Context, migration *migrator.Migration, conn *dbschema.DatabaseConnection) error {
		for _, observe := range set {
			if err := observe(ctx, migration, conn); err != nil {
				return err
			}
		}
		return nil
	}
}

// replayedObservers runs each observer that is set, in order, as the one hook
// the replay takes, or returns nil when none is.
func replayedObservers(observers ...func(*dbschema.DatabaseConnection) error) func(*dbschema.DatabaseConnection) error {
	var set []func(*dbschema.DatabaseConnection) error
	for _, observe := range observers {
		if observe != nil {
			set = append(set, observe)
		}
	}
	if len(set) == 0 {
		return nil
	}
	return func(conn *dbschema.DatabaseConnection) error {
		for _, observe := range set {
			if err := observe(conn); err != nil {
				return err
			}
		}
		return nil
	}
}

// WriteAdvisorySourceNotice writes why the advisory source the extension rules
// read was not used and which snapshot they matched instead, the line `ptah db
// vulnerabilities` writes for the same condition. It writes nothing for a run
// that matched no advisory data or read the source it asked. A run that named
// its source reports each extension as unassessed as well; a default run
// reads the fallback as its data, and this line is how it says so.
func WriteAdvisorySourceNotice(w io.Writer, report Report) error {
	_, err := io.WriteString(w, vulnerability.FallbackWarning(report.AdvisoryOrigin, "postgres"))
	return err
}
