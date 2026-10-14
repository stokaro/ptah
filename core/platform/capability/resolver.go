package capability

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// VersionScheme says how a server version is read out of the string the server
// reports. A descriptor's Versions.Scheme spells one of these values.
//
// Each scheme is a grammar of version strings, not an engine: an engine whose
// string reads like another's names the same scheme.
type VersionScheme string

const (
	// VersionDotted reads the first dotted number in the string, so
	// "PostgreSQL 16.3 (Debian ...)" reads as 16.3 and "8.0.42-log" as 8.0.42.
	VersionDotted VersionScheme = "dotted"
	// VersionAfterDash reads the number after " - " when the string has one,
	// and the first dotted number otherwise. It is for a banner that opens with
	// a marketing year: "Microsoft SQL Server 2025 (RTM-CU7) ... -
	// 17.0.4065.4" reads as 17.0.4065.
	VersionAfterDash VersionScheme = "after-dash"
	// VersionAfterRelease reads the number after the word "release", ignoring
	// case, when the string has it, and the first dotted number otherwise. It
	// is for a banner whose first number is not the version: "Oracle AI
	// Database 26ai Free Release 23.26.2.0.0" reads as 23.26.2.
	VersionAfterRelease VersionScheme = "after-release"
	// VersionReplicationPrefix reads the first dotted number after a leading
	// "5.5.5-", the replication-compatibility prefix a server can report over
	// the MySQL protocol: "5.5.5-10.11.6-MariaDB" reads as 10.11.6.
	VersionReplicationPrefix VersionScheme = "replication-prefix"
	// VersionYBRelease reads the number after "-YB-", ignoring case, when the
	// string has it, and the first dotted number otherwise; and it reads
	// nothing unless the major is a release year, 2024 or later. The banner
	// opens with the version of the PostgreSQL engine underneath,
	// "PostgreSQL 11.2-YB-2024.2.4.0-b0", and a number below the floor is
	// that version rather than a release.
	VersionYBRelease VersionScheme = "yb-release"
	// VersionReleaseBranch reads a dotted release that opens the string,
	// "26.2.1.14", or a build branch, "stable-25-4-1" for 25.4.1. It reads
	// nothing out of a string that opens with neither, as a floating build
	// such as "trunk" does.
	VersionReleaseBranch VersionScheme = "release-branch"
)

// VersionSchemes returns every declared version scheme.
func VersionSchemes() []VersionScheme {
	return []VersionScheme{
		VersionDotted, VersionAfterDash, VersionAfterRelease,
		VersionReplicationPrefix, VersionYBRelease, VersionReleaseBranch,
	}
}

// VersionAttribution says how a server version is credited to a measured
// release line, which is what [VersionResolution.VersionSpecific] and
// [VersionResolution.Saturated] report. A descriptor's Versions.Attribution
// spells one of these values.
type VersionAttribution string

const (
	// AttributeMeasuredLines credits a version to a line when its major and
	// minor are those of a measured line. A version newer than the newest
	// measured line is past the top of the ladder, and a gap between two
	// measured lines is neither credited nor past the top.
	AttributeMeasuredLines VersionAttribution = "measured-lines"
	// AttributeThroughNewest credits every version up to and including the
	// newest measured major, and a newer major is past the top. The newest
	// line is reported as "<major>.x". With no newest line, every version is
	// credited and none is past the top.
	AttributeThroughNewest VersionAttribution = "through-newest"
)

// VersionAttributions returns every declared version attribution.
func VersionAttributions() []VersionAttribution {
	return []VersionAttribution{AttributeMeasuredLines, AttributeThroughNewest}
}

// Resolver answers the dialect questions of this package from one registry
// of dialect descriptors: which preset a dialect selects, which ladder a
// server version climbs, which product a version string names, and how long
// an identifier may be. Every answer is read out of the descriptors; nothing
// in the resolver names an engine.
//
// The package-level functions answer from a resolver over the process
// registry, platform.Dialects. Build one with [NewResolver] to answer from a
// registry of your own, such as one that describes an engine Ptah does not
// ship. A Resolver is immutable and safe for concurrent use.
type Resolver struct {
	registry *descriptor.Registry
	// dialects holds the rules of each descriptor, in registry order.
	dialects []*dialectRules
	// banners holds the same rules in the order banner tokens are tried.
	banners []*dialectRules
	byName  map[string]*dialectRules
}

// NewResolver reads the capability rules of every descriptor in registry. It
// refuses, with [descriptor.ErrInvalidDescriptor], a descriptor that names a
// preset [NamedPresets] does not list, spells a scheme, attribution or
// identifier unit this package does not declare, states a version that is not
// one, orders its ladder steps other than strictly oldest first, or leaves a
// banner token or ladder with no default preset. A nil registry answers like
// an empty one: every dialect is unknown.
func NewResolver(registry *descriptor.Registry) (*Resolver, error) {
	resolver := &Resolver{registry: registry, byName: make(map[string]*dialectRules)}
	for _, described := range registry.Dialects() {
		rules, err := readRules(described)
		if err != nil {
			return nil, err
		}
		resolver.add(rules)
	}
	return resolver.orderBanners(), nil
}

// lenientResolver reads the rules of every descriptor in registry, and treats
// one it cannot read as a dialect that states no rules. The process registry
// takes descriptors from platform.Register, and one invalid descriptor there
// must not take every other dialect's answers with it; the conservative
// answers for that one dialect are what a dialect with no rules receives.
func lenientResolver(registry *descriptor.Registry) *Resolver {
	resolver := &Resolver{registry: registry, byName: make(map[string]*dialectRules)}
	for _, described := range registry.Dialects() {
		if rules, err := readRules(described); err == nil {
			resolver.add(rules)
		}
	}
	return resolver.orderBanners()
}

func (r *Resolver) add(rules *dialectRules) {
	r.dialects = append(r.dialects, rules)
	r.byName[rules.name] = rules
}

// processResolver caches the resolver over the current process registry. A
// registry never changes once it is made, and platform.Register replaces it
// with a new one, so the pointer says whether the cache is current.
var processResolver atomic.Pointer[Resolver]

// process returns the resolver over the process registry.
func process() *Resolver {
	registry := platform.Dialects()
	if cached := processResolver.Load(); cached != nil && cached.registry == registry {
		return cached
	}
	built := lenientResolver(registry)
	processResolver.Store(built)
	return built
}

// lookup returns the rules of the dialect a spelling selects, or nil for a
// spelling no descriptor claims and for a dialect that states none.
func (r *Resolver) lookup(dialect string) *dialectRules {
	return r.byName[r.registry.Normalize(dialect)]
}

// ForDialect returns the default preset a dialect name selects, as
// [ForDialect] does, from this resolver's registry.
func (r *Resolver) ForDialect(dialect string) Capabilities {
	if rules := r.lookup(dialect); rules != nil {
		return rules.preset
	}
	return Capabilities{}
}

// Dialects returns the names of the dialects that select a default preset,
// sorted.
func (r *Resolver) Dialects() []string {
	var names []string
	for _, rules := range r.dialects {
		if rules.hasPreset {
			names = append(names, rules.name)
		}
	}
	slices.Sort(names)
	return names
}

// Identifiers returns the identifier limit a dialect's descriptor states, as
// [Identifiers] does, from this resolver's registry.
func (r *Resolver) Identifiers(dialect string) IdentifierLimit {
	if rules := r.lookup(dialect); rules != nil {
		return rules.limit
	}
	return IdentifierLimit{}
}

// BannerPlatform reports the dialect a version string names, as
// [BannerPlatform] does, from this resolver's registry.
//
// A dialect names itself through its descriptor's banner tokens. A product's
// banner may also carry a token of the head of its family: YugabyteDB reports
// "PostgreSQL 15.12-YB-...", and Spanner "Cloud Spanner PostgreSQL". So
// families are tried in the registry order of their heads, and inside a
// family the members before the head; the first dialect with a token in the
// string is the answer. A string naming products of two families is answered
// by the family registered first, and no server's banner does that.
func (r *Resolver) BannerPlatform(version string) string {
	if named := r.banner(version); named != nil {
		return named.name
	}
	return ""
}

func (r *Resolver) banner(version string) *dialectRules {
	lower := strings.ToLower(version)
	for _, rules := range r.banners {
		if slices.ContainsFunc(rules.banner, func(token string) bool { return strings.Contains(lower, token) }) {
			return rules
		}
	}
	return nil
}

// orderBanners records the order banner tokens are tried in. Families are
// tried in the registry order of their heads, a dialect with no family being
// one of its own; inside a family, the members come before the head, each in
// registry order.
func (r *Resolver) orderBanners() *Resolver {
	position := make(map[string]int, len(r.dialects))
	for index, rules := range r.dialects {
		position[rules.name] = index
	}
	group := func(rules *dialectRules) int {
		if head, found := position[rules.family]; found {
			return head
		}
		return position[rules.name]
	}
	// A member ranks before the head of its family.
	rank := func(rules *dialectRules) int {
		if rules.member() {
			return 0
		}
		return 1
	}
	r.banners = slices.Clone(r.dialects)
	slices.SortStableFunc(r.banners, func(left, right *dialectRules) int {
		return cmp.Or(group(left)-group(right), rank(left)-rank(right))
	})
	return r
}

// ResolveServerVersion resolves a server version string onto a preset, as
// [ResolveServerVersion] does, from this resolver's registry.
//
// A product the string names outranks the declared dialect, and the answer is
// read off that product's ladder. A product with no ladder has one preset, and
// the string that named it selected that preset exactly.
//
// The exception is a string naming only the head of the declared dialect's
// family, such as "PostgreSQL 16.3" for a CockroachDB connection. The head's
// name is the family's, and a member may report a banner carrying no token of
// its own, so the string is a less specific spelling of the declared server
// and the declared dialect's ladder decides. Measured, claiming such a banner
// for the head turned SpannerPostgres into Postgres14 across 19 keys, among
// them the keys that stop Ptah emitting DDL Spanner refuses.
//
// A string naming no product climbs the declared dialect's ladder. A string
// with no number in it at all receives the declared dialect's default preset,
// and is not recognized.
func (r *Resolver) ResolveServerVersion(dialect, version string) VersionResolution {
	declared := r.lookup(dialect)
	if named := r.banner(version); named != nil && (declared == nil || declared.family != named.name) {
		resolution := named.resolveBanner(version)
		resolution.ResolvedDialect = named.name
		return resolution
	}

	normalized := r.registry.Normalize(dialect)
	var preset Capabilities
	if declared != nil {
		preset = declared.preset
	}
	if _, ok := parseVersion(version); !ok {
		// Recognized stays false: the preset is the default, picked without
		// reading anything out of the string.
		return VersionResolution{Capabilities: preset, ResolvedDialect: normalized}
	}
	if declared == nil || declared.ladder == nil {
		// The version parsed; this dialect simply has no ladder to spend it
		// on. That is not the operator's mistake, so it is recognized.
		return VersionResolution{Capabilities: preset, Recognized: true, ResolvedDialect: normalized}
	}
	resolution := declared.ladder.resolve(version)
	resolution.ResolvedDialect = normalized
	return resolution
}

// dialectRules is what one descriptor states for this package, read and
// checked.
type dialectRules struct {
	name   string
	family string
	// preset is the default preset, and hasPreset whether the descriptor
	// names one.
	preset    Capabilities
	hasPreset bool
	banner    []string
	// ladder is nil for a dialect with no version ladder.
	ladder *versionLadder
	limit  IdentifierLimit
}

// member reports whether the dialect belongs to another dialect's family.
func (d *dialectRules) member() bool {
	return d.family != "" && d.family != d.name
}

// resolveBanner answers a version string that names this dialect's product.
// A product with no ladder has one preset, and the banner selected it.
func (d *dialectRules) resolveBanner(version string) VersionResolution {
	if d.ladder == nil {
		return VersionResolution{Capabilities: d.preset, VersionSpecific: true, Recognized: true}
	}
	return d.ladder.resolve(version)
}

type versionLadder struct {
	scheme      VersionScheme
	floor       Capabilities
	steps       []ladderStep
	attribution VersionAttribution
	measured    []serverVersion
	// newest is the newest measured line as the descriptor states it, and
	// newestVersion its parsed form; both are zero when no line is newest.
	newest        string
	newestVersion serverVersion
	unreadable    Capabilities
	// unreadableRecognized and reportNewest shape the answer for a string
	// the scheme cannot read; see descriptor.UnreadableVersion.
	unreadableRecognized bool
	reportNewest         bool
}

type ladderStep struct {
	min    serverVersion
	preset Capabilities
}

// resolve reads a version out of the string and climbs the ladder.
func (l *versionLadder) resolve(version string) VersionResolution {
	v, ok := l.scheme.read(version)
	if !ok {
		resolution := VersionResolution{Capabilities: l.unreadable, Recognized: l.unreadableRecognized}
		if l.reportNewest {
			resolution.NewestMeasured = l.newestLabel()
		}
		return resolution
	}
	resolution := VersionResolution{Capabilities: l.pick(v), Recognized: true, NewestMeasured: l.newestLabel()}
	switch l.attribution {
	case AttributeMeasuredLines:
		resolution.VersionSpecific = slices.ContainsFunc(l.measured, func(line serverVersion) bool {
			return sameMajorMinor(v, line)
		})
		resolution.Saturated = compareServerVersion(v, l.newestVersion) > 0
	case AttributeThroughNewest:
		resolution.Saturated = l.newest != "" && v.major > l.newestVersion.major
		resolution.VersionSpecific = !resolution.Saturated
	}
	return resolution
}

// pick returns the preset of the newest step at or below v.
func (l *versionLadder) pick(v serverVersion) Capabilities {
	selected := l.floor
	for _, step := range l.steps {
		if compareFullVersion(v, step.min) >= 0 {
			selected = step.preset
		}
	}
	return selected
}

// newestLabel is the newest measured line as VersionResolution reports it.
func (l *versionLadder) newestLabel() string {
	if l.newest == "" || l.attribution != AttributeThroughNewest {
		return l.newest
	}
	return strconv.Itoa(l.newestVersion.major) + ".x"
}

// read reads a version out of a string according to the scheme.
func (s VersionScheme) read(version string) (serverVersion, bool) {
	switch s {
	case VersionDotted:
		return parseVersion(version)
	case VersionAfterDash:
		if _, after, found := strings.Cut(version, " - "); found {
			return parseVersion(after)
		}
		return parseVersion(version)
	case VersionAfterRelease:
		const marker = "release "
		if index := strings.Index(strings.ToLower(version), marker); index >= 0 {
			return parseVersion(version[index+len(marker):])
		}
		return parseVersion(version)
	case VersionReplicationPrefix:
		return parseVersion(strings.TrimPrefix(version, replicationPrefix))
	case VersionYBRelease:
		text := version
		if _, after, found := strings.Cut(strings.ToUpper(version), "-YB-"); found {
			text = after
		}
		v, ok := parseVersion(text)
		return v, ok && v.major >= firstYBReleaseYear
	case VersionReleaseBranch:
		return releaseOrBranch(version)
	default:
		return serverVersion{}, false
	}
}

// replicationPrefix is the fake version prefix a server reports over the
// MySQL protocol for replication compatibility ("5.5.5-10.11.6-MariaDB").
const replicationPrefix = "5.5.5-"

// firstYBReleaseYear is the first release numbered by year. Every number
// below it in a banner is the version of the PostgreSQL engine underneath.
const firstYBReleaseYear = 2024

// releaseOrBranch reads the release out of a string that opens with it,
// either dotted or as a build branch `stable-<major>-<minor>[-<patch>]`. The
// dotted parse alone would read the branch "stable-25-4-1" as 25.0.
func releaseOrBranch(version string) (serverVersion, bool) {
	trimmed := strings.TrimSpace(version)
	if rest, ok := strings.CutPrefix(strings.ToLower(trimmed), "stable-"); ok {
		parts := strings.Split(rest, "-")
		if len(parts) < 2 || len(parts) > 3 {
			return serverVersion{}, false
		}
		numbers := make([]int, 3)
		for i, part := range parts {
			number, err := strconv.Atoi(part)
			if err != nil || number < 0 {
				return serverVersion{}, false
			}
			numbers[i] = number
		}
		return serverVersion{major: numbers[0], minor: numbers[1], patch: numbers[2]}, true
	}
	if trimmed == "" || trimmed[0] < '0' || trimmed[0] > '9' {
		return serverVersion{}, false
	}
	return parseVersion(trimmed)
}

// shippedPresets maps every preset this package ships to its name, which is
// how a descriptor names it.
var shippedPresets = sync.OnceValue(func() map[string]Capabilities {
	named := NamedPresets()
	byName := make(map[string]Capabilities, len(named))
	for _, preset := range named {
		byName[preset.Name] = preset.Capabilities
	}
	return byName
})

// readRules reads and checks what one descriptor states for this package.
func readRules(described descriptor.Dialect) (*dialectRules, error) {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: dialect %q: %s", descriptor.ErrInvalidDescriptor, described.Name, fmt.Sprintf(format, args...))
	}
	stated := described.Capabilities
	rules := &dialectRules{name: described.Name, family: described.Family}

	if stated.DefaultPreset != "" {
		preset, err := namedPreset(stated.DefaultPreset)
		if err != nil {
			return nil, invalid("default preset: %v", err)
		}
		rules.preset, rules.hasPreset = preset, true
	}
	for _, token := range stated.BannerTokens {
		if token == "" || token != strings.ToLower(token) {
			return nil, invalid("banner token %q must be non-empty lowercase text", token)
		}
	}
	if len(stated.BannerTokens) > 0 && !rules.hasPreset {
		return nil, invalid("banner tokens name a product with no default preset")
	}
	rules.banner = slices.Clone(stated.BannerTokens)

	ladder, err := readLadder(stated.Versions, rules.preset)
	if err != nil {
		return nil, invalid("versions: %v", err)
	}
	if ladder != nil && !rules.hasPreset {
		return nil, invalid("a version ladder needs a default preset")
	}
	rules.ladder = ladder

	limit, err := readIdentifierLimit(described.IdentifierLimit)
	if err != nil {
		return nil, invalid("identifier limit: %v", err)
	}
	rules.limit = limit
	return rules, nil
}

// readLadder reads a version ladder, or returns nil for none.
func readLadder(stated descriptor.Versions, defaultPreset Capabilities) (*versionLadder, error) {
	if stated.Scheme == "" {
		if stated.Floor != "" || len(stated.Steps) > 0 || stated.Attribution != "" || len(stated.Measured) > 0 ||
			stated.Newest != "" || stated.Unreadable != (descriptor.UnreadableVersion{}) {
			return nil, errors.New("a ladder with no scheme must state nothing else")
		}
		return nil, nil
	}
	ladder := &versionLadder{
		scheme:               VersionScheme(stated.Scheme),
		attribution:          VersionAttribution(stated.Attribution),
		newest:               stated.Newest,
		unreadable:           defaultPreset,
		unreadableRecognized: stated.Unreadable.Recognized,
		reportNewest:         stated.Unreadable.ReportNewest,
	}
	if !slices.Contains(VersionSchemes(), ladder.scheme) {
		return nil, fmt.Errorf("scheme %q is not one of %q", stated.Scheme, VersionSchemes())
	}
	if !slices.Contains(VersionAttributions(), ladder.attribution) {
		return nil, fmt.Errorf("attribution %q is not one of %q", stated.Attribution, VersionAttributions())
	}
	floor, err := namedPreset(stated.Floor)
	if err != nil {
		return nil, fmt.Errorf("floor: %w", err)
	}
	ladder.floor = floor
	for i, step := range stated.Steps {
		minimum, ok := parseLine(step.Min, 3)
		if !ok {
			return nil, fmt.Errorf("step %d: %q is not a version of one to three numbers", i, step.Min)
		}
		if i > 0 && compareFullVersion(minimum, ladder.steps[i-1].min) <= 0 {
			return nil, fmt.Errorf("step %d: %q does not follow %q", i, step.Min, stated.Steps[i-1].Min)
		}
		preset, err := namedPreset(step.Preset)
		if err != nil {
			return nil, fmt.Errorf("step %d: %w", i, err)
		}
		ladder.steps = append(ladder.steps, ladderStep{min: minimum, preset: preset})
	}
	if stated.Unreadable.Preset != "" {
		unreadable, err := namedPreset(stated.Unreadable.Preset)
		if err != nil {
			return nil, fmt.Errorf("unreadable: %w", err)
		}
		ladder.unreadable = unreadable
	}
	return ladder, readAttribution(stated, ladder)
}

// readAttribution reads the measured lines the attribution needs.
func readAttribution(stated descriptor.Versions, ladder *versionLadder) error {
	switch ladder.attribution {
	case AttributeMeasuredLines:
		for _, line := range stated.Measured {
			measured, ok := parseLine(line, 2)
			if !ok {
				return fmt.Errorf("measured line %q is not major.minor", line)
			}
			ladder.measured = append(ladder.measured, measured)
		}
		newest, ok := parseLine(stated.Newest, 2)
		if !ok || !slices.Contains(stated.Measured, stated.Newest) {
			return fmt.Errorf("newest line %q is not one of the measured lines %q", stated.Newest, stated.Measured)
		}
		ladder.newestVersion = newest
	case AttributeThroughNewest:
		if len(stated.Measured) > 0 {
			return fmt.Errorf("attribution %q reads no measured lines, and %q are stated", ladder.attribution, stated.Measured)
		}
		if stated.Newest == "" {
			return nil
		}
		newest, ok := parseLine(stated.Newest, 1)
		if !ok {
			return fmt.Errorf("newest line %q is not a major version", stated.Newest)
		}
		ladder.newestVersion = newest
	}
	return nil
}

// readIdentifierLimit reads a stated identifier limit.
func readIdentifierLimit(stated descriptor.IdentifierLimit) (IdentifierLimit, error) {
	limit := IdentifierLimit{Max: stated.Max, Unit: IdentifierUnit(stated.Unit)}
	switch {
	case stated.Max == 0 && stated.Unit == "":
		return limit, nil
	case stated.Max <= 0:
		return IdentifierLimit{}, fmt.Errorf("max %d is not a positive length", stated.Max)
	case limit.Unit != IdentifierBytes && limit.Unit != IdentifierCharacters:
		return IdentifierLimit{}, fmt.Errorf("unit %q is not %q or %q", stated.Unit, IdentifierBytes, IdentifierCharacters)
	default:
		return limit, nil
	}
}

// namedPreset returns the shipped preset a descriptor names.
func namedPreset(name string) (Capabilities, error) {
	preset, found := shippedPresets()[name]
	if !found {
		return Capabilities{}, fmt.Errorf("preset %q is not one this package ships", name)
	}
	return preset, nil
}

// parseLine reads a version a descriptor states: one to most dot-separated
// decimal numbers and nothing else.
func parseLine(line string, most int) (serverVersion, bool) {
	parts := strings.Split(line, ".")
	if len(parts) > most {
		return serverVersion{}, false
	}
	var numbers [3]int
	for i, part := range parts {
		if part == "" || strings.Trim(part, "0123456789") != "" {
			return serverVersion{}, false
		}
		number, err := strconv.Atoi(part)
		if err != nil {
			return serverVersion{}, false
		}
		numbers[i] = number
	}
	return serverVersion{major: numbers[0], minor: numbers[1], patch: numbers[2]}, true
}

// compareFullVersion orders two versions by major, minor and patch.
func compareFullVersion(left, right serverVersion) int {
	if order := compareServerVersion(left, right); order != 0 {
		return order
	}
	return left.patch - right.patch
}
