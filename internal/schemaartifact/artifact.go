// Package schemaartifact maps Ptah desired-schema IR to OCI artifacts.
package schemaartifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/fsnapshot"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/pathguard"
)

const (
	// FileName is the canonical schema layer name.
	FileName = "schema.hcl"
	// LayerMediaType identifies canonical Ptah schema HCL layers.
	LayerMediaType = "application/vnd.stokaro.ptah.schema.hcl.v1"
	// ManagedDataFileName is the declared-rows layer name, present only in an
	// artifact whose schema declares managed data.
	ManagedDataFileName = "managed-data.json"
	// ManagedDataLayerMediaType identifies the declared-rows layer. The version
	// suffix is the format version: a reader that does not know this media type
	// refuses the artifact rather than reading the schema layer and publishing a
	// database without the rows its author declared.
	ManagedDataLayerMediaType = "application/vnd.stokaro.ptah.managed-data.v1+json"
	// ChecksFileName is the release-assertion layer name, present only in an
	// artifact published with checks.
	ChecksFileName = "checks.sql"
	// ChecksLayerMediaType identifies the release-assertion layer.
	//
	// The version suffix carries the same weight it does above: a reader that
	// does not know this media type refuses the artifact rather than pulling
	// the schema and verifying nothing, which is the failure this layer exists
	// to design out -- checks approved at review time and different checks
	// evaluated afterwards.
	ChecksLayerMediaType = "application/vnd.stokaro.ptah.checks.v1+sql"

	annotationFormat = "io.stokaro.ptah.schema-format"
	canonicalFormat  = "hcl"
)

// layerMediaTypes names the media type of every layer this package writes that
// is not canonical schema HCL, and acceptedLayerMediaTypes is the whole set a
// pull admits. A layer outside the set is refused by the reader.
var (
	layerMediaTypes = map[string]string{
		ManagedDataFileName: ManagedDataLayerMediaType,
		ChecksFileName:      ChecksLayerMediaType,
	}
	acceptedLayerMediaTypes = []string{
		LayerMediaType, ManagedDataLayerMediaType, ChecksLayerMediaType,
	}
)

// PushOptions controls schema artifact tags and metadata.
type PushOptions struct {
	Tags        []string
	Version     string
	PlainHTTP   bool
	Now         func() time.Time
	Annotations map[string]string
	// Latest moves the latest alias onto this push, and GeneratedVersion
	// writes a timestamped version tag when Version names none. Both are
	// opt-in: a publish and an alias move are two operations, and doing both
	// by default makes every publish a promotion nobody asked for.
	Latest           bool
	GeneratedVersion bool
	// Checks is the release-assertion source published beside the schema,
	// empty when the artifact carries none.
	//
	// It is bytes the caller supplies rather than a field of the schema model,
	// deliberately. The model is walked by every comparator, planner and
	// renderer, and a checks field there would be traversed by all of them to
	// serve one publisher and one reader. What the artifact needs is that the
	// approved checks and the evaluated checks are the same bytes, which a
	// layer gives without the model knowing they exist (stokaro/ptah#3458).
	Checks []byte
}

// PushResult describes a published immutable schema artifact.
type PushResult struct {
	ociartifact.PushResult
	Version string
}

// Artifact is a validated canonical schema retrieved from OCI storage.
//
// Database carries the declared rows of the managed-data layer in its
// ManagedData entries when the artifact has one. Their File and SourceDir are
// empty: an artifact that travels carries rows, not a path into the working
// copy that published them.
type Artifact struct {
	Database   *schemamodel.Database
	FileSystem fs.FS
	// Checks is the release-assertion source the artifact carries, nil when it
	// carries none. It is the bytes that were published, so an evaluation that
	// reads them here is evaluating what was approved.
	Checks     []byte
	Descriptor ocispec.Descriptor
	Reference  ociartifact.Reference
}

type preparedPush struct {
	FileSystem  fs.FS
	Tags        []string
	Version     string
	Annotations map[string]string
}

// Capture renders db into a lossless canonical HCL snapshot, with the layers
// the artifact carries beside it.
//
// checks is the release-assertion source, empty for an artifact that carries
// none. It is taken here rather than read from db for the reason
// [PushOptions.Checks] gives.
func Capture(db *schemamodel.Database, checks []byte) (fs.FS, error) {
	if db == nil {
		return nil, fmt.Errorf("schema database is required")
	}
	var err error
	for _, role := range db.Roles {
		if role.Password != "" {
			return nil, fmt.Errorf("schema artifact cannot contain password for role %q", role.Name)
		}
	}
	var managed []byte
	if len(db.ManagedData) > 0 {
		managed, err = encodeManagedData(db)
		if err != nil {
			return nil, err
		}
	}
	rendered, err := atlashclrender.Render(db)
	if err != nil {
		return nil, fmt.Errorf("render canonical schema HCL: %w", err)
	}
	if len(rendered.Diagnostics) > 0 {
		return nil, fmt.Errorf(
			"schema artifact cannot be rendered without loss:\n%s",
			formatDiagnostics(rendered.Diagnostics),
		)
	}
	parsed, err := atlashcl.Parse(rendered.Data, FileName)
	if err != nil {
		return nil, fmt.Errorf("validate canonical schema HCL: %w", err)
	}
	roundTrip, err := atlashclrender.Render(parsed)
	if err != nil {
		return nil, fmt.Errorf("re-render canonical schema HCL: %w", err)
	}
	if len(roundTrip.Diagnostics) > 0 || !bytes.Equal(roundTrip.Data, rendered.Data) {
		return nil, fmt.Errorf("canonical schema HCL is not stable after parsing")
	}
	files := map[string][]byte{FileName: rendered.Data}
	if managed != nil {
		files[ManagedDataFileName] = managed
	}
	if len(checks) > 0 {
		files[ChecksFileName] = checks
	}
	snapshot, err := fsnapshot.FromFiles(files)
	if err != nil {
		return nil, fmt.Errorf("build schema artifact filesystem: %w", err)
	}
	return snapshot, nil
}

// Push publishes db to the repository named by reference.
func Push(
	ctx context.Context,
	reference string,
	db *schemamodel.Database,
	opts PushOptions,
) (PushResult, error) {
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.PlainHTTP})
	if err != nil {
		return PushResult{}, err
	}
	return push(ctx, client, reference, db, opts)
}

// PushTo publishes db to target for integration and transport tests.
func PushTo(
	ctx context.Context,
	target oras.Target,
	db *schemamodel.Database,
	opts PushOptions,
) (PushResult, error) {
	prepared, err := prepare(db, opts)
	if err != nil {
		return PushResult{}, err
	}
	result, err := ociartifact.PushTo(ctx, target, prepared.FileSystem, ociartifact.PushOptions{
		ArtifactType:    ociartifact.SchemaArtifactType,
		LayerMediaType:  LayerMediaType,
		LayerMediaTypes: layerMediaTypes,
		Tags:            prepared.Tags,
		WriteOnceTags:   prepared.writeOnceTags(),
		Annotations:     prepared.Annotations,
	})
	if err != nil {
		return PushResult{}, err
	}
	return PushResult{PushResult: result, Version: prepared.Version}, nil
}

func push(
	ctx context.Context,
	client *ociartifact.Client,
	reference string,
	db *schemamodel.Database,
	opts PushOptions,
) (PushResult, error) {
	prepared, err := prepare(db, opts)
	if err != nil {
		return PushResult{}, err
	}
	result, err := client.Push(ctx, reference, prepared.FileSystem, ociartifact.PushOptions{
		ArtifactType:    ociartifact.SchemaArtifactType,
		LayerMediaType:  LayerMediaType,
		LayerMediaTypes: layerMediaTypes,
		Tags:            prepared.Tags,
		WriteOnceTags:   prepared.writeOnceTags(),
		Annotations:     prepared.Annotations,
	})
	if err != nil {
		return PushResult{}, err
	}
	return PushResult{PushResult: result, Version: prepared.Version}, nil
}

// Pull retrieves and validates a canonical schema through client.
func Pull(ctx context.Context, client *ociartifact.Client, reference string) (Artifact, error) {
	if client == nil {
		return Artifact{}, fmt.Errorf("OCI client is required")
	}
	pulled, err := client.Pull(ctx, reference, ociartifact.PullOptions{
		ExpectedArtifactTypes:   []string{ociartifact.SchemaArtifactType},
		AcceptedLayerMediaTypes: acceptedLayerMediaTypes,
	})
	if err != nil {
		return Artifact{}, err
	}
	return validatePulled(pulled)
}

// PullFrom retrieves and validates a canonical schema from target.
func PullFrom(ctx context.Context, target oras.ReadOnlyTarget, selector string) (Artifact, error) {
	pulled, err := ociartifact.PullFrom(ctx, target, selector, ociartifact.PullOptions{
		ExpectedArtifactTypes:   []string{ociartifact.SchemaArtifactType},
		AcceptedLayerMediaTypes: acceptedLayerMediaTypes,
	})
	if err != nil {
		return Artifact{}, err
	}
	return validatePulled(pulled)
}

// PullToFile retrieves reference and materializes it at output. It returns the
// paths it created, the canonical HCL first.
func PullToFile(ctx context.Context, reference, output string, plainHTTP bool) (Artifact, []string, error) {
	if strings.TrimSpace(output) == "" {
		return Artifact{}, nil, fmt.Errorf("schema artifact output file is required")
	}
	resolved, err := pathguard.ResolveCLIPath(output)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("resolve schema artifact output: %w", err)
	}
	if _, err := os.Lstat(resolved); err == nil {
		return Artifact{}, nil, fmt.Errorf("schema artifact output already exists: %s", resolved)
	} else if !os.IsNotExist(err) {
		return Artifact{}, nil, fmt.Errorf("stat schema artifact output: %w", err)
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: plainHTTP})
	if err != nil {
		return Artifact{}, nil, err
	}
	artifact, err := Pull(ctx, client, reference)
	if err != nil {
		return Artifact{}, nil, err
	}
	written, err := Materialize(artifact, resolved)
	if err != nil {
		return Artifact{}, nil, err
	}
	return artifact, written, nil
}

// Materialize writes the files of artifact to disk: the canonical HCL at
// output, and, when the artifact declares rows, the managed-data layer beside it
// under its canonical name. It returns the paths it created, output first.
//
// The rows are written because an artifact that declares them is not the schema
// without them. A materialization that wrote the HCL alone would hand a consumer
// a `data` block whose `file` names a path in the working copy that published
// the artifact, which exists nowhere else -- so the consumer would plan a schema
// with the rows silently missing, or refuse with a message about a file it was
// never going to find (stokaro/ptah#3256).
//
// Neither file survives a failure to write the other. A canonical HCL beside a
// half-written row layer is the one outcome a reader cannot tell from an
// artifact that declares no rows at all.
func Materialize(artifact Artifact, output string) ([]string, error) {
	contents, err := fs.ReadFile(artifact.FileSystem, FileName)
	if err != nil {
		return nil, fmt.Errorf("read canonical schema artifact: %w", err)
	}
	managed, err := fs.ReadFile(artifact.FileSystem, ManagedDataFileName)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read managed data layer: %w", err)
	}
	if err := writeExclusive(output, contents); err != nil {
		return nil, err
	}
	if managed == nil {
		return []string{output}, nil
	}
	rows := filepath.Join(filepath.Dir(output), ManagedDataFileName)
	if err := writeExclusive(rows, managed); err != nil {
		_ = os.Remove(output)
		return nil, err
	}
	return []string{output, rows}, nil
}

func prepare(
	db *schemamodel.Database,
	opts PushOptions,
) (preparedPush, error) {
	snapshot, err := Capture(db, opts.Checks)
	if err != nil {
		return preparedPush{}, err
	}
	version := opts.Version
	if version == "" && opts.GeneratedVersion {
		now := opts.Now
		if now == nil {
			now = time.Now
		}
		version = ociartifact.VersionTag(now())
	}
	tags := append(make([]string, 0), opts.Tags...)
	if opts.Latest {
		tags = append(tags, ociartifact.DefaultTag)
	}
	annotations := maps.Clone(opts.Annotations)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotationFormat] = canonicalFormat
	return preparedPush{
		FileSystem:  snapshot,
		Tags:        tags,
		Version:     version,
		Annotations: annotations,
	}, nil
}

func validatePulled(pulled ociartifact.Artifact) (Artifact, error) {
	if pulled.Annotations[annotationFormat] != canonicalFormat {
		return Artifact{}, fmt.Errorf("unsupported schema artifact format %q", pulled.Annotations[annotationFormat])
	}
	entries, err := fs.ReadDir(pulled.FileSystem, ".")
	if err != nil {
		return Artifact{}, fmt.Errorf("read schema artifact: %w", err)
	}
	managedDataPresent := false
	checksPresent := false
	schemaPresent := false
	for _, entry := range entries {
		if entry.IsDir() {
			return Artifact{}, fmt.Errorf(
				"schema artifact must contain only %s, %s and %s",
				FileName, ManagedDataFileName, ChecksFileName,
			)
		}
		switch entry.Name() {
		case FileName:
			schemaPresent = true
		case ManagedDataFileName:
			managedDataPresent = true
		case ChecksFileName:
			checksPresent = true
		default:
			return Artifact{}, fmt.Errorf("schema artifact carries unexpected file %s", entry.Name())
		}
	}
	if !schemaPresent {
		return Artifact{}, fmt.Errorf("schema artifact must contain %s", FileName)
	}
	data, err := fs.ReadFile(pulled.FileSystem, FileName)
	if err != nil {
		return Artifact{}, fmt.Errorf("read schema artifact: %w", err)
	}
	db, err := atlashcl.Parse(data, FileName)
	if err != nil {
		return Artifact{}, fmt.Errorf("parse schema artifact: %w", err)
	}
	managed := []byte(nil)
	if managedDataPresent {
		managed, err = fs.ReadFile(pulled.FileSystem, ManagedDataFileName)
		if err != nil {
			return Artifact{}, fmt.Errorf("read managed data layer: %w", err)
		}
	}
	if err := AttachManagedRows(db, managed); err != nil {
		return Artifact{}, err
	}
	checks := []byte(nil)
	if checksPresent {
		checks, err = fs.ReadFile(pulled.FileSystem, ChecksFileName)
		if err != nil {
			return Artifact{}, fmt.Errorf("read checks layer: %w", err)
		}
	}
	return Artifact{
		Database:   db,
		FileSystem: pulled.FileSystem,
		Checks:     checks,
		Descriptor: pulled.Descriptor,
		Reference:  pulled.Reference,
	}, nil
}

func formatDiagnostics(diagnostics []atlashclrender.Diagnostic) string {
	var builder strings.Builder
	for _, diagnostic := range diagnostics {
		fmt.Fprintf(&builder, "- %s: %s\n", diagnostic.Path, diagnostic.Message)
	}
	return strings.TrimSuffix(builder.String(), "\n")
}

func writeExclusive(output string, contents []byte) error {
	parent := filepath.Dir(output)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create schema artifact output directory: %w", err)
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return fmt.Errorf("open schema artifact output directory: %w", err)
	}
	defer root.Close()
	name := filepath.Base(output)
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create schema artifact output: %w", err)
	}
	if _, err := file.Write(contents); err != nil {
		_ = file.Close()
		_ = root.Remove(name)
		return fmt.Errorf("write schema artifact output: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = root.Remove(name)
		return fmt.Errorf("sync schema artifact output: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = root.Remove(name)
		return fmt.Errorf("close schema artifact output: %w", err)
	}
	return nil
}

// writeOnceTags is the version tag when there is one. A push that generated no
// version has nothing to protect from being moved, and passing an empty string
// would make the write-once check refuse a tag nobody named.
func (p preparedPush) writeOnceTags() []string {
	if p.Version == "" {
		return nil
	}
	return []string{p.Version}
}
