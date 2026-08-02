// Package schemaartifact maps Ptah desired-schema IR to OCI artifacts.
package schemaartifact

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/pathguard"
)

const (
	// FileName is the canonical schema layer name.
	FileName = "schema.hcl"
	// LayerMediaType identifies canonical Ptah schema HCL layers.
	LayerMediaType = "application/vnd.stokaro.ptah.schema.hcl.v1"

	annotationFormat = "io.stokaro.ptah.schema-format"
	canonicalFormat  = "hcl"
)

// PushOptions controls schema artifact tags and metadata.
type PushOptions struct {
	Tags        []string
	Version     string
	PlainHTTP   bool
	Now         func() time.Time
	Annotations map[string]string
}

// PushResult describes a published immutable schema artifact.
type PushResult struct {
	ociartifact.PushResult
	Version string
}

// Artifact is a validated canonical schema retrieved from OCI storage.
type Artifact struct {
	Database   *goschema.Database
	FileSystem fs.FS
	Descriptor ocispec.Descriptor
	Reference  ociartifact.Reference
}

type preparedPush struct {
	FileSystem  fs.FS
	Tags        []string
	Version     string
	Annotations map[string]string
}

// Capture renders db into a lossless canonical HCL snapshot.
func Capture(db *goschema.Database) (fs.FS, error) {
	if db == nil {
		return nil, fmt.Errorf("schema database is required")
	}
	if len(db.ManagedData) > 0 {
		return nil, fmt.Errorf("schema artifact cannot represent managed data without loss")
	}
	for _, role := range db.Roles {
		if role.Password != "" {
			return nil, fmt.Errorf("schema artifact cannot contain password for role %q", role.Name)
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
	snapshot, err := fsnapshot.FromFiles(map[string][]byte{FileName: rendered.Data})
	if err != nil {
		return nil, fmt.Errorf("build schema artifact filesystem: %w", err)
	}
	return snapshot, nil
}

// Push publishes db to the repository named by reference.
func Push(
	ctx context.Context,
	reference string,
	db *goschema.Database,
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
	db *goschema.Database,
	opts PushOptions,
) (PushResult, error) {
	prepared, err := prepare(db, opts)
	if err != nil {
		return PushResult{}, err
	}
	result, err := ociartifact.PushTo(ctx, target, prepared.FileSystem, ociartifact.PushOptions{
		ArtifactType:   ociartifact.SchemaArtifactType,
		LayerMediaType: LayerMediaType,
		Tags:           prepared.Tags,
		WriteOnceTags:  []string{prepared.Version},
		Annotations:    prepared.Annotations,
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
	db *goschema.Database,
	opts PushOptions,
) (PushResult, error) {
	prepared, err := prepare(db, opts)
	if err != nil {
		return PushResult{}, err
	}
	result, err := client.Push(ctx, reference, prepared.FileSystem, ociartifact.PushOptions{
		ArtifactType:   ociartifact.SchemaArtifactType,
		LayerMediaType: LayerMediaType,
		Tags:           prepared.Tags,
		WriteOnceTags:  []string{prepared.Version},
		Annotations:    prepared.Annotations,
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
		ExpectedArtifactTypes: []string{ociartifact.SchemaArtifactType},
		LayerMediaType:        LayerMediaType,
	})
	if err != nil {
		return Artifact{}, err
	}
	return validatePulled(pulled)
}

// PullFrom retrieves and validates a canonical schema from target.
func PullFrom(ctx context.Context, target oras.ReadOnlyTarget, selector string) (Artifact, error) {
	pulled, err := ociartifact.PullFrom(ctx, target, selector, ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.SchemaArtifactType},
		LayerMediaType:        LayerMediaType,
	})
	if err != nil {
		return Artifact{}, err
	}
	return validatePulled(pulled)
}

// PullToFile retrieves reference and writes its canonical HCL to output.
func PullToFile(ctx context.Context, reference, output string, plainHTTP bool) (Artifact, string, error) {
	if strings.TrimSpace(output) == "" {
		return Artifact{}, "", fmt.Errorf("schema artifact output file is required")
	}
	resolved, err := pathguard.ResolveCLIPath(output)
	if err != nil {
		return Artifact{}, "", fmt.Errorf("resolve schema artifact output: %w", err)
	}
	if _, err := os.Lstat(resolved); err == nil {
		return Artifact{}, "", fmt.Errorf("schema artifact output already exists: %s", resolved)
	} else if !os.IsNotExist(err) {
		return Artifact{}, "", fmt.Errorf("stat schema artifact output: %w", err)
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: plainHTTP})
	if err != nil {
		return Artifact{}, "", err
	}
	artifact, err := Pull(ctx, client, reference)
	if err != nil {
		return Artifact{}, "", err
	}
	contents, err := fs.ReadFile(artifact.FileSystem, FileName)
	if err != nil {
		return Artifact{}, "", fmt.Errorf("read canonical schema artifact: %w", err)
	}
	if err := writeExclusive(resolved, contents); err != nil {
		return Artifact{}, "", err
	}
	return artifact, resolved, nil
}

func prepare(
	db *goschema.Database,
	opts PushOptions,
) (preparedPush, error) {
	snapshot, err := Capture(db)
	if err != nil {
		return preparedPush{}, err
	}
	version := opts.Version
	if version == "" {
		now := opts.Now
		if now == nil {
			now = time.Now
		}
		version = ociartifact.VersionTag(now())
	}
	tags := append(append([]string{}, opts.Tags...), ociartifact.DefaultTag)
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
	if len(entries) != 1 || entries[0].Name() != FileName || entries[0].IsDir() {
		return Artifact{}, fmt.Errorf("schema artifact must contain exactly %s", FileName)
	}
	data, err := fs.ReadFile(pulled.FileSystem, FileName)
	if err != nil {
		return Artifact{}, fmt.Errorf("read schema artifact: %w", err)
	}
	db, err := atlashcl.Parse(data, FileName)
	if err != nil {
		return Artifact{}, fmt.Errorf("parse schema artifact: %w", err)
	}
	return Artifact{
		Database:   db,
		FileSystem: pulled.FileSystem,
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
