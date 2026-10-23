// Package providerarchive reads the one executable out of an official provider
// archive (".tar.gz" or ".zip"). It writes nothing to disk itself: the caller
// supplies the destination, so the caller decides where unverified bytes go.
package providerarchive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// MaxExecutableSize bounds the bytes read for the executable, so a hostile
// archive cannot fill the disk.
const MaxExecutableSize = 1 << 30

var (
	// ErrUnsupportedFormat reports an archive name that is neither ".tar.gz" nor ".zip".
	ErrUnsupportedFormat = errors.New("unsupported archive format")
	// ErrUnsafePath reports an entry whose name could leave the extraction directory.
	ErrUnsafePath = errors.New("archive entry has an unsafe path")
	// ErrMissingExecutable reports an archive without the expected executable.
	ErrMissingExecutable = errors.New("archive does not contain the executable")
	// ErrDuplicateExecutable reports an archive that names the executable twice.
	ErrDuplicateExecutable = errors.New("archive contains the executable more than once")
	// ErrNotRegular reports an executable entry that is a link, directory or device.
	ErrNotRegular = errors.New("archive executable is not a regular file")
	// ErrTooLarge reports an executable above [MaxExecutableSize].
	ErrTooLarge = errors.New("archive executable is too large")
)

// CopyExecutable copies the entry named executable from the archive at
// archivePath to dst. The format comes from the suffix of archivePath. Every
// entry name in the archive is checked, not only the one copied: an archive with
// an absolute path, a ".." element, a backslash or a drive letter is refused.
// The executable must be a top-level regular file that occurs once. Bytes may
// reach dst before a later entry fails the check, so the caller discards dst on
// any error.
func CopyExecutable(archivePath, executable string, dst io.Writer) error {
	switch {
	case strings.HasSuffix(archivePath, ".tar.gz"):
		return copyTar(archivePath, executable, dst)
	case strings.HasSuffix(archivePath, ".zip"):
		return copyZip(archivePath, executable, dst)
	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedFormat, path.Base(strings.ReplaceAll(archivePath, `\`, "/")))
	}
}

// cleanName returns the entry name without a leading "./" or a trailing slash,
// or ErrUnsafePath.
func cleanName(name string) (string, error) {
	unsafe := name == "" || strings.ContainsAny(name, "\\\x00") || strings.HasPrefix(name, "/") ||
		(len(name) >= 2 && name[1] == ':')
	if !unsafe {
		for element := range strings.SplitSeq(name, "/") {
			if element == ".." {
				unsafe = true
			}
		}
	}
	if unsafe {
		return "", fmt.Errorf("%w: %q", ErrUnsafePath, name)
	}
	return strings.TrimSuffix(path.Clean(name), "/"), nil
}

func copyTar(archivePath, executable string, dst io.Writer) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("read gzip stream: %w", err)
	}
	reader := tar.NewReader(gz)
	found := false
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}
		name, err := cleanName(header.Name)
		if err != nil {
			return err
		}
		if name != executable {
			continue
		}
		if found {
			return ErrDuplicateExecutable
		}
		if header.Typeflag != tar.TypeReg {
			return fmt.Errorf("%w: %q", ErrNotRegular, header.Name)
		}
		if err := copyLimited(dst, reader, header.Size); err != nil {
			return err
		}
		found = true
	}
	// Reading to the end of the gzip stream checks its trailing CRC.
	if _, err := io.Copy(io.Discard, gz); err != nil { // #nosec G110 -- the bytes are discarded, nothing accumulates
		return fmt.Errorf("read gzip stream: %w", err)
	}
	if !found {
		return fmt.Errorf("%w: %q", ErrMissingExecutable, executable)
	}
	return nil
}

func copyZip(archivePath, executable string, dst io.Writer) error {
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("read zip archive: %w", err)
	}
	defer func() { _ = archive.Close() }()
	found := false
	for _, entry := range archive.File {
		name, err := cleanName(entry.Name)
		if err != nil {
			return err
		}
		if name != executable {
			continue
		}
		if found {
			return ErrDuplicateExecutable
		}
		if !entry.Mode().IsRegular() {
			return fmt.Errorf("%w: %q", ErrNotRegular, entry.Name)
		}
		reader, err := entry.Open()
		if err != nil {
			return fmt.Errorf("read zip entry: %w", err)
		}
		err = copyLimited(dst, reader, int64(min(entry.UncompressedSize64, MaxExecutableSize+1)))
		_ = reader.Close()
		if err != nil {
			return err
		}
		found = true
	}
	if !found {
		return fmt.Errorf("%w: %q", ErrMissingExecutable, executable)
	}
	return nil
}

// copyLimited copies src to dst and refuses more than MaxExecutableSize bytes,
// whatever size the entry header declared.
func copyLimited(dst io.Writer, src io.Reader, declared int64) error {
	if declared > MaxExecutableSize {
		return ErrTooLarge
	}
	n, err := io.Copy(dst, io.LimitReader(src, MaxExecutableSize+1))
	if err != nil {
		return fmt.Errorf("copy executable: %w", err)
	}
	if n > MaxExecutableSize {
		return ErrTooLarge
	}
	return nil
}
