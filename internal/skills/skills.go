// Package skills installs the agent skills Ptah ships into the directories
// coding agents read skills from.
//
// A skill is a directory holding a SKILL.md, whose frontmatter names the skill
// and says when an agent should use it, and whose body tells the agent how to
// drive Ptah. The skills live under docs/skills in the repository and reach
// the binary through [docs.Skills]; this package lists them, says where each
// agent reads them, and copies one to disk. It never deletes a file, and it
// overwrites one only when asked.
package skills

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"ptah.run/docs"
)

// Agent names a coding agent that reads skills from a directory.
type Agent string

// The agents whose skill directories Ptah knows. Codex, Cursor and GitHub
// Copilot all read the shared `.agents/skills` directory; Claude Code reads its
// own.
const (
	Claude  Agent = "claude"
	Codex   Agent = "codex"
	Cursor  Agent = "cursor"
	Copilot Agent = "copilot"
)

// Agents returns every agent Ptah knows, in a fixed order.
func Agents() []Agent { return []Agent{Claude, Codex, Cursor, Copilot} }

// ErrUnknownAgent reports an agent name Ptah does not know.
var ErrUnknownAgent = errors.New("unknown agent")

// ParseAgent returns the agent a name selects, ignoring case.
func ParseAgent(name string) (Agent, error) {
	agent := Agent(strings.ToLower(strings.TrimSpace(name)))
	if !slices.Contains(Agents(), agent) {
		return "", fmt.Errorf("%w %q; use one of %s", ErrUnknownAgent, name, agentList())
	}
	return agent, nil
}

func agentList() string {
	names := make([]string, 0, len(Agents()))
	for _, agent := range Agents() {
		names = append(names, string(agent))
	}
	return strings.Join(names, ", ")
}

// SkillsDir returns the directory the agent reads skills from, relative to
// the project root or to the user's home directory: the agents read the same
// relative path in both.
func SkillsDir(agent Agent) string {
	if agent == Claude {
		return filepath.Join(".claude", "skills")
	}
	return filepath.Join(".agents", "skills")
}

// skillsRoot is the directory inside [docs.Skills] that holds one directory
// per skill.
const skillsRoot = "skills"

// skillFile is the file that makes a directory a skill.
const skillFile = "SKILL.md"

// Skill is one skill Ptah ships.
type Skill struct {
	// Name is the skill's directory name, which its frontmatter repeats.
	Name string
	// Description is the frontmatter description: what the skill does and
	// when an agent should use it.
	Description string
}

// List returns the skills Ptah ships, sorted by name.
func List() ([]Skill, error) {
	return list(docs.Skills)
}

func list(source fs.FS) ([]Skill, error) {
	entries, err := fs.ReadDir(source, skillsRoot)
	if err != nil {
		return nil, fmt.Errorf("read the shipped skills: %w", err)
	}
	var skills []Skill
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		body, readErr := fs.ReadFile(source, path.Join(skillsRoot, entry.Name(), skillFile))
		if readErr != nil {
			return nil, fmt.Errorf("read skill %s: %w", entry.Name(), readErr)
		}
		skills = append(skills, Skill{Name: entry.Name(), Description: frontmatterValue(body, "description")})
	}
	return skills, nil
}

// frontmatterValue returns the value of one key in a SKILL.md frontmatter, with
// surrounding quotes removed, or "" when the key is absent. The skills are
// Ptah's own files, so the reader handles the one-line form they are written in.
func frontmatterValue(body []byte, key string) string {
	lines := strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n")
	if len(lines) == 0 || lines[0] != "---" {
		return ""
	}
	for _, line := range lines[1:] {
		if line == "---" {
			break
		}
		if value, ok := strings.CutPrefix(line, key+":"); ok {
			return strings.Trim(strings.TrimSpace(value), `"`)
		}
	}
	return ""
}

// ErrUnknownSkill reports a skill name Ptah does not ship.
var ErrUnknownSkill = errors.New("unknown skill")

// ErrFileDiffers reports a file already on disk, at a path a skill installs
// to, whose content is not the skill's.
var ErrFileDiffers = errors.New("a file with other content is already there")

// InstallOptions configures [Install].
type InstallOptions struct {
	// Overwrite replaces a file whose content differs from the skill's. Without
	// it, such a file refuses the whole install before anything is written.
	Overwrite bool
}

// FileStatus says what an install did with one file.
type FileStatus string

// The outcomes of one file.
const (
	// Written means the file was created or replaced.
	Written FileStatus = "written"
	// Unchanged means the file was already there with the skill's content.
	Unchanged FileStatus = "unchanged"
)

// InstalledFile is one file of an installed skill.
type InstalledFile struct {
	// Path is where the file is, dir joined with the skill's relative path.
	Path string
	// Status is what the install did with it.
	Status FileStatus
}

// Install copies the named skill into dir, as dir/<name>/SKILL.md and the
// files beside it, creating directories as needed. Files already present
// with the skill's content are left as they are. A file with other content
// refuses the install with [ErrFileDiffers] unless opts.Overwrite is set, and
// the refusal comes before any file is written. Install deletes nothing.
//
// The result lists every file of the skill, in a stable order. An unknown
// name fails with [ErrUnknownSkill].
func Install(name, dir string, opts InstallOptions) ([]InstalledFile, error) {
	return install(docs.Skills, name, dir, opts)
}

type plannedFile struct {
	target  string
	content []byte
	present bool
}

func install(source fs.FS, name, dir string, opts InstallOptions) ([]InstalledFile, error) {
	planned, err := planInstall(source, name, dir, opts)
	if err != nil {
		return nil, err
	}
	installed := make([]InstalledFile, 0, len(planned))
	for _, file := range planned {
		if file.present {
			installed = append(installed, InstalledFile{Path: file.target, Status: Unchanged})
			continue
		}
		if err := os.MkdirAll(filepath.Dir(file.target), 0o750); err != nil {
			return installed, fmt.Errorf("create the directory for %s: %w", file.target, err)
		}
		if err := os.WriteFile(file.target, file.content, 0o600); err != nil {
			return installed, fmt.Errorf("write %s: %w", file.target, err)
		}
		installed = append(installed, InstalledFile{Path: file.target, Status: Written})
	}
	return installed, nil
}

// planInstall reads every file of the skill and compares it with what dir
// holds, so that a refusal happens before anything is written.
func planInstall(source fs.FS, name, dir string, opts InstallOptions) ([]plannedFile, error) {
	// A name is one directory under skills/. Joined unchecked, a name such as
	// "../skills/ptah-schema" would clean to a shipped skill and be accepted
	// under a spelling no listing shows.
	if name == "" || name == "." || name == ".." || name != path.Base(name) {
		return nil, unknownSkill(source, name)
	}
	root := path.Join(skillsRoot, name)
	if _, err := fs.Stat(source, path.Join(root, skillFile)); err != nil {
		return nil, unknownSkill(source, name)
	}
	var planned []plannedFile
	err := fs.WalkDir(source, root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		content, err := fs.ReadFile(source, current)
		if err != nil {
			return err
		}
		relative := strings.TrimPrefix(current, skillsRoot+"/")
		target := filepath.Join(dir, filepath.FromSlash(relative))
		existing, err := os.ReadFile(target) // #nosec G304 -- the target is the caller's chosen skills directory joined with a path from the embedded skill.
		switch {
		case errors.Is(err, fs.ErrNotExist):
			planned = append(planned, plannedFile{target: target, content: content})
		case err != nil:
			return fmt.Errorf("read %s: %w", target, err)
		case bytes.Equal(existing, content):
			planned = append(planned, plannedFile{target: target, content: content, present: true})
		case opts.Overwrite:
			planned = append(planned, plannedFile{target: target, content: content})
		default:
			return fmt.Errorf("%s: %w; pass --overwrite to replace it", target, ErrFileDiffers)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return planned, nil
}

func unknownSkill(source fs.FS, name string) error {
	skills, err := list(source)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(skills))
	for _, skill := range skills {
		names = append(names, skill.Name)
	}
	return fmt.Errorf("%w %q; Ptah ships %s", ErrUnknownSkill, name, strings.Join(names, ", "))
}
