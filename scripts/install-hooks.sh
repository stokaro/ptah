#!/usr/bin/env sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
hooks_dir="$repo_root/.git/hooks"
pre_commit="$hooks_dir/pre-commit"
marker="# ptah-managed-pre-commit-hook"

mkdir -p "$hooks_dir"

if [ -f "$pre_commit" ] && ! grep -qF "$marker" "$pre_commit"; then
	backup="$pre_commit.ptah-backup.$(date +%Y%m%d%H%M%S)"
	mv "$pre_commit" "$backup"
	echo "Backed up existing pre-commit hook to $backup"
fi

cat >"$pre_commit" <<'HOOK'
#!/usr/bin/env sh
set -eu
# ptah-managed-pre-commit-hook

echo "Running qtlint..."
go tool qtlint ./...

echo "Running test style baseline check..."
scripts/check-test-style.sh

echo "Running golangci-lint..."
golangci-lint run ./...
HOOK

chmod +x "$pre_commit"
echo "Installed $pre_commit"
