#!/usr/bin/env node
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rowsPath = join(scriptDir, 'data', 'feature-matrix-rows.json');
const repoRoot = join(scriptDir, '..', '..', '..');

// A matrix note names flags, and a flag name is a fact the code owns.
//
// The audit behind stokaro/ptah#1924 read all 191 rows against the code their
// evidence cites and found seven notes saying something the tree does not do --
// a 3.7% error rate in the most user-visible claims this repository publishes.
// Nothing caught them, because the three checks next door each guard something
// else: the generated page against the row data, a cell arguing for a verdict
// its row no longer holds, and a cited path the tree does not have. None of
// them opens the code.
//
// This is the checkable subset that issue asks for first, and the direction it
// catches is the one that actually recurs: a flag is RENAMED in the code and
// the note keeps the old spelling. Renaming `check-destructive` in
// cmd/migrate/generate.go turns this red on the row that names
// `--check-destructive`, which is the whole point -- the note cannot drift away
// from the flag set without saying so.
//
// What it does NOT catch is worth stating, because a gate whose limits are
// unstated is read as covering more than it does. It compares against the flag
// set of the WHOLE tree, so it cannot tell that a flag belongs to a different
// verb than the row is about, and it cannot tell a plausible wrong name from a
// right one: the audit's own `--version` where `--server-version` was meant
// passes here, because `--version` is a flag. What it removes is the class
// where a note names a flag that exists nowhere at all.

// A flag mention is a backticked long option. Backticks are the whole rule:
// unquoted prose about "the --format flag" is not a claim about a spelling the
// way `--format` is, and the matrix writes every flag it means in code font.
const flagMention = /`(--[a-z0-9][a-z0-9-]*)`/g;

// A flag NAME reaches the tree in two spellings. The registrars take it bare --
// `flags.String("dev-url", …)`, `atlasargs.NativeString("dir", …)` -- and the
// argument mappers, the diagnostics and the tests write it whole.
const bareRegistration =
  /\.(?:String|Bool|Int|Int64|Uint|Float64|Duration|StringArray|StringSlice|IntSlice|Var)(?:Var)?P?\(\s*(?:&[A-Za-z0-9_.[\]]+\s*,\s*)?"([a-z0-9][a-z0-9-]*)"/g;
const argsRegistration = /atlasargs\.[A-Za-z]+\(\s*"([a-z0-9][a-z0-9-]*)"/g;
const namedLookup = /\.(?:Lookup|Changed|MarkHidden|MarkDeprecated|MarkRequired)\(\s*"([a-z0-9][a-z0-9-]*)"/g;
// And a flag whose name is a constant reaches its registrar through the
// identifier rather than through a literal: `checkDestructiveFlag =
// "check-destructive"`, `MigrationsTableFlagName = "migrations-table"`.
const flagConstant = /\b[A-Za-z][A-Za-z0-9_]*Flag(?:Name)?\s*=\s*"([a-z0-9][a-z0-9-]*)"/g;
const wholeFlag = /"(--[a-z0-9][a-z0-9-]*)"/g;

// The directories a flag can be registered in. Named rather than the whole tree
// so a stray string in a fixture cannot make an unregistered flag look real.
const flagRoots = ['cmd', 'config', 'internal', 'migration', 'core', 'testkit'];

// knownFlags collects every flag name the Go sources spell, in either form.
export function knownFlags(sources) {
  const known = new Set();
  for (const source of sources) {
    for (const [, name] of source.matchAll(bareRegistration)) {
      known.add(`--${name}`);
    }
    for (const [, name] of source.matchAll(argsRegistration)) {
      known.add(`--${name}`);
    }
    for (const [, name] of source.matchAll(namedLookup)) {
      known.add(`--${name}`);
    }
    for (const [, name] of source.matchAll(flagConstant)) {
      known.add(`--${name}`);
    }
    for (const [, name] of source.matchAll(wholeFlag)) {
      known.add(name);
    }
  }
  return known;
}

// unknownFlagMentions collects the flags one row's NOTE names that the tree does
// not register.
//
// The note and not the evidence, and that is a measurement rather than a
// preference. The note is the field rendered onto the page, and it is the field
// the audit found wrong. The evidence field deliberately carries flags that
// exist nowhere: several rows establish that a verb does not register a flag by
// showing it answers `unknown flag` byte-identically to a nonsense control --
// `--skip-chxxxx`, `--name-formxxxx`, `--totally-bogus-flag` -- and a check
// reading that field would report the controls as defects and be turned off.
export function unknownFlagMentions(row, known) {
  const text = `${row.note ?? ''}`;
  const unknown = [];
  for (const [, flag] of text.matchAll(flagMention)) {
    if (!known.has(flag)) {
      unknown.push(flag);
    }
  }
  return [...new Set(unknown)];
}

function goSources(directory) {
  const sources = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === 'node_modules' || entry.name === 'testdata') {
        continue;
      }
      sources.push(...goSources(path));
      continue;
    }
    if (entry.name.endsWith('.go')) {
      sources.push(readFileSync(path, 'utf8'));
    }
  }
  return sources;
}

function treeFlags() {
  const sources = [];
  for (const root of flagRoots) {
    const path = join(repoRoot, root);
    try {
      if (statSync(path).isDirectory()) {
        sources.push(...goSources(path));
      }
    } catch {
      // A root this repository does not have contributes nothing, which is what
      // lets the list name a directory a later split may introduce.
    }
  }
  return knownFlags(sources);
}

function report(rows, known) {
  const findings = [];
  for (const [index, row] of rows.entries()) {
    for (const flag of unknownFlagMentions(row, known)) {
      findings.push({ index, feature: row.feature, flag });
    }
  }
  return findings;
}

// selftest drives the rule with fixtures, because a check that finds nothing and
// a check that examines nothing print the same thing.
function selftest() {
  const cases = [
    {
      name: 'a bare registration is a known flag',
      sources: ['flags.String("dev-url", "", "help")'],
      row: { note: 'takes `--dev-url`' },
      want: [],
    },
    {
      name: 'a registration through a variable is a known flag',
      sources: ['flags.StringVarP(&target.dir, "dir", "d", "", "help")'],
      row: { note: 'takes `--dir`' },
      want: [],
    },
    {
      name: 'an atlasargs registration is a known flag',
      sources: ['atlasargs.NativeStringDefault("dir", "", "Migration directory", "migrations-dir", "file://migrations")'],
      row: { note: 'takes `--dir`' },
      want: [],
    },
    {
      name: 'a flag spelled whole is a known flag',
      sources: ['args := []string{"--server-version", version}'],
      row: { note: 'refined by `--server-version`' },
      want: [],
    },
    {
      name: 'a flag the tree does not spell at all',
      sources: ['flags.String("dev-url", "", "help")'],
      row: { note: 'refined by a `--version` server string' },
      want: ['--version'],
    },
    {
      name: 'a flag whose name is a constant is a known flag',
      sources: ['const checkDestructiveFlag = "check-destructive"'],
      row: { note: 'fails with `--check-destructive`' },
      want: [],
    },
    {
      name: 'an exported flag-name constant is a known flag',
      sources: ['MigrationsTableFlagName = "migrations-table"'],
      row: { note: 'plus `--migrations-table`' },
      want: [],
    },
    {
      name: 'the evidence is NOT read, because it carries nonsense controls',
      sources: [],
      row: { evidence: 'byte-identical to the control `--skip-chxxxx`' },
      want: [],
    },
    {
      name: 'the same unknown flag twice is one finding',
      sources: [],
      row: { note: '`--gone` and `--gone`' },
      want: ['--gone'],
    },
    {
      name: 'prose without backticks is not a claim about a spelling',
      sources: [],
      row: { note: 'the --version flag of some other tool' },
      want: [],
    },
  ];

  const failures = [];
  for (const { name, sources, row, want } of cases) {
    const got = unknownFlagMentions(row, knownFlags(sources));
    if (JSON.stringify(got) !== JSON.stringify(want)) {
      failures.push(`${name}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
    }
  }
  if (failures.length > 0) {
    console.error('check-matrix-flag-names.mjs --selftest: FAILED');
    for (const failure of failures) {
      console.error(`  ${failure}`);
    }
    process.exit(1);
  }
  console.log(`check-matrix-flag-names.mjs --selftest: OK (${cases.length} cases)`);
}

if (process.argv.includes('--selftest')) {
  selftest();
} else {
  const rows = JSON.parse(readFileSync(rowsPath, 'utf8'));
  const known = treeFlags();
  const findings = report(rows, known);
  if (findings.length > 0) {
    console.error('check-matrix-flag-names.mjs: a note names a flag this tree does not register');
    for (const { index, feature, flag } of findings) {
      console.error(`  row ${index} (${feature}): ${flag}`);
    }
    process.exit(1);
  }
  console.log(`check-matrix-flag-names.mjs: OK (${rows.length} rows, ${known.size} registered flags)`);
}
