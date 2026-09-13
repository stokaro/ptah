// The symptoms a reader arrives with, and the questions each one reaches.
//
// A reader who hits a failure has the symptom, not the vocabulary: they know
// the database said "already exists", not that the word for it is adoption. A
// chip is therefore a curated route rather than a search term -- "drift
// ignored" has to reach the question that says "drift disappeared", which no
// substring match would find.
//
// Free text in the field still matches the text of a question and its answer.
// The chips are for the words that are not in the text.
//
// Each symptom is also a `searchAliases` entry on the page, so the same word
// typed into the site search lands on this page rather than nowhere; the two
// lists are held together by scripts/check-faq-groups.mjs.

/** @type {{ label: string, questions: string[] }[]} */
export const faqSymptoms = [
  {
    label: 'already exists',
    questions: ['up-fails-on-existing-object', 'adopt-existing-database', 'import-from-another-tool'],
  },
  {
    label: 'dirty state',
    questions: ['dirty-state-after-failure', 'why-not-rerun-the-sql', 'repair-and-set-change-nothing', 'allow-dirty-two-meanings'],
  },
  {
    label: 'stuck migration',
    questions: ['dirty-state-after-failure', 'why-not-rerun-the-sql', 'partial-migration-transaction', 'concurrent-deployment-jobs'],
  },
  {
    label: 'checksum mismatch',
    questions: ['checksum-broke-after-merge', 'missing-sum-file', 'digest-is-not-authenticity'],
  },
  {
    label: 'table was dropped',
    questions: ['removed-table-plans-drop', 'block-destructive-changes-in-ci', 'reference-data-row-removed', 'status-versus-drift'],
  },
  {
    label: 'not reversible',
    questions: ['import-down-files', 'down-is-not-a-backup', 'checkpoint-rollback-boundary', 'app-rollback-and-database'],
  },
  {
    label: 'drift ignored',
    questions: ['ignores-hide-drift', 'drift-without-sql', 'status-versus-drift', 'compare-exit-code'],
  },
  {
    label: 'no such command',
    questions: ['command-not-in-my-binary', 'native-vs-compat-surface', 'replace-atlas-gradually'],
  },
  {
    label: 'slow fresh database',
    questions: ['squash-migrations', 'checkpoint-not-applied', 'delete-old-migrations', 'checkpoint-has-no-data'],
  },
  {
    label: 'partial migration',
    questions: ['partial-migration-transaction', 'tx-mode-all-limits', 'dirty-state-after-failure', 'why-not-rerun-the-sql'],
  },
];
