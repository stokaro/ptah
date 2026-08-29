// Package quickstart runs the commands a quick-start page publishes and checks
// the output the page shows against what the commands actually print.
//
// The commands are read out of the page. Nothing here holds a second copy of
// them, and that is the whole point: a runner carrying its own transcript stays
// green while the page it claims to cover rots, which is the failure this
// package exists to prevent. Rewording a step on the page changes what runs on
// the next pull request, and deleting a step removes it from the run.
//
// A page opts in with `quickstart: true` in its frontmatter. Inside an opted-in
// page the fence language decides what a block is: `bash` and `powershell`
// blocks are shell-specific steps, a `console` block outside tabs is a command
// sequence shared by both shells, an `sql` block is a file the reader is told
// to write, and a `text` block introduced by "… on standard output:" or "… on
// standard error:" is an expectation for the step above it. Extract reports
// the shapes that would otherwise be skipped in silence -- an output block
// introduced by neither stream, a step-less expectation, a Bash block in a
// Windows tab -- because a skipped assertion reads exactly like a passing one.
//
// Run turns one page into one shell script per shell, in page order, and runs
// it in a fresh working directory with a sentinel between steps so each step's
// two streams can be told apart. One process, so `cd` and the rest of the shell
// state behave as they do for a reader working through the page by hand.
package quickstart
