// Package terminologyguard hosts the repository guard that holds the native
// command tree's help text to the terminology registry.
//
// The registry is docs/site/scripts/data/terminology.json, and section 7 of
// docs/STYLE_GUIDE.md is generated from it. It has two readers, because the
// corpus it governs is in two languages:
//
//   - docs/site/scripts/check-terminology.mjs reads Markdown prose -- the site,
//     the repository docs, the READMEs;
//   - this package reads the help text a person sees when they run the binary:
//     every Short, Long and Example in [ptah.run/cmd/root.NewRootCommand]'s
//     tree, and every flag usage string under it.
//
// Neither reader covers the other's corpus, and the split is not tidiness. The
// census behind stokaro/ptah#2380 found the `direct schema changes` rule broken
// in fourteen pages AND in the first sentence of `ptah --help`, where the site
// hero and the binary disagreed word for word. A Markdown checker cannot see the
// second, and the alternative -- matching Go string literals with a regular
// expression from JavaScript -- reads source text rather than help text, so it
// cannot see a sentence assembled at run time and would be subtly wrong about
// the ones it can see.
//
// TWO READERS, ONE FILE, AND THE HAZARD THAT CREATES. A second reader stops
// reading silently: a renamed field, a moved path, a JSON shape that no longer
// unmarshals into anything, and this package would enumerate zero bans and
// report a clean tree forever. [TestTheRegistryReachesBothReaders] is what
// refuses that. It requires the terms this package parsed to be exactly the rows
// the generated block of docs/STYLE_GUIDE.md carries -- an artifact the other
// reader writes -- so a Go half that stopped parsing disagrees with a table it
// did not produce, rather than agreeing with itself.
//
// WHICH TREE. The ban's `helpText.tree` field says, in the registry rather than
// here, and today it says `native`. `ptah-compat` describes itself in Atlas's
// vocabulary on purpose, which the compatibility policy in AGENTS.md asks for;
// the same decision excludes the `atlas/` documentation tree from the prose
// reader, and it is recorded once, as data, instead of twice as two independent
// judgments. A ban with no `helpText` governs prose only and this package skips
// it.
//
// The package carries no runtime code of its own.
package terminologyguard
