// One Pagefind ranking override for both Starlight's reader UI and the ranking
// smoke test. Search aliases are curated vocabulary, not repeated body prose.
export const pagefindRanking = {
  metaWeights: { searchAliases: 16 },
};
