package featurehost

import (
 "ptah.run/core/ast"
 "ptah.run/core/plangraph"
)

// Project edges without changing emission ownership or contribution groups.
// ScheduleRewritten remains the authority for validating each rewrite claim.
func rewriteBoundaries(common plangraph.Contribution[[]ast.Node], features Result) []plangraph.Dependency {
 replacements := make(map[plangraph.StepID]plangraph.StepID)
 for _, rewrite := range features.Rewrites {
  for _, source := range rewrite.Sources { replacements[source] = rewrite.Replacement }
 }
 var projected []plangraph.Dependency
 contributions := append([]plangraph.Contribution[[]ast.Node]{common}, features.Contributions...)
 for _, contribution := range contributions {
  for _, edge := range contribution.Dependencies {
   before,after := edge.Before,edge.After
   if id,found:=replacements[before];found {before=id}
   if id,found:=replacements[after];found {after=id}
   if before!=after && (before!=edge.Before || after!=edge.After) {
    projected=append(projected,plangraph.Dependency{Before:before,After:after})
   }
  }
 }
 return projected
}
