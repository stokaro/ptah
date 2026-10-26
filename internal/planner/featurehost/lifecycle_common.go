package featurehost

import (
 "fmt"
 "ptah.run/core/featureplan"
 "ptah.run/core/schemaext"
)

func lifecycleCommon(windows Windows, metadata []featureplan.CommonStep, count int) (map[int]bool, error) {
 result := make(map[int]bool, len(windows.LifecycleCommon))
 for _, index := range windows.LifecycleCommon {
  if windows.Lifecycle == nil || index < windows.Lifecycle.Start || index >= windows.Lifecycle.End || index >= count || result[index] || len(metadata) != count || len(metadata[index].Effects) == 0 {
   return nil, fmt.Errorf("%w: lifecycle common step needs a unique index and established footprint inside its span", schemaext.ErrInvalidValue)
  }
  result[index] = true
 }
 return result, nil
}
