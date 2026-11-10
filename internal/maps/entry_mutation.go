package maps

type mapEntry[K comparable, V any] struct {
	key   K
	value V
}

// Delay non-reflexive inserts until source iteration finishes. Source and
// destination may alias; writing such keys during range would create new
// entries that range is allowed to visit and copy again. Ordinary key writes
// cannot add entries to an aliased source and keep their original fast path.
func copyMapEntries[K comparable, V any](dst, src map[K]V, onlyMissing bool) {
	var pending []mapEntry[K, V]
	for key, value := range src {
		if key != key {
			pending = append(pending, mapEntry[K, V]{key, value})
			continue
		}
		if onlyMissing {
			if _, exists := dst[key]; exists {
				continue
			}
		}
		dst[key] = value
	}
	for _, entry := range pending {
		dst[entry.key] = entry.value
	}
}

// A NaN-containing key cannot be looked up or deleted, even using the key
// produced by range. Rebuild only when needed, preserving the original map so
// FromRef/AsMap aliases continue to observe the mutation.
func rebuildNonReflexiveEntries[K comparable, V any](data map[K]V, entries []mapEntry[K, V]) {
	for key, value := range data {
		if key == key {
			entries = append(entries, mapEntry[K, V]{key, value})
		}
	}
	clear(data)
	for _, entry := range entries {
		data[entry.key] = entry.value
	}
}

func filterMapEntries[K comparable, V any](data map[K]V, keep func(K, V) bool) {
	var retained []mapEntry[K, V]
	removedNonReflexive := false
	for key, value := range data {
		accepted := keep(key, value)
		if key != key {
			if accepted {
				retained = append(retained, mapEntry[K, V]{key, value})
			} else {
				removedNonReflexive = true
			}
		} else if !accepted {
			delete(data, key)
		}
	}
	if removedNonReflexive {
		rebuildNonReflexiveEntries(data, retained)
	}
}

func mapEntryValues[K comparable, V any](data map[K]V, transform func(K, V) V) {
	var transformed []mapEntry[K, V]
	for key, value := range data {
		value = transform(key, value)
		if key != key {
			transformed = append(transformed, mapEntry[K, V]{key, value})
		} else {
			data[key] = value
		}
	}
	if len(transformed) > 0 {
		rebuildNonReflexiveEntries(data, transformed)
	}
}
