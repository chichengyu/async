package async

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/testutil"
)

var errTestSentinel = errors.New("test sentinel error")

func genInts(n int) []int {
	s := make([]int, n)
	for i := 0; i < n; i++ {
		s[i] = i
	}
	return s
}

func cmpInt(a, b int) int { return a - b }

func shuffleData(data []int) {
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})
}

func freshCtx() context.Context {
	return core.EnsureTraceID(context.Background())
}

// ============================================================
// Section 1: 数据变换操作测试 (Sort / Reverse / Shuffle / Repeat)
// ============================================================

func TestSlice_Chain_Sort(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			shuffleData(data)

			sb := Slice(freshCtx(), data)
			result := sb.Sort(cmpInt).Values()

			for i := 1; i < len(result); i++ {
				if result[i-1] > result[i] {
					t.Fatalf("Sort failed at index %d: %d > %d", i, result[i-1], result[i])
				}
			}
		})
	}
}

func TestSlice_Chain_StableSort(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			type pair struct{ v, tag int }
			pairs := make([]pair, tier.Size)
			for i := range pairs {
				pairs[i] = pair{v: tier.Size - i, tag: i}
			}
			sb := Slice(freshCtx(), pairs)
			result := sb.StableSort(func(a, b pair) int { return a.v - b.v }).Values()

			for i := 1; i < len(result); i++ {
				if result[i-1].v > result[i].v {
					t.Fatalf("StableSort order broken at %d", i)
				}
				if result[i-1].v == result[i].v && result[i-1].tag > result[i].tag {
					t.Fatalf("StableSort stability broken at %d", i)
				}
			}
		})
	}
}

func TestSlice_Chain_IsSorted(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)

			if !Slice(freshCtx(), data).IsSorted(cmpInt) {
				t.Fatal("sorted data should be IsSorted=true")
			}
			shuffleData(data)
			if Slice(freshCtx(), data).IsSorted(cmpInt) {
				t.Fatal("shuffled data should be IsSorted=false")
			}
		})
	}
}

func TestSlice_Chain_Reverse(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			result := Slice(freshCtx(), data).Reverse().Values()
			n := len(result)
			for i := 0; i < n; i++ {
				if result[i] != n-1-i {
					t.Fatalf("Reverse failed at index %d: expected %d, got %d", i, n-1-i, result[i])
				}
			}
		})
	}
}

func TestSlice_Chain_Shuffle(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			first := Slice(freshCtx(), data).Shuffle().Values()
			second := Slice(freshCtx(), data).Shuffle().Values()

			sameCount := 0
			for i := range first {
				if first[i] == second[i] {
					sameCount++
				}
			}
			if tier.Size >= 100 && sameCount == tier.Size {
				t.Fatal("Shuffle should produce different order")
			}
		})
	}
}

func TestSlice_Chain_Repeat(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			base := []int{1, 2, 3}
			repeatN := tier.Size / 3
			if repeatN < 1 {
				repeatN = 1
			}
			result := Slice(freshCtx(), base).Repeat(repeatN).Values()
			expectedLen := len(base) * repeatN
			if len(result) != expectedLen {
				t.Fatalf("Repeat: expected len %d, got %d", expectedLen, len(result))
			}
		})
	}
}

// ============================================================
// Section 2: 过滤去重操作测试 (Filter / DeleteFunc / Compact / Dedup)
// ============================================================

func TestSlice_Chain_Filter(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			even := func(v int) bool { return v%2 == 0 }
			result := Slice(freshCtx(), data).Filter(even).Values()
			for _, v := range result {
				if v%2 != 0 {
					t.Fatalf("Filter: found odd value %d", v)
				}
			}
		})
	}
}

func TestSlice_Chain_DeleteFunc(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			odd := func(v int) bool { return v%2 != 0 }
			result := Slice(freshCtx(), data).DeleteFunc(odd).Values()
			for _, v := range result {
				if v%2 != 0 {
					t.Fatalf("DeleteFunc: found odd value %d", v)
				}
			}
		})
	}
}

func TestSlice_Chain_Compact(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := make([]int, 0, tier.Size)
			chunk := tier.Size / 10
			if chunk < 2 {
				chunk = 2
			}
			for i := 0; i < tier.Size; i += chunk {
				for j := 0; j < chunk && len(data) < tier.Size; j++ {
					data = append(data, i)
				}
			}
			eq := func(a, b int) bool { return a == b }
			result := Slice(freshCtx(), data).Compact(eq).Values()
			for i := 1; i < len(result); i++ {
				if result[i] == result[i-1] {
					t.Fatalf("Compact: consecutive duplicates at %d: %d==%d", i, result[i], result[i-1])
				}
			}
		})
	}
}

func TestSlice_Chain_Dedup(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			eq := func(a, b int) bool { return a == b }
			result := Slice(freshCtx(), data).Dedup(eq).Values()
			if len(result) != tier.Size {
				t.Fatalf("Dedup on distinct values: expected %d, got %d", tier.Size, len(result))
			}
		})
	}
}

// ============================================================
// Section 3: 切片修改操作测试 (Append / Prepend / Insert / Delete / DeleteRange / Replace)
// ============================================================

func TestSlice_Chain_Append(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			result := Slice(freshCtx(), data).Append(-1, -2).Values()
			if len(result) != tier.Size+2 {
				t.Fatalf("Append: expected len %d, got %d", tier.Size+2, len(result))
			}
			if result[tier.Size] != -1 || result[tier.Size+1] != -2 {
				t.Fatal("Append: last elements mismatch")
			}
		})
	}
}

func TestSlice_Chain_Prepend(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			result := Slice(freshCtx(), data).Prepend(-2, -1).Values()
			if len(result) != tier.Size+2 {
				t.Fatalf("Prepend: expected len %d, got %d", tier.Size+2, len(result))
			}
			if result[0] != -2 || result[1] != -1 {
				t.Fatal("Prepend: first elements mismatch")
			}
		})
	}
}

func TestSlice_Chain_Insert(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			pos := tier.Size / 2
			result := Slice(freshCtx(), data).Insert(pos, 999, 1000).Values()
			if len(result) != tier.Size+2 {
				t.Fatalf("Insert: expected len %d, got %d", tier.Size+2, len(result))
			}
			if result[pos] != 999 || result[pos+1] != 1000 {
				t.Fatal("Insert: inserted elements mismatch")
			}
		})
	}
}

func TestSlice_Chain_Delete(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			result := Slice(freshCtx(), data).Delete(0).Values()
			if len(result) != tier.Size-1 {
				t.Fatalf("Delete: expected len %d, got %d", tier.Size-1, len(result))
			}
			if result[0] != 1 {
				t.Fatalf("Delete: expected first=1, got %d", result[0])
			}
		})
	}
}

func TestSlice_Chain_DeleteRange(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			n := tier.Size / 5
			if n < 1 {
				n = 1
			}
			result := Slice(freshCtx(), data).DeleteRange(0, n).Values()
			expectedLen := tier.Size - n
			if len(result) != expectedLen {
				t.Fatalf("DeleteRange: expected len %d, got %d", expectedLen, len(result))
			}
			if result[0] != n {
				t.Fatalf("DeleteRange: expected first=%d, got %d", n, result[0])
			}
		})
	}
}

func TestSlice_Chain_Replace(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			result := Slice(freshCtx(), data).Replace(0, 2, 99, 100).Values()
			if len(result) != tier.Size {
				t.Fatalf("Replace: expected len %d, got %d", tier.Size, len(result))
			}
			if result[0] != 99 || result[1] != 100 {
				t.Fatal("Replace: replaced elements mismatch")
			}
		})
	}
}

// ============================================================
// Section 4: 子切片操作测试 (Take / Drop / SliceRange / Clip / Grow)
// ============================================================

func TestSlice_Chain_Take(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			n := tier.Size / 2
			if n < 1 {
				n = 1
			}
			result := Slice(freshCtx(), data).Take(n).Values()
			if len(result) != n {
				t.Fatalf("Take: expected len %d, got %d", n, len(result))
			}
		})
	}
}

func TestSlice_Chain_Drop(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			n := tier.Size / 2
			if n < 1 {
				n = 1
			}
			result := Slice(freshCtx(), data).Drop(n).Values()
			expectedLen := tier.Size - n
			if len(result) != expectedLen {
				t.Fatalf("Drop: expected len %d, got %d", expectedLen, len(result))
			}
		})
	}
}

func TestSlice_Chain_SliceRange(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			n := tier.Size / 4
			if n < 1 {
				n = 1
			}
			result := Slice(freshCtx(), data).SliceRange(n, n*2).Values()
			expectedLen := n
			if len(result) != expectedLen {
				t.Fatalf("SliceRange: expected len %d, got %d", expectedLen, len(result))
			}
		})
	}
}

func TestSlice_Chain_SliceAlias(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			n := tier.Size / 4
			if n < 1 {
				n = 1
			}
			result := Slice(freshCtx(), data).Slice(n, n*2).Values()
			expectedLen := n
			if len(result) != expectedLen {
				t.Fatalf("Slice: expected len %d, got %d", expectedLen, len(result))
			}
		})
	}
}

func TestSlice_Chain_Clip(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			result := Slice(freshCtx(), data).Clip().Values()
			if len(result) != tier.Size {
				t.Fatalf("Clip: len should equal %d, got %d", tier.Size, len(result))
			}
		})
	}
}

func TestSlice_Chain_Grow(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			result := Slice(freshCtx(), data).Grow(100).Values()
			if len(result) != tier.Size {
				t.Fatalf("Grow: len should not change, expected %d, got %d", tier.Size, len(result))
			}
		})
	}
}

// ============================================================
// Section 5: 查询操作测试 (Len / IsEmpty / First / Last / Items / Values / Clone)
// ============================================================

func TestSlice_Chain_Len(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			l := Slice(freshCtx(), data).Len()
			if l != tier.Size {
				t.Fatalf("Len: expected %d, got %d", tier.Size, l)
			}
		})
	}
}

func TestSlice_Chain_IsEmpty(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			if Slice(freshCtx(), data).IsEmpty() && tier.Size > 0 {
				t.Fatal("IsEmpty: non-empty slice should return false")
			}
			if !Slice(freshCtx(), []int{}).IsEmpty() {
				t.Fatal("IsEmpty: empty slice should return true")
			}
		})
	}
}

func TestSlice_Chain_First(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			v, ok := Slice(freshCtx(), data).First()
			if !ok || v != 0 {
				t.Fatalf("First: expected (0, true), got (%d, %v)", v, ok)
			}
		})
	}
}

func TestSlice_Chain_Last(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			v, ok := Slice(freshCtx(), data).Last()
			if !ok || v != tier.Size-1 {
				t.Fatalf("Last: expected (%d, true), got (%d, %v)", tier.Size-1, v, ok)
			}
		})
	}
}

func TestSlice_Chain_Items(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			ref := Slice(freshCtx(), data).Items()
			if len(ref) != tier.Size {
				t.Fatalf("Items: expected len %d, got %d", tier.Size, len(ref))
			}
		})
	}
}

func TestSlice_Chain_Values(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			vals := Slice(freshCtx(), data).Values()
			if len(vals) != tier.Size {
				t.Fatalf("Values: expected len %d, got %d", tier.Size, len(vals))
			}
		})
	}
}

func TestSlice_Chain_Clone(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			cloned := Slice(freshCtx(), data).Clone()
			if len(cloned) != tier.Size {
				t.Fatalf("Clone: expected len %d, got %d", tier.Size, len(cloned))
			}
		})
	}
}

// ============================================================
// Section 6: 搜索操作测试 (Contains / Index / Find / FindLast / All / Any / Count / Max / Min / BinarySearch)
// ============================================================

func TestSlice_Chain_Contains(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			mid := tier.Size / 2
			if !Slice(freshCtx(), data).Contains(func(v int) bool { return v == mid }) {
				t.Fatalf("Contains: should find %d", mid)
			}
			if Slice(freshCtx(), data).Contains(func(v int) bool { return v == tier.Size+999 }) {
				t.Fatal("Contains: should not find out-of-range value")
			}
		})
	}
}

func TestSlice_Chain_Index(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			mid := tier.Size / 2
			idx := Slice(freshCtx(), data).Index(func(v int) bool { return v == mid })
			if idx != mid {
				t.Fatalf("Index: expected %d, got %d", mid, idx)
			}
		})
	}
}

func TestSlice_Chain_Find(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			mid := tier.Size / 2
			v, ok := Slice(freshCtx(), data).Find(func(x int) bool { return x >= mid })
			if !ok || v != mid {
				t.Fatalf("Find: expected (%d, true), got (%d, %v)", mid, v, ok)
			}
		})
	}
}

func TestSlice_Chain_FindLast(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			v, ok := Slice(freshCtx(), data).FindLast(func(x int) bool { return x%2 == 0 })
			if !ok {
				t.Fatal("FindLast: should find an even number")
			}
			if v%2 != 0 {
				t.Fatalf("FindLast: expected even, got %d", v)
			}
		})
	}
}

func TestSlice_Chain_All(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			if !Slice(freshCtx(), data).All(func(v int) bool { return v >= 0 }) {
				t.Fatal("All: all values should be >=0")
			}
			if tier.Size > 10 {
				if Slice(freshCtx(), data).All(func(v int) bool { return v < tier.Size/2 }) {
					t.Fatal("All: not all values should be < mid")
				}
			}
		})
	}
}

func TestSlice_Chain_Any(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			mid := tier.Size / 2
			if !Slice(freshCtx(), data).Any(func(v int) bool { return v == mid }) {
				t.Fatalf("Any: should find %d", mid)
			}
			if Slice(freshCtx(), data).Any(func(v int) bool { return v < 0 }) {
				t.Fatal("Any: no negative values")
			}
		})
	}
}

func TestSlice_Chain_Count(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			cnt := Slice(freshCtx(), data).Count(func(v int) bool { return v%2 == 0 })
			expected := tier.Size/2 + tier.Size%2
			if cnt != expected {
				t.Fatalf("Count: expected %d, got %d", expected, cnt)
			}
		})
	}
}

func TestSlice_Chain_Max(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			shuffleData(data)
			maxV, ok := Slice(freshCtx(), data).Max(cmpInt)
			if !ok || maxV != tier.Size-1 {
				t.Fatalf("Max: expected %d, got (%d, %v)", tier.Size-1, maxV, ok)
			}
		})
	}
}

func TestSlice_Chain_Min(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			shuffleData(data)
			minV, ok := Slice(freshCtx(), data).Min(cmpInt)
			if !ok || minV != 0 {
				t.Fatalf("Min: expected 0, got (%d, %v)", minV, ok)
			}
		})
	}
}

func TestSlice_Chain_BinarySearch(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			mid := tier.Size / 2
			idx, found := Slice(freshCtx(), data).BinarySearch(mid, cmpInt)
			if !found || idx != mid {
				t.Fatalf("BinarySearch: expected (%d, true), got (%d, %v)", mid, idx, found)
			}
		})
	}
}

// ============================================================
// Section 7: 分块拆分操作测试 (Chunked / ChunkedN / Split)
// ============================================================

func TestSlice_Chain_Chunked(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			chunks := Slice(freshCtx(), data).Chunked(100)
			total := 0
			for _, c := range chunks {
				total += len(c)
			}
			if total != tier.Size {
				t.Fatalf("Chunked: total=%d, expected %d", total, tier.Size)
			}
		})
	}
}

func TestSlice_Chain_ChunkedN(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			chunks := Slice(freshCtx(), data).ChunkedN(16)
			total := 0
			for _, c := range chunks {
				total += len(c)
			}
			if total != tier.Size {
				t.Fatalf("ChunkedN: total=%d, expected %d", total, tier.Size)
			}
		})
	}
}

func TestSlice_Chain_Split(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			matched, unmatched := Slice(freshCtx(), data).Split(func(v int) bool { return v%2 == 0 })
			expectedEven := tier.Size/2 + tier.Size%2
			if len(matched) != expectedEven {
				t.Fatalf("Split matched: expected %d, got %d", expectedEven, len(matched))
			}
			expectedOdd := tier.Size / 2
			if len(unmatched) != expectedOdd {
				t.Fatalf("Split unmatched: expected %d, got %d", expectedOdd, len(unmatched))
			}
		})
	}
}

// ============================================================
// Section 8: 终端 Map 测试（SliceBuilder / ParallelSlice / SerialSlice 高并发）
// ============================================================

func TestSlice_Chain_Map_SliceBuilder(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				testutil.SkipIfTooLarge1M(t, tier.Size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				vals := r.Values()
				if len(vals) != tier.Size {
					t.Fatalf("Map: expected %d values, got %d", tier.Size, len(vals))
				}
				for i := range vals {
					if vals[i] != data[i]*2 {
						t.Fatalf("Map at %d: expected %d, got %d", i, data[i]*2, vals[i])
					}
				}
			})
		}
	}
}

func TestSlice_Chain_Map_ParallelSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				testutil.SkipIfTooLarge1M(t, tier.Size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Parallel().Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				vals := r.Values()
				if len(vals) != tier.Size {
					t.Fatalf("Map: expected %d values, got %d", tier.Size, len(vals))
				}
			})
		}
	}
}

func TestSlice_Chain_Map_SerialSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Serial().Map(func(ctx context.Context, v int) (int, error) {
				return v * 2, nil
			})
			if r.Err() {
				t.Fatalf("Map error: %v", r.Error())
			}
			vals := r.Values()
			if len(vals) != tier.Size {
				t.Fatalf("Map: expected %d values, got %d", tier.Size, len(vals))
			}
		})
	}
}

func TestSlice_Chain_Map_SerialSlice_FailFast(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			// 成功路径：FailFast 模式无错误时所有结果正确
			r := Slice(freshCtx(), data).Serial().FailFast().Map(func(ctx context.Context, v int) (int, error) {
				return v * 2, nil
			})
			if r.Err() {
				t.Fatalf("Map Serial FF: unexpected error: %v", r.Error())
			}
			vals := r.Values()
			if len(vals) != tier.Size {
				t.Fatalf("Map Serial FF: expected %d values, got %d", tier.Size, len(vals))
			}
			// 失败路径：FailFast 模式下首个错误立即返回
			r2 := Slice(freshCtx(), data).Serial().FailFast().Map(func(ctx context.Context, v int) (int, error) {
				if v == tier.Size/2 {
					return 0, errors.New("injected error")
				}
				return v * 2, nil
			})
			if !r2.Err() {
				t.Fatalf("Map Serial FF: expected error but got none")
			}
			if r2.Len() != tier.Size {
				t.Fatalf("Map Serial FF: expected results len %d, got %d", tier.Size, r2.Len())
			}
		})
	}
}

func TestSlice_Chain_Map_SliceWith(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			r := SliceWith[string](freshCtx(), data).Map(func(ctx context.Context, v int) (string, error) {
				return strconv.Itoa(v), nil
			})
			if r.Err() {
				t.Fatalf("SliceWith Map error: %v", r.Error())
			}
			vals := r.Values()
			if len(vals) != tier.Size {
				t.Fatalf("SliceWith Map: expected %d values, got %d", tier.Size, len(vals))
			}
		})
	}
}

// ============================================================
// Section 9: 终端 ForEach 测试（SliceBuilder / ParallelSlice / SerialSlice 高并发）
// ============================================================

func TestSlice_Chain_ForEach_SliceBuilder(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				testutil.SkipIfTooLarge1M(t, tier.Size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genInts(tier.Size)
				var sum int64
				r := Slice(freshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&sum, int64(v))
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if r.SuccessCount() != int64(tier.Size) {
					t.Fatalf("ForEach SuccessCount: expected %d, got %d", tier.Size, r.SuccessCount())
				}
			})
		}
	}
}

func TestSlice_Chain_ForEach_ParallelSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				testutil.SkipIfTooLarge1M(t, tier.Size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genInts(tier.Size)
				var sum int64
				r := Slice(freshCtx(), data).Parallel().Worker(conc).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&sum, int64(v))
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if r.SuccessCount() != int64(tier.Size) {
					t.Fatalf("ForEach SuccessCount: expected %d, got %d", tier.Size, r.SuccessCount())
				}
			})
		}
	}
}

func TestSlice_Chain_ForEach_SerialSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			var sum int64
			r := Slice(freshCtx(), data).Serial().ForEach(func(ctx context.Context, v int) error {
				atomic.AddInt64(&sum, int64(v))
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEach error: %v", r.Error())
			}
			if r.SuccessCount() != int64(tier.Size) {
				t.Fatalf("ForEach SuccessCount: expected %d, got %d", tier.Size, r.SuccessCount())
			}
		})
	}
}

func TestSlice_Chain_ForEach_SerialSlice_FailFast(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			// 成功路径：FailFast 模式无错误时所有任务成功
			var counter int64
			r := Slice(freshCtx(), data).Serial().FailFast().ForEach(func(ctx context.Context, v int) error {
				atomic.AddInt64(&counter, 1)
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEach Serial FF: unexpected error: %v", r.Error())
			}
			if r.SuccessCount() != int64(tier.Size) {
				t.Fatalf("ForEach Serial FF: expected %d, got %d", tier.Size, r.SuccessCount())
			}
			// 失败路径：FailFast 模式下首个错误立即返回
			r2 := Slice(freshCtx(), data).Serial().FailFast().ForEach(func(ctx context.Context, v int) error {
				if v == tier.Size/2 {
					return errors.New("injected error")
				}
				return nil
			})
			if !r2.Err() {
				t.Fatalf("ForEach Serial FF: expected error but got none")
			}
		})
	}
}

// ============================================================
// Section 10: 终端 Reduce 测试（SliceBuilder / SerialSlice）
// ============================================================

func TestSlice_Chain_Reduce_SliceBuilder(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			sum, err := Slice(freshCtx(), data).Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
				return acc + v, nil
			})
			if err != nil {
				t.Fatalf("Reduce error: %v", err)
			}
			expected := tier.Size * (tier.Size - 1) / 2
			if sum != expected {
				t.Fatalf("Reduce: expected sum %d, got %d", expected, sum)
			}
		})
	}
}

func TestSlice_Chain_Reduce_SerialSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			sum, err := Slice(freshCtx(), data).Serial().Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
				return acc + v, nil
			})
			if err != nil {
				t.Fatalf("Reduce error: %v", err)
			}
			expected := tier.Size * (tier.Size - 1) / 2
			if sum != expected {
				t.Fatalf("Reduce: expected sum %d, got %d", expected, sum)
			}
		})
	}
}

// ============================================================
// Section 11: 终端 Stream 测试（SliceBuilder / ParallelSlice 高并发）
// ============================================================

func TestSlice_Chain_Stream_SliceBuilder(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				testutil.SkipIfTooLarge1M(t, tier.Size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genInts(tier.Size)
				ch := Slice(freshCtx(), data).Worker(conc).Stream(func(ctx context.Context, v int) (int, error) {
					return v * 2, nil
				}, 0)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream error: %v", res.Err)
					}
					count++
				}
				if count != tier.Size {
					t.Fatalf("Stream: expected %d results, got %d", tier.Size, count)
				}
			})
		}
	}
}

func TestSlice_Chain_Stream_ParallelSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				testutil.SkipIfTooLarge1M(t, tier.Size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genInts(tier.Size)
				ch := Slice(freshCtx(), data).Parallel().Worker(conc).Stream(func(ctx context.Context, v int) (int, error) {
					return v * 2, nil
				}, 0)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream error: %v", res.Err)
					}
					count++
				}
				if count != tier.Size {
					t.Fatalf("Stream: expected %d results, got %d", tier.Size, count)
				}
			})
		}
	}
}

// ============================================================
// Section 12: 终端 MapBatch 测试（SliceBuilder / ParallelSlice）
// ============================================================

func TestSlice_Chain_MapBatch_SliceBuilder(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Chunk(100).MapBatch(func(ctx context.Context, chunk []int) (int, error) {
				sum := 0
				for _, v := range chunk {
					sum += v
				}
				return sum, nil
			})
			if r.Err() {
				t.Fatalf("MapBatch error: %v", r.Error())
			}
			if r.Len() <= 0 {
				t.Fatal("MapBatch: expected non-empty results")
			}
		})
	}
}

func TestSlice_Chain_MapBatch_ParallelSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Parallel().Chunk(100).MapBatch(func(ctx context.Context, chunk []int) (int, error) {
				sum := 0
				for _, v := range chunk {
					sum += v
				}
				return sum, nil
			})
			if r.Err() {
				t.Fatalf("MapBatch error: %v", r.Error())
			}
			if r.Len() <= 0 {
				t.Fatal("MapBatch: expected non-empty results")
			}
		})
	}
}

// ============================================================
// Section 13: 终端 ForEachBatch 测试（SliceBuilder / ParallelSlice）
// ============================================================

func TestSlice_Chain_ForEachBatch_SliceBuilder(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			var processed int64
			r := Slice(freshCtx(), data).Chunk(100).ForEachBatch(func(ctx context.Context, chunk []int) error {
				atomic.AddInt64(&processed, int64(len(chunk)))
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEachBatch error: %v", r.Error())
			}
			if processed != int64(tier.Size) {
				t.Fatalf("ForEachBatch: expected %d, got %d", tier.Size, processed)
			}
		})
	}
}

func TestSlice_Chain_ForEachBatch_ParallelSlice(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			var processed int64
			r := Slice(freshCtx(), data).Parallel().Chunk(100).ForEachBatch(func(ctx context.Context, chunk []int) error {
				atomic.AddInt64(&processed, int64(len(chunk)))
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEachBatch error: %v", r.Error())
			}
			if processed != int64(tier.Size) {
				t.Fatalf("ForEachBatch: expected %d, got %d", tier.Size, processed)
			}
		})
	}
}

// ============================================================
// Section 14: SliceResult 操作测试（通过 Map 终端方法返回）
// ============================================================

func TestSlice_Chain_Result_Error(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Error() != nil {
		t.Fatalf("Error: expected nil, got %v", r.Error())
	}
}

func TestSlice_Chain_Result_IsOk_IsErr(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if !r.Ok() {
		t.Fatal("Ok: expected true")
	}
	if r.Err() {
		t.Fatal("Err: expected false")
	}
}

func TestSlice_Chain_Result_Values(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	vals := r.Values()
	if len(vals) != 3 || vals[0] != 2 || vals[1] != 4 || vals[2] != 6 {
		t.Fatalf("Values: unexpected %v", vals)
	}
}

func TestSlice_Chain_Result_Errors(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		if v == 2 {
			return 0, errTestSentinel
		}
		return v, nil
	})
	errs := r.Errors()
	if len(errs) != 1 {
		t.Fatalf("Errors: expected 1 error, got %d", len(errs))
	}
}

func TestSlice_Chain_Result_FailValues(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		if v == 2 {
			return 0, errTestSentinel
		}
		return v, nil
	})
	fvs := r.FailValues()
	if len(fvs) != 1 {
		t.Fatalf("FailValues: expected 1, got %d", len(fvs))
	}
}

func TestSlice_Chain_Result_Results(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	results := r.Results()
	if len(results) != 3 {
		t.Fatalf("Results: expected 3, got %d", len(results))
	}
}

func TestSlice_Chain_Result_Must(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	vals := r.Must()
	if len(vals) != 3 {
		t.Fatalf("Must: expected 3, got %d", len(vals))
	}
}

func TestSlice_Chain_Result_Unwrap(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	vals, err := r.Unwrap()
	if err != nil {
		t.Fatalf("Unwrap error: %v", err)
	}
	if len(vals) != 3 {
		t.Fatalf("Unwrap: expected 3, got %d", len(vals))
	}
}

func TestSlice_Chain_Result_Len(t *testing.T) {
	r := Slice(freshCtx(), genInts(100)).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Len() != 100 {
		t.Fatalf("Len: expected 100, got %d", r.Len())
	}
}

func TestSlice_Chain_Result_First(t *testing.T) {
	r := Slice(freshCtx(), []int{10, 20, 30}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	v, found := r.First()
	if !found || v != 10 {
		t.Fatalf("First: expected (10, true), got (%d, %v)", v, found)
	}
}

// ============================================================
// Section 15: ForEachResult 操作测试（通过 ForEach 终端方法返回）
// ============================================================

func TestSlice_Chain_ForEachResult_Error(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).ForEach(func(ctx context.Context, v int) error {
		return nil
	})
	if r.Error() != nil {
		t.Fatalf("Error: expected nil, got %v", r.Error())
	}
}

func TestSlice_Chain_ForEachResult_IsOk_IsErr(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).ForEach(func(ctx context.Context, v int) error {
		return nil
	})
	if !r.Ok() {
		t.Fatal("Ok: expected true")
	}
	if r.Err() {
		t.Fatal("Err: expected false")
	}
}

func TestSlice_Chain_ForEachResult_Total(t *testing.T) {
	r := Slice(freshCtx(), genInts(1000)).ForEach(func(ctx context.Context, v int) error {
		return nil
	})
	if r.Total() != 1000 {
		t.Fatalf("Total: expected 1000, got %d", r.Total())
	}
}

func TestSlice_Chain_ForEachResult_FailCount(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).ForEach(func(ctx context.Context, v int) error {
		if v == 2 {
			return errTestSentinel
		}
		return nil
	})
	if r.FailCount() != 1 {
		t.Fatalf("FailCount: expected 1, got %d", r.FailCount())
	}
}

func TestSlice_Chain_ForEachResult_SuccessCount(t *testing.T) {
	r := Slice(freshCtx(), []int{1, 2, 3}).ForEach(func(ctx context.Context, v int) error {
		if v == 2 {
			return errTestSentinel
		}
		return nil
	})
	if r.SuccessCount() != 2 {
		t.Fatalf("SuccessCount: expected 2, got %d", r.SuccessCount())
	}
}

// ============================================================
// Section 16: 构建器与配置测试（Construction / ModeSwitch / Config）
// ============================================================

func TestSlice_Chain_Builder_Slice(t *testing.T) {
	data := []int{1, 2, 3}
	sb := Slice(freshCtx(), data)
	if sb == nil {
		t.Fatal("Slice: expected non-nil builder")
	}
	if sb.Len() != 3 {
		t.Fatalf("Slice: expected len 3, got %d", sb.Len())
	}
}

func TestSlice_Chain_SliceWith(t *testing.T) {
	data := []int{1, 2, 3}
	sb := SliceWith[string](freshCtx(), data)
	if sb == nil {
		t.Fatal("SliceWith: expected non-nil builder")
	}
	r := sb.Map(func(ctx context.Context, v int) (string, error) {
		return strconv.Itoa(v), nil
	})
	vals := r.Values()
	if len(vals) != 3 || vals[0] != "1" {
		t.Fatalf("SliceWith: unexpected result %v", vals)
	}
}

func TestSlice_Chain_Serial(t *testing.T) {
	data := []int{1, 2, 3}
	ss := Slice(freshCtx(), data).Serial()
	if ss == nil {
		t.Fatal("Serial: expected non-nil SerialSlice")
	}
	vals := ss.Values()
	if len(vals) != 3 {
		t.Fatalf("Serial: expected 3 values, got %d", len(vals))
	}
}

func TestSlice_Chain_Parallel(t *testing.T) {
	data := []int{1, 2, 3}
	ps := Slice(freshCtx(), data).Parallel()
	if ps == nil {
		t.Fatal("Parallel: expected non-nil ParallelSlice")
	}
	vals := ps.Values()
	if len(vals) != 3 {
		t.Fatalf("Parallel: expected 3 values, got %d", len(vals))
	}
}

func TestSlice_Chain_ToSerial(t *testing.T) {
	data := []int{1, 2, 3}
	ss := Slice(freshCtx(), data).Parallel().ToSerial()
	vals := ss.Values()
	if len(vals) != 3 {
		t.Fatalf("ToSerial: expected 3 values, got %d", len(vals))
	}
}

func TestSlice_Chain_ToParallel(t *testing.T) {
	data := []int{1, 2, 3}
	ps := Slice(freshCtx(), data).Serial().ToParallel()
	vals := ps.Values()
	if len(vals) != 3 {
		t.Fatalf("ToParallel: expected 3 values, got %d", len(vals))
	}
}

// ── 配置方法测试 ──

func TestSlice_Chain_Concurrency(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).Worker(4).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Concurrency: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("Concurrency: expected 1000, got %d", r.Len())
	}
}

func TestSlice_Chain_DefaultConcurrency(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).DefaultWorker().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultConcurrency: %v", r.Error())
	}
}

func TestSlice_Chain_DefaultPool(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).DefaultPool().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultPool: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("DefaultPool: expected 1000, got %d", r.Len())
	}
}

func TestSlice_Chain_Pool(t *testing.T) {
	p := pool.NewPool[int](16)
	defer p.Close()
	data := genInts(1000)
	r := Slice(freshCtx(), data).Pool(p).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Pool: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("Pool: expected 1000, got %d", r.Len())
	}
}

func TestSlice_Chain_PoolAuto(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).PoolAuto(pool.NewPool[int](16)).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("PoolAuto: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("PoolAuto: expected 1000, got %d", r.Len())
	}
}

func TestSlice_Chain_Timeout(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).Timeout(5 * time.Second).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Timeout: %v", r.Error())
	}
}

func TestSlice_Chain_DefaultTimeout(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).DefaultTimeout().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultTimeout: %v", r.Error())
	}
}

func TestSlice_Chain_FailFast(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).FailFast().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("FailFast: %v", r.Error())
	}
}

func TestSlice_Chain_DefaultFailFast(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).DefaultFailFast().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultFailFast: %v", r.Error())
	}
}

func TestSlice_Chain_NoFailFast(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).Parallel().NoFailFast().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("NoFailFast: %v", r.Error())
	}
}

func TestSlice_Chain_Shards(t *testing.T) {
	data := genInts(10000)
	r := Slice(freshCtx(), data).Shards(8).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Shards: %v", r.Error())
	}
	if r.Len() != 10000 {
		t.Fatalf("Shards: expected 10000, got %d", r.Len())
	}
}

func TestSlice_Chain_DefaultShard(t *testing.T) {
	data := genInts(10000)
	r := Slice(freshCtx(), data).DefaultShard().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultShard: %v", r.Error())
	}
	if r.Len() != 10000 {
		t.Fatalf("DefaultShard: expected 10000, got %d", r.Len())
	}
}

func TestSlice_Chain_Chunk(t *testing.T) {
	data := genInts(10000)
	r := Slice(freshCtx(), data).Chunk(100).MapBatch(func(ctx context.Context, chunk []int) (int, error) {
		return len(chunk), nil
	})
	if r.Err() {
		t.Fatalf("Chunk: %v", r.Error())
	}
}

func TestSlice_Chain_DefaultChunk(t *testing.T) {
	data := genInts(1000)
	r := Slice(freshCtx(), data).DefaultChunk().MapBatch(func(ctx context.Context, chunk []int) (int, error) {
		return len(chunk), nil
	})
	if r.Err() {
		t.Fatalf("DefaultChunk: %v", r.Error())
	}
}

func TestSlice_Chain_Buf(t *testing.T) {
	data := genInts(1000)
	ch := Slice(freshCtx(), data).Buf(2048).Stream(func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 1000 {
		t.Fatalf("Buf: expected 1000, got %d", count)
	}
}

func TestSlice_Chain_DefaultBuf(t *testing.T) {
	data := genInts(1000)
	ch := Slice(freshCtx(), data).DefaultBuf().Stream(func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 1000 {
		t.Fatalf("DefaultBuf: expected 1000, got %d", count)
	}
}

func TestSlice_Chain_Logger(t *testing.T) {
	data := genInts(100)
	r := Slice(freshCtx(), data).Logger(&testLogger{}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Logger: %v", r.Error())
	}
	core.SetLogger(nil)
}

func TestSlice_Chain_DefaultLogger(t *testing.T) {
	data := genInts(100)
	r := Slice(freshCtx(), data).DefaultLogger().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultLogger: %v", r.Error())
	}
}

// ============================================================
// Section 17: 高并发下并行 + FailFast 压力测试
// ============================================================

func TestSlice_Chain_FailFast_Stress(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			var called int64
			r := Slice(freshCtx(), data).Worker(64).FailFast().Map(func(ctx context.Context, v int) (int, error) {
				atomic.AddInt64(&called, 1)
				if v == tier.Size/2 {
					return 0, errTestSentinel
				}
				return v, nil
			})
			if r.Error() == nil {
				t.Fatal("FailFast: expected error but got nil")
			}
			if called >= int64(tier.Size) && tier.Size > 0 {
				t.Logf("FailFast: early exit verified, called=%d/%d", called, tier.Size)
			}
		})
	}
}

// ============================================================
// Section 18: 极限高并发多策略组合压力测试
// 说明：
//   - short 模式：每档数据都运行，并发度固定在 8，快速验证并发正确性
//   - 完整模式：每档数据全并发矩阵（基础 64/128 + 极限 256/512/1024）
//   - race 检测：go test -race -run "TestSlice_Chain_HighConcurrency" -count=1 -timeout 30m ./...
// ============================================================

func TestSlice_Chain_HighConcurrency_Map(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(8).Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
		return
	}
	basicConc := []int{64, 128}
	extremeConc := []int{256, 512, 1024}
	for _, tier := range testutil.AllTiers {
		for _, conc := range basicConc {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range extremeConc {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
}

func TestSlice_Chain_HighConcurrency_Pool_Map(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				p := pool.NewPool[int](8)
				r := Slice(freshCtx(), data).PoolAuto(p).Worker(8).Map(func(ctx context.Context, v int) (int, error) {
					return v, nil
				})
				if r.Err() {
					t.Fatalf("Pool Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
		return
	}
	basicConc := []int{32, 64}
	extremeConc := []int{128, 256}
	for _, tier := range testutil.AllTiers {
		for _, conc := range basicConc {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				p := pool.NewPool[int](conc)
				r := Slice(freshCtx(), data).PoolAuto(p).Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
					return v, nil
				})
				if r.Err() {
					t.Fatalf("Pool Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range extremeConc {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				p := pool.NewPool[int](conc)
				r := Slice(freshCtx(), data).PoolAuto(p).Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
					return v, nil
				})
				if r.Err() {
					t.Fatalf("Pool Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
}

func TestSlice_Chain_HighConcurrency_Shards_Map(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_s8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Shards(8).Map(func(ctx context.Context, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Shards Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
		return
	}
	basicShards := []int{8, 16, 32}
	extremeShards := []int{64, 128}
	for _, tier := range testutil.AllTiers {
		for _, shards := range basicShards {
			name := fmt.Sprintf("%s_s%d", tier.Name, shards)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Shards(shards).Map(func(ctx context.Context, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Shards Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
	for _, tier := range testutil.AllTiers {
		for _, shards := range extremeShards {
			name := fmt.Sprintf("%s_s%d", tier.Name, shards)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Shards(shards).Map(func(ctx context.Context, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Shards Map error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
}

func TestSlice_Chain_HighConcurrency_Stream(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8_b256", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				ch := Slice(freshCtx(), data).Worker(8).Buf(256).Stream(func(ctx context.Context, v int) (int, error) {
					return v, nil
				}, 0)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream error: %v", res.Err)
					}
					count++
				}
				if count != tier.Size {
					t.Fatalf("Stream: expected %d results, got %d", tier.Size, count)
				}
			})
		}
		return
	}
	basicConc := []int{64, 128}
	extremeConc := []int{256, 512, 1024}
	bufSizes := []int{0, 1024, 8192}
	for _, tier := range testutil.AllTiers {
		for _, conc := range basicConc {
			for _, buf := range bufSizes {
				name := fmt.Sprintf("%s_c%d_b%d", tier.Name, conc, buf)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					data := genInts(tier.Size)
					ch := Slice(freshCtx(), data).Worker(conc).Buf(buf).Stream(func(ctx context.Context, v int) (int, error) {
						return v, nil
					}, 0)
					count := 0
					for res := range ch {
						if res.Err != nil {
							t.Fatalf("Stream error: %v", res.Err)
						}
						count++
					}
					if count != tier.Size {
						t.Fatalf("Stream: expected %d results, got %d", tier.Size, count)
					}
				})
			}
		}
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range extremeConc {
			for _, buf := range bufSizes {
				name := fmt.Sprintf("%s_c%d_b%d", tier.Name, conc, buf)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					data := genInts(tier.Size)
					ch := Slice(freshCtx(), data).Worker(conc).Buf(buf).Stream(func(ctx context.Context, v int) (int, error) {
						return v, nil
					}, 0)
					count := 0
					for res := range ch {
						if res.Err != nil {
							t.Fatalf("Stream error: %v", res.Err)
						}
						count++
					}
					if count != tier.Size {
						t.Fatalf("Stream: expected %d results, got %d", tier.Size, count)
					}
				})
			}
		}
	}
}

func TestSlice_Chain_HighConcurrency_ForEach_Stress(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(8).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("ForEach: expected %d calls, got %d", tier.Size, counter)
				}
				if r.SuccessCount() != int64(tier.Size) {
					t.Fatalf("SuccessCount: expected %d, got %d", tier.Size, r.SuccessCount())
				}
			})
		}
		return
	}
	basicConc := []int{64, 128}
	extremeConc := []int{256, 512, 1024}
	for _, tier := range testutil.AllTiers {
		for _, conc := range basicConc {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("ForEach: expected %d calls, got %d", tier.Size, counter)
				}
				if r.SuccessCount() != int64(tier.Size) {
					t.Fatalf("SuccessCount: expected %d, got %d", tier.Size, r.SuccessCount())
				}
			})
		}
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range extremeConc {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("ForEach: expected %d calls, got %d", tier.Size, counter)
				}
				if r.SuccessCount() != int64(tier.Size) {
					t.Fatalf("SuccessCount: expected %d, got %d", tier.Size, r.SuccessCount())
				}
			})
		}
	}
}

// ── Map + Timeout 极限并发（命中 mapTO 路径）──

func TestSlice_Chain_HighConcurrency_Map_Timeout(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(8).Timeout(30 * time.Second).Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+Timeout error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(conc).Timeout(30 * time.Second).Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+Timeout error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
}

// ── Map + FailFast 极限并发（命中 mapParallelFF 路径）──

func TestSlice_Chain_HighConcurrency_Map_FailFast(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(8).FailFast().Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+FailFast error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512, 1024} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(conc).FailFast().Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+FailFast error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
}

// ── Map + Timeout + FailFast 极限并发（命中 mapTOFF 路径）──

func TestSlice_Chain_HighConcurrency_Map_TimeoutFailFast(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(8).Timeout(30 * time.Second).FailFast().Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+TO+FF error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(conc).Timeout(30 * time.Second).FailFast().Map(func(ctx context.Context, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+TO+FF error: %v", r.Error())
				}
				if r.Len() != tier.Size {
					t.Fatalf("expected %d results, got %d", tier.Size, r.Len())
				}
			})
		}
	}
}

// ── ForEach + Timeout 极限并发（命中 eachTO 路径）──

func TestSlice_Chain_HighConcurrency_ForEach_Timeout(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(8).Timeout(30 * time.Second).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+Timeout error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(conc).Timeout(30 * time.Second).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+Timeout error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
	}
}

// ── ForEach + FailFast 极限并发（命中 eachParFF 路径）──

func TestSlice_Chain_HighConcurrency_ForEach_FailFast(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(8).FailFast().ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+FailFast error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512, 1024} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(conc).FailFast().ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+FailFast error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
	}
}

// ── MapBatch 极限并发（命中 mapChunked 路径）──

func TestSlice_Chain_HighConcurrency_MapBatch(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(8).Chunk(100).MapBatch(func(ctx context.Context, batch []int) (int, error) {
					sum := 0
					for _, v := range batch {
						sum += v
					}
					return sum, nil
				})
				if r.Err() {
					t.Fatalf("MapBatch error: %v", r.Error())
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				r := Slice(freshCtx(), data).Worker(conc).Chunk(100).MapBatch(func(ctx context.Context, batch []int) (int, error) {
					sum := 0
					for _, v := range batch {
						sum += v
					}
					return sum, nil
				})
				if r.Err() {
					t.Fatalf("MapBatch error: %v", r.Error())
				}
			})
		}
	}
}

// ── ForEachBatch 极限并发（命中 eachChunked 路径）──

func TestSlice_Chain_HighConcurrency_ForEachBatch(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(8).Chunk(100).ForEachBatch(func(ctx context.Context, batch []int) error {
					atomic.AddInt64(&counter, int64(len(batch)))
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEachBatch error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d, got %d", tier.Size, counter)
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(conc).Chunk(100).ForEachBatch(func(ctx context.Context, batch []int) error {
					atomic.AddInt64(&counter, int64(len(batch)))
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEachBatch error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d, got %d", tier.Size, counter)
				}
			})
		}
	}
}

// ── Stream + FailFast 极限并发（命中 streamFF 路径）──

func TestSlice_Chain_HighConcurrency_Stream_FailFast(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				ch := Slice(freshCtx(), data).Worker(8).FailFast().Stream(func(ctx context.Context, v int) (int, error) {
					return v, nil
				}, 256)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream+FF error: %v", res.Err)
					}
					count++
				}
				if count != tier.Size {
					t.Fatalf("Stream+FF: expected %d results, got %d", tier.Size, count)
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				ch := Slice(freshCtx(), data).Worker(conc).FailFast().Stream(func(ctx context.Context, v int) (int, error) {
					return v, nil
				}, 256)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream+FF error: %v", res.Err)
					}
					count++
				}
				if count != tier.Size {
					t.Fatalf("Stream+FF: expected %d results, got %d", tier.Size, count)
				}
			})
		}
	}
}

// ── ForEach + Shards 极限并发（命中 eachSharded 路径）──

func TestSlice_Chain_HighConcurrency_ForEach_Shards(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_s8", tier.Name), func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Shards(8).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+Shards error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, shards := range []int{32, 64, 128} {
			name := fmt.Sprintf("%s_s%d", tier.Name, shards)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Shards(shards).ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+Shards error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
	}
}

// ── ForEach + Timeout + FailFast 极限并发（命中 eachTOFF 路径）──

func TestSlice_Chain_HighConcurrency_ForEach_TOFF(t *testing.T) {
	if testing.Short() {
		for _, tier := range testutil.AllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.Name), func(t *testing.T) {
				testutil.SkipIfTooLarge1M(t, tier.Size)
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(8).Timeout(30 * time.Second).FailFast().ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+TO+FF error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
		return
	}
	for _, tier := range testutil.AllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genInts(tier.Size)
				var counter int64
				r := Slice(freshCtx(), data).Worker(conc).Timeout(30 * time.Second).FailFast().ForEach(func(ctx context.Context, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+TO+FF error: %v", r.Error())
				}
				if counter != int64(tier.Size) {
					t.Fatalf("expected %d calls, got %d", tier.Size, counter)
				}
			})
		}
	}
}

// ============================================================
// Section 19: 串行链路（SerialSlice 配置方法）完整性测试
// ============================================================

func TestSlice_Chain_SerialSlice_FailFast(t *testing.T) {
	data := genInts(100)
	var called int64
	r := Slice(freshCtx(), data).Serial().FailFast().Map(func(ctx context.Context, v int) (int, error) {
		called := atomic.AddInt64(&called, 1)
		if called == 50 {
			return 0, errTestSentinel
		}
		return v, nil
	})
	if r.Error() == nil {
		t.Fatal("Serial FailFast: expected error")
	}
}

func TestSlice_Chain_SerialSlice_NoFailFast(t *testing.T) {
	data := genInts(100)
	r := Slice(freshCtx(), data).Serial().NoFailFast().Map(func(ctx context.Context, v int) (int, error) {
		if v == 50 {
			return 0, errTestSentinel
		}
		return v, nil
	})
	if r.Error() == nil {
		t.Fatal("Serial NoFailFast: still should have error in results")
	}
}

func TestSlice_Chain_SerialSlice_Timeout(t *testing.T) {
	data := genInts(100)
	r := Slice(freshCtx(), data).Serial().Timeout(5 * time.Second).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Serial Timeout: %v", r.Error())
	}
}

func TestSlice_Chain_SerialSlice_DefaultTimeout(t *testing.T) {
	data := genInts(100)
	r := Slice(freshCtx(), data).Serial().DefaultTimeout().Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Serial DefaultTimeout: %v", r.Error())
	}
}

func TestSlice_Chain_SerialSlice_Logger(t *testing.T) {
	data := genInts(10)
	r := Slice(freshCtx(), data).Serial().Logger(&testLogger{}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Serial Logger: %v", r.Error())
	}
	core.SetLogger(nil)
}

func TestSlice_Chain_SerialSlice_ToParallel(t *testing.T) {
	data := genInts(1000)
	ps := Slice(freshCtx(), data).Serial().ToParallel()
	vals := ps.Worker(16).Values()
	if len(vals) != 1000 {
		t.Fatalf("Serial->ToParallel: expected 1000 values, got %d", len(vals))
	}
}

// ============================================================
// Section 20: 并行链路（ParallelSlice 配置方法 + 模式切换）完整性测试
// ============================================================

func TestSlice_Chain_ParallelSlice_AllConfig(t *testing.T) {
	data := genInts(5000)
	ps := Slice(freshCtx(), data).Parallel().
		Worker(16).
		FailFast().
		Timeout(10 * time.Second).
		Shards(8)
	_ = ps.NoFailFast()
	_ = ps.DefaultTimeout()
	_ = ps.DefaultWorker()
	_ = ps.DefaultShard()
	_ = ps.DefaultPool()
	_ = ps.PoolAuto(pool.NewPool[int](8))
	_ = ps.DefaultChunk()
	vals := ps.Values()
	if len(vals) != 5000 {
		t.Fatalf("Parallel AllConfig: expected 5000, got %d", len(vals))
	}
}

func TestSlice_Chain_ParallelSlice_ToSerial(t *testing.T) {
	data := genInts(1000)
	ss := Slice(freshCtx(), data).Parallel().ToSerial()
	vals := ss.Values()
	if len(vals) != 1000 {
		t.Fatalf("Parallel->ToSerial: expected 1000 values, got %d", len(vals))
	}
}

// ============================================================
// Section 21: 边界条件测试
// ============================================================

func TestSlice_Chain_EmptySlice(t *testing.T) {
	for _, op := range []string{
		"Sort", "StableSort", "Reverse", "Filter", "DeleteFunc", "Compact",
		"Dedup", "Clip", "Take", "Drop", "SliceRange", "Shuffle", "Repeat",
	} {
		t.Run(op, func(t *testing.T) {
			sb := Slice(freshCtx(), []int{})
			switch op {
			case "Sort":
				sb.Sort(cmpInt)
			case "StableSort":
				sb.StableSort(cmpInt)
			case "Reverse":
				sb.Reverse()
			case "Filter":
				sb.Filter(func(v int) bool { return true })
			case "DeleteFunc":
				sb.DeleteFunc(func(v int) bool { return true })
			case "Compact":
				sb.Compact(func(a, b int) bool { return a == b })
			case "Dedup":
				sb.Dedup(func(a, b int) bool { return a == b })
			case "Clip":
				sb.Clip()
			case "Take":
				sb.Take(1)
			case "Drop":
				sb.Drop(1)
			case "SliceRange":
				sb.SliceRange(0, 0)
			case "Shuffle":
				sb.Shuffle()
			case "Repeat":
				sb.Repeat(3)
			}
			if sb.Len() > 0 && op != "Repeat" {
				t.Fatalf("%s on empty: expected len 0, got %d", op, sb.Len())
			}
		})
	}
}

func TestSlice_Chain_EmptyMap(t *testing.T) {
	r := Slice(freshCtx(), []int{}).Map(func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("empty Map should not error: %v", r.Error())
	}
	if r.Len() != 0 {
		t.Fatalf("empty Map: expected 0 results, got %d", r.Len())
	}
}

func TestSlice_Chain_EmptyForEach(t *testing.T) {
	r := Slice(freshCtx(), []int{}).ForEach(func(ctx context.Context, v int) error {
		return nil
	})
	if r.Err() {
		t.Fatalf("empty ForEach should not error: %v", r.Error())
	}
	if r.Total() != 0 {
		t.Fatalf("empty ForEach: expected 0 total, got %d", r.Total())
	}
}

func TestSlice_Chain_EmptyStream(t *testing.T) {
	ch := Slice(freshCtx(), []int{}).Stream(func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 0 {
		t.Fatalf("empty Stream: expected 0 results, got %d", count)
	}
}

func TestSlice_Chain_EmptyReduce(t *testing.T) {
	sum, err := Slice(freshCtx(), []int{}).Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("empty Reduce error: %v", err)
	}
	if sum != 0 {
		t.Fatalf("empty Reduce: expected 0, got %d", sum)
	}
}

func TestSlice_Chain_FirstLast_Empty(t *testing.T) {
	sb := Slice(freshCtx(), []int{})
	if sb.IsEmpty() != true {
		t.Fatal("empty: IsEmpty should be true")
	}
	v, ok := sb.First()
	if ok {
		t.Fatalf("empty First: expected false, got val=%d", v)
	}
	v, ok = sb.Last()
	if ok {
		t.Fatalf("empty Last: expected false, got val=%d", v)
	}
}

// ============================================================
// Section 22: 并发安全测试 - 多 goroutine 同时读取构建器
// ============================================================

func TestSlice_Chain_ConcurrentReads(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			sb := Slice(freshCtx(), data)
			var wg sync.WaitGroup
			workers := 32
			errCh := make(chan error, workers)
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = sb.Len()
					_ = sb.IsEmpty()
					_, _ = sb.First()
					_, _ = sb.Last()
					_ = sb.Values()
					_ = sb.Clone()
				}()
			}
			wg.Wait()
			close(errCh)
		})
	}
}

func TestSlice_Chain_ConcurrentMapCalls(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			t.Parallel()
			var wg sync.WaitGroup
			workers := 16
			var successCount int64
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					data := genInts(tier.Size / workers)
					if len(data) == 0 {
						return
					}
					r := Slice(freshCtx(), data).Worker(8).Map(func(ctx context.Context, v int) (int, error) {
						return v, nil
					})
					if r.Err() {
						t.Errorf("concurrent Map error: %v", r.Error())
						return
					}
					atomic.AddInt64(&successCount, 1)
				}()
			}
			wg.Wait()
			if successCount != int64(workers) {
				t.Fatalf("concurrent Map: expected %d successes, got %d", workers, successCount)
			}
		})
	}
}

// ============================================================
// Section 23: Race 检测专项测试（go test -race）
// 说明：每条测试覆盖一个并发执行路径，在 -race 下可检测数据竞争。
//       所有 4 档数据规模全覆盖，每档都有独立的 race 检测。
//       运行方式：
//         快速（推荐 CI）：go test -race -short -run "TestSlice_Chain_Race" -count=1 -timeout 10m ./...
//         完整：          go test -race -run "TestSlice_Chain_Race" -count=1 -timeout 30m ./...
// ============================================================

func TestSlice_Chain_Race_Map(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			var sum int64
			r := Slice(freshCtx(), data).Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
				atomic.AddInt64(&sum, int64(v))
				return v, nil
			})
			if r.Err() {
				t.Fatalf("Race Map: %v", r.Error())
			}
			if r.Len() != tier.Size {
				t.Fatalf("Race Map: expected %d, got %d", tier.Size, r.Len())
			}
		})
	}
}

func TestSlice_Chain_Race_ForEach(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			var counter int64
			var errCount int64
			r := Slice(freshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, v int) error {
				atomic.AddInt64(&counter, 1)
				if v%2 == 0 {
					atomic.AddInt64(&errCount, 1)
				}
				return nil
			})
			_ = r.Error()
			if r.Total() != int64(tier.Size) {
				t.Fatalf("Race ForEach Total: expected %d, got %d", tier.Size, r.Total())
			}
			if counter != int64(tier.Size) {
				t.Fatalf("Race ForEach counter: expected %d, got %d", tier.Size, counter)
			}
		})
	}
}

func TestSlice_Chain_Race_FailFast(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			var called int64
			r := Slice(freshCtx(), data).Worker(conc).FailFast().Map(func(ctx context.Context, v int) (int, error) {
				idx := atomic.AddInt64(&called, 1)
				if idx == 100 {
					return 0, errTestSentinel
				}
				return v, nil
			})
			if r.Error() == nil {
				t.Fatal("Race FailFast: expected error but got nil")
			}
		})
	}
}

func TestSlice_Chain_Race_Stream(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			ch := Slice(freshCtx(), data).Worker(conc).Buf(4096).Stream(func(ctx context.Context, v int) (int, error) {
				return v, nil
			}, 0)
			count := 0
			for res := range ch {
				if res.Err != nil {
					t.Fatalf("Race Stream: %v", res.Err)
				}
				count++
			}
			if count != tier.Size {
				t.Fatalf("Race Stream: expected %d, got %d", tier.Size, count)
			}
		})
	}
}

func TestSlice_Chain_Race_PoolShared(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			p := pool.NewPool[int](16)
			r := Slice(freshCtx(), data).PoolAuto(p).Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
				return v, nil
			})
			if r.Err() {
				t.Fatalf("Race PoolShared: %v", r.Error())
			}
			if r.Len() != tier.Size {
				t.Fatalf("Race PoolShared: expected %d, got %d", tier.Size, r.Len())
			}
		})
	}
}

func TestSlice_Chain_Race_Shards(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Worker(conc).Shards(16).Map(func(ctx context.Context, v int) (int, error) {
				return v * 2, nil
			})
			if r.Err() {
				t.Fatalf("Race Shards: %v", r.Error())
			}
			if r.Len() != tier.Size {
				t.Fatalf("Race Shards: expected %d, got %d", tier.Size, r.Len())
			}
		})
	}
}

func TestSlice_Chain_Race_MapBatch(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Worker(conc).Chunk(100).MapBatch(func(ctx context.Context, batch []int) (int, error) {
				sum := 0
				for _, v := range batch {
					sum += v
				}
				return sum, nil
			})
			if r.Err() {
				t.Fatalf("Race MapBatch: %v", r.Error())
			}
			if r.Len() == 0 {
				t.Fatal("Race MapBatch: expected non-zero results")
			}
		})
	}
}

func TestSlice_Chain_Race_ForEachBatch(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			var counter int64
			r := Slice(freshCtx(), data).Worker(conc).Chunk(100).ForEachBatch(func(ctx context.Context, batch []int) error {
				atomic.AddInt64(&counter, int64(len(batch)))
				return nil
			})
			if r.Err() {
				t.Fatalf("Race ForEachBatch: %v", r.Error())
			}
			if counter != int64(tier.Size) {
				t.Fatalf("Race ForEachBatch: expected %d, got %d", tier.Size, counter)
			}
		})
	}
}

func TestSlice_Chain_Race_Reduce(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			sum, err := Slice(freshCtx(), data).Worker(conc).Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
				return acc + v, nil
			})
			if err != nil {
				t.Fatalf("Race Reduce: %v", err)
			}
			_ = sum
		})
	}
}

func TestSlice_Chain_Race_SerialMode(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Serial().Map(func(ctx context.Context, v int) (int, error) {
				return v, nil
			})
			if r.Err() {
				t.Fatalf("Race Serial: %v", r.Error())
			}
			if r.Len() != tier.Size {
				t.Fatalf("Race Serial: expected %d, got %d", tier.Size, r.Len())
			}
		})
	}
}

func TestSlice_Chain_Race_SerialMode_FailFast(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Serial().FailFast().Map(func(ctx context.Context, v int) (int, error) {
				return v, nil
			})
			if r.Err() {
				t.Fatalf("Race Serial FF: %v", r.Error())
			}
			if r.Len() != tier.Size {
				t.Fatalf("Race Serial FF: expected %d, got %d", tier.Size, r.Len())
			}
		})
	}
}

func TestSlice_Chain_Race_ParallelMode(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			r := Slice(freshCtx(), data).Parallel().Worker(conc).Map(func(ctx context.Context, v int) (int, error) {
				return v, nil
			})
			if r.Err() {
				t.Fatalf("Race Parallel: %v", r.Error())
			}
			if r.Len() != tier.Size {
				t.Fatalf("Race Parallel: expected %d, got %d", tier.Size, r.Len())
			}
		})
	}
}

func TestSlice_Chain_Race_ForEach_Shards(t *testing.T) {
	shards := 32
	if testing.Short() {
		shards = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			data := genInts(tier.Size)
			var counter int64
			r := Slice(freshCtx(), data).Shards(shards).ForEach(func(ctx context.Context, v int) error {
				atomic.AddInt64(&counter, 1)
				return nil
			})
			if r.Err() {
				t.Fatalf("Race ForEach Shards: %v", r.Error())
			}
			if r.SuccessCount() != int64(tier.Size) {
				t.Fatalf("Race ForEach Shards: expected %d, got %d", tier.Size, r.SuccessCount())
			}
		})
	}
}

func TestSlice_Chain_Race_ForEach_TOFF(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)
			data := genInts(tier.Size)
			var counter int64
			r := Slice(freshCtx(), data).Worker(conc).Timeout(30 * time.Second).FailFast().ForEach(func(ctx context.Context, v int) error {
				atomic.AddInt64(&counter, 1)
				return nil
			})
			if r.Err() {
				t.Fatalf("Race ForEach TOFF: %v", r.Error())
			}
			if r.SuccessCount() != int64(tier.Size) {
				t.Fatalf("Race ForEach TOFF: expected %d, got %d", tier.Size, r.SuccessCount())
			}
		})
	}
}

// ============================================================
// 辅助类型
// ============================================================

type testLogger struct{}

func (l *testLogger) Log(_ context.Context, _ core.LogLevel, _ string, _ ...core.LogField) {}
func (l *testLogger) With(_ ...core.LogField) core.Logger                                  { return l }
func (l *testLogger) WithContext(ctx context.Context) context.Context                      { return ctx }
