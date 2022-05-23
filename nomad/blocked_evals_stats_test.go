package nomad

import (
	"testing"
	"time"

	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

func now(year int) time.Time {
	return time.Date(2000+year, 1, 2, 3, 4, 5, 6, time.UTC)
}

func TestBlockResourceSummary_Copy(t *testing.T) {
	a := &BlockedResourcesSummary{
		Timestamp: now(1),
		CPU:       100,
		MemoryMB:  200,
	}

	c := a.Copy()
	c.Timestamp = now(2)
	c.CPU = 333
	c.MemoryMB = 444

	// a not modified
	require.Equal(t, now(1), a.Timestamp)
	require.Equal(t, 100, a.CPU)
	require.Equal(t, 200, a.MemoryMB)
}

func TestBlockResourceSummary_Add(t *testing.T) {
	now1 := now(1)
	now2 := now(2)
	a := &BlockedResourcesSummary{
		Timestamp: now1,
		CPU:       600,
		MemoryMB:  256,
	}

	b := &BlockedResourcesSummary{
		Timestamp: now2,
		CPU:       250,
		MemoryMB:  128,
	}

	result := a.Add(b)

	// a not modified
	require.Equal(t, 600, a.CPU)
	require.Equal(t, 256, a.MemoryMB)
	require.Equal(t, now1, a.Timestamp)

	// b not modified
	require.Equal(t, 250, b.CPU)
	require.Equal(t, 128, b.MemoryMB)
	require.Equal(t, now2, b.Timestamp)

	// result is a + b, using timestamp from b
	require.Equal(t, 850, result.CPU)
	require.Equal(t, 384, result.MemoryMB)
	require.Equal(t, now2, result.Timestamp)
}

func TestBlockResourceSummary_Add_nil(t *testing.T) {
	now1 := now(1)
	b := &BlockedResourcesSummary{
		Timestamp: now1,
		CPU:       250,
		MemoryMB:  128,
	}

	// zero + b == b
	result := (*BlockedResourcesSummary)(nil).Add(b)
	require.Equal(t, now1, result.Timestamp)
	require.Equal(t, 250, result.CPU)
	require.Equal(t, 128, result.MemoryMB)
}

func TestBlockResourceSummary_Subtract(t *testing.T) {
	now1 := now(1)
	now2 := now(2)
	a := &BlockedResourcesSummary{
		Timestamp: now1,
		CPU:       600,
		MemoryMB:  256,
	}

	b := &BlockedResourcesSummary{
		Timestamp: now2,
		CPU:       250,
		MemoryMB:  120,
	}

	result := a.Subtract(b)

	// a not modified
	require.Equal(t, 600, a.CPU)
	require.Equal(t, 256, a.MemoryMB)
	require.Equal(t, now1, a.Timestamp)

	// b not modified
	require.Equal(t, 250, b.CPU)
	require.Equal(t, 120, b.MemoryMB)
	require.Equal(t, now2, b.Timestamp)

	// result is a + b, using timestamp from b
	require.Equal(t, 350, result.CPU)
	require.Equal(t, 136, result.MemoryMB)
	require.Equal(t, now2, result.Timestamp)
}

func TestBlockResourceSummary_IsZero(t *testing.T) {
	now1 := now(1)

	// cpu and mem zero, timestamp is ignored
	require.True(t, (&BlockedResourcesSummary{
		Timestamp: now1,
		CPU:       0,
		MemoryMB:  0,
	}).IsZero())

	// cpu non-zero
	require.False(t, (&BlockedResourcesSummary{
		Timestamp: now1,
		CPU:       1,
		MemoryMB:  0,
	}).IsZero())

	// mem non-zero
	require.False(t, (&BlockedResourcesSummary{
		Timestamp: now1,
		CPU:       0,
		MemoryMB:  1,
	}).IsZero())
}

func TestBlockResourceStats_New(t *testing.T) {
	a := NewBlockedResourcesStats()
	require.NotNil(t, a.ByJob)
	require.Empty(t, a.ByJob)
	require.NotNil(t, a.ByNode)
	require.Empty(t, a.ByNode)
}

var (
	id1 = structs.NamespacedID{
		ID:        "1",
		Namespace: "one",
	}

	id2 = structs.NamespacedID{
		ID:        "2",
		Namespace: "two",
	}

	node1 = node{
		dc:    "dc1",
		class: "alpha",
	}

	node2 = node{
		dc:    "dc1",
		class: "beta",
	}
)

func TestBlockResourceStats_Copy(t *testing.T) {
	now1 := now(1)

	a := NewBlockedResourcesStats()
	a.ByJob = map[structs.NamespacedID]*BlockedResourcesSummary{
		id1: {
			Timestamp: now1,
			CPU:       100,
			MemoryMB:  256,
		},
	}
	a.ByNode = map[node]*BlockedResourcesSummary{
		node1: {
			Timestamp: now1,
			CPU:       300,
			MemoryMB:  333,
		},
	}

	c := a.Copy()
	c.ByJob[id1].CPU = 999
	c.ByNode[node1].CPU = 999

	require.Equal(t, 100, a.ByJob[id1].CPU)
	require.Equal(t, 300, a.ByNode[node1].CPU)
}

func TestBlockResourcesStats_Add(t *testing.T) {
	a := NewBlockedResourcesStats()
	a.ByJob = map[structs.NamespacedID]*BlockedResourcesSummary{
		id1: {Timestamp: now(1), CPU: 111, MemoryMB: 222},
	}
	a.ByNode = map[node]*BlockedResourcesSummary{
		node1: {Timestamp: now(2), CPU: 333, MemoryMB: 444},
	}

	b := NewBlockedResourcesStats()
	b.ByJob = map[structs.NamespacedID]*BlockedResourcesSummary{
		id1: {Timestamp: now(3), CPU: 200, MemoryMB: 300},
		id2: {Timestamp: now(4), CPU: 400, MemoryMB: 500},
	}
	b.ByNode = map[node]*BlockedResourcesSummary{
		node1: {Timestamp: now(5), CPU: 600, MemoryMB: 700},
		node2: {Timestamp: now(6), CPU: 800, MemoryMB: 900},
	}

	t.Run("a add b", func(t *testing.T) {
		result := a.Add(b)

		require.Equal(t, map[structs.NamespacedID]*BlockedResourcesSummary{
			id1: {Timestamp: now(3), CPU: 311, MemoryMB: 522},
			id2: {Timestamp: now(4), CPU: 400, MemoryMB: 500},
		}, result.ByJob)

		require.Equal(t, map[node]*BlockedResourcesSummary{
			node1: {Timestamp: now(5), CPU: 933, MemoryMB: 1144},
			node2: {Timestamp: now(6), CPU: 800, MemoryMB: 900},
		}, result.ByNode)
	})

	// make sure we handle zeros in both directions
	// and timestamps originate from rhs
	t.Run("b add a", func(t *testing.T) {
		result := b.Add(a)
		require.Equal(t, map[structs.NamespacedID]*BlockedResourcesSummary{
			id1: {Timestamp: now(1), CPU: 311, MemoryMB: 522},
			id2: {Timestamp: now(4), CPU: 400, MemoryMB: 500},
		}, result.ByJob)

		require.Equal(t, map[node]*BlockedResourcesSummary{
			node1: {Timestamp: now(2), CPU: 933, MemoryMB: 1144},
			node2: {Timestamp: now(6), CPU: 800, MemoryMB: 900},
		}, result.ByNode)
	})
}

func TestBlockedResourcesStats_Subtract(t *testing.T) {
	a := NewBlockedResourcesStats()
	a.ByJob = map[structs.NamespacedID]*BlockedResourcesSummary{
		id1: {Timestamp: now(1), CPU: 100, MemoryMB: 100},
		id2: {Timestamp: now(2), CPU: 200, MemoryMB: 200},
	}
	a.ByNode = map[node]*BlockedResourcesSummary{
		node1: {Timestamp: now(3), CPU: 300, MemoryMB: 300},
		node2: {Timestamp: now(4), CPU: 400, MemoryMB: 400},
	}

	b := NewBlockedResourcesStats()
	b.ByJob = map[structs.NamespacedID]*BlockedResourcesSummary{
		id1: {Timestamp: now(5), CPU: 10, MemoryMB: 11},
		id2: {Timestamp: now(6), CPU: 12, MemoryMB: 13},
	}
	b.ByNode = map[node]*BlockedResourcesSummary{
		node1: {Timestamp: now(7), CPU: 14, MemoryMB: 15},
		node2: {Timestamp: now(8), CPU: 16, MemoryMB: 17},
	}

	result := a.Subtract(b)

	// id1
	require.Equal(t, now(5), result.ByJob[id1].Timestamp)
	require.Equal(t, 90, result.ByJob[id1].CPU)
	require.Equal(t, 89, result.ByJob[id1].MemoryMB)

	// id2
	require.Equal(t, now(6), result.ByJob[id2].Timestamp)
	require.Equal(t, 188, result.ByJob[id2].CPU)
	require.Equal(t, 187, result.ByJob[id2].MemoryMB)

	// node1
	require.Equal(t, now(7), result.ByNode[node1].Timestamp)
	require.Equal(t, 286, result.ByNode[node1].CPU)
	require.Equal(t, 285, result.ByNode[node1].MemoryMB)

	// node2
	require.Equal(t, now(8), result.ByNode[node2].Timestamp)
	require.Equal(t, 384, result.ByNode[node2].CPU)
	require.Equal(t, 383, result.ByNode[node2].MemoryMB)
}
