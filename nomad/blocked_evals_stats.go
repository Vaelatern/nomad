package nomad

import (
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/nomad/nomad/structs"
)

// BlockedStats returns all the stats about the blocked eval tracker.
type BlockedStats struct {
	// TotalEscaped is the total number of blocked evaluations that have escaped
	// computed node classes.
	TotalEscaped int

	// TotalBlocked is the total number of blocked evaluations.
	TotalBlocked int

	// TotalQuotaLimit is the total number of blocked evaluations that are due
	// to the quota limit being reached.
	TotalQuotaLimit int

	// BlockedResources stores the amount of resources requested by blocked
	// evaluations.
	BlockedResources *BlockedResourcesStats
}

// node stores information related to nodes.
type node struct {
	dc    string
	class string
}

func (n node) String() string {
	return fmt.Sprintf("%s/%s", n.dc, n.class)
}

// NewBlockedStats returns a new BlockedStats.
func NewBlockedStats() *BlockedStats {
	return &BlockedStats{
		BlockedResources: NewBlockedResourcesStats(),
	}
}

// Block updates the stats for the blocked eval tracker with the details of the
// evaluation being blocked.
func (b *BlockedStats) Block(eval *structs.Evaluation) {
	b.TotalBlocked++
	resourceStats := generateResourceStats(eval)

	fmt.Println("Block, id:", eval.ID, "total:", b.TotalBlocked)

	fmt.Println("block, resourceStats:")
	spew.Dump(resourceStats.ByJob)

	fmt.Println("block, BlockedResources before Add:")
	spew.Dump(b.BlockedResources.ByJob)

	b.BlockedResources = b.BlockedResources.Add(resourceStats)

	fmt.Println("block, BlockedResources after Add:")
	spew.Dump(b.BlockedResources.ByJob)
}

// Unblock updates the stats for the blocked eval tracker with the details of the
// evaluation being unblocked.
func (b *BlockedStats) Unblock(eval *structs.Evaluation) {
	b.TotalBlocked--
	resourceStats := generateResourceStats(eval)

	fmt.Println("Unblock, id:", eval.ID, "total:", b.TotalBlocked)

	fmt.Println("unblock, resourceStats:")
	spew.Dump(resourceStats.ByJob)

	fmt.Println("unblock, BlockedResources before Subtract:")
	spew.Dump(b.BlockedResources.ByJob)

	b.BlockedResources = b.BlockedResources.Subtract(resourceStats)

	fmt.Println("unblock, BlockedResources after Subtract:")
	spew.Dump(b.BlockedResources.ByJob)
}

// prune deletes any key zero metric values older than the cutoff.
func (b *BlockedStats) prune(cutoff time.Time) {
	shouldPrune := func(s *BlockedResourcesSummary) bool {
		return s.Timestamp.Before(cutoff) && s.IsZero()
	}

	for k, v := range b.BlockedResources.ByJob {
		if shouldPrune(v) {
			delete(b.BlockedResources.ByJob, k)
		}
	}

	for k, v := range b.BlockedResources.ByNode {
		if shouldPrune(v) {
			delete(b.BlockedResources.ByNode, k)
		}
	}
}

// generateResourceStats returns a summary of the resources requested by the
// input evaluation.
func generateResourceStats(eval *structs.Evaluation) *BlockedResourcesStats {
	dcs := make(map[string]struct{})
	classes := make(map[string]struct{})

	resources := &BlockedResourcesSummary{
		Timestamp: time.Now().UTC(),
	}

	fmt.Println("GRS id:", eval.ID)

	for _, allocMetrics := range eval.FailedTGAllocs {

		fmt.Println(" nodes avail:", allocMetrics.NodesAvailable)

		for dc := range allocMetrics.NodesAvailable {
			fmt.Println(" set dc:", dc)
			dcs[dc] = struct{}{}
		}

		fmt.Println(" class exh:", allocMetrics.ClassExhausted)

		for class := range allocMetrics.ClassExhausted {
			fmt.Println(" set class:", class)
			classes[class] = struct{}{}
		}

		fmt.Println(" res exh:", allocMetrics.ResourcesExhausted)

		for _, r := range allocMetrics.ResourcesExhausted {
			resources.CPU += r.CPU
			resources.MemoryMB += r.MemoryMB

			fmt.Println("add cpu:", r.CPU, "mem:", r.MemoryMB, "tot_cpu:", resources.CPU, "tot_mem:", resources.MemoryMB)
		}
	}

	byJob := make(map[structs.NamespacedID]*BlockedResourcesSummary)
	nsID := structs.NewNamespacedID(eval.JobID, eval.Namespace)
	byJob[nsID] = resources

	fmt.Println("ASSIGN", nsID)
	spew.Dump(resources)

	byNodeInfo := make(map[node]*BlockedResourcesSummary)
	for dc := range dcs {
		for class := range classes {
			k := node{dc: dc, class: class}
			byNodeInfo[k] = resources
		}
	}

	return &BlockedResourcesStats{
		ByJob:  byJob,
		ByNode: byNodeInfo,
	}
}

// BlockedResourcesStats stores resources requested by blocked evaluations,
// tracked both by job and by node.
type BlockedResourcesStats struct {
	ByJob  map[structs.NamespacedID]*BlockedResourcesSummary
	ByNode map[node]*BlockedResourcesSummary
}

// NewBlockedResourcesStats returns a new BlockedResourcesStats.
func NewBlockedResourcesStats() *BlockedResourcesStats {
	return &BlockedResourcesStats{
		ByJob:  make(map[structs.NamespacedID]*BlockedResourcesSummary),
		ByNode: make(map[node]*BlockedResourcesSummary),
	}
}

// Copy returns a deep copy of the blocked resource stats.
func (b *BlockedResourcesStats) Copy() *BlockedResourcesStats {
	result := NewBlockedResourcesStats()

	for k, v := range b.ByJob {
		result.ByJob[k] = v.Copy()
	}

	for k, v := range b.ByNode {
		result.ByNode[k] = v.Copy()
	}

	return result
}

// Add returns a new BlockedResourcesStats with the values set to the current
// resource values plus the input.
func (b *BlockedResourcesStats) Add(a *BlockedResourcesStats) *BlockedResourcesStats {
	result := b.Copy()

	for k, v := range a.ByJob {
		result.ByJob[k] = b.ByJob[k].Add(v)
	}

	for k, v := range a.ByNode {
		result.ByNode[k] = b.ByNode[k].Add(v)
	}

	return result
}

// Subtract returns a new BlockedResourcesStats with the values set to the
// current resource values minus the input.
func (b *BlockedResourcesStats) Subtract(a *BlockedResourcesStats) *BlockedResourcesStats {
	result := b.Copy()

	for k, v := range a.ByJob {
		result.ByJob[k] = b.ByJob[k].Subtract(v)
	}

	for k, v := range a.ByNode {
		result.ByNode[k] = b.ByNode[k].Subtract(v)
	}

	return result
}

// BlockedResourcesSummary stores resource values for blocked evals.
type BlockedResourcesSummary struct {
	Timestamp time.Time
	CPU       int
	MemoryMB  int
}

// Copy creates a deep copy of b.
//
// b must not be nil.
func (b *BlockedResourcesSummary) Copy() *BlockedResourcesSummary {
	return &BlockedResourcesSummary{
		Timestamp: b.Timestamp,
		CPU:       b.CPU,
		MemoryMB:  b.MemoryMB,
	}
}

// Add returns a new BlockedResourcesSummary with each resource set to the
// current value plus the input.
//
// If b is nil, a copy of a is returned (i.e. 0 + a == a).
func (b *BlockedResourcesSummary) Add(a *BlockedResourcesSummary) *BlockedResourcesSummary {
	if b == nil {
		return a.Copy()
	}
	return &BlockedResourcesSummary{
		Timestamp: a.Timestamp,
		CPU:       b.CPU + a.CPU,
		MemoryMB:  b.MemoryMB + a.MemoryMB,
	}
}

// Subtract returns a new BlockedResourcesSummary with each resource set to the
// current value minus the input.
//
// b must not be nil.
func (b *BlockedResourcesSummary) Subtract(a *BlockedResourcesSummary) *BlockedResourcesSummary {
	return &BlockedResourcesSummary{
		Timestamp: a.Timestamp,
		CPU:       b.CPU - a.CPU,
		MemoryMB:  b.MemoryMB - a.MemoryMB,
	}
}

// IsZero returns true if all resource values are zero.
//
// b must not be nil.
func (b *BlockedResourcesSummary) IsZero() bool {
	return b.CPU == 0 && b.MemoryMB == 0
}
