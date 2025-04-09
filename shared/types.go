package shared

import (
	"time"
)

// Define the Vertex structure - this represents an account
type Vertex struct {
	ID                 string         // Address of the vertex used as unique identifier
	Label              int            // Current shard ID of where the vertex resides
	Edges              map[string]int // Map of neighbour vertex IDs to edge weights
	LabelUpdateCounter int            // Number of times the vertex has updated its label
	NewLabel           int            // Used only for synchronous updating mode
}

// Define the Graph structure
type Graph struct {
	Vertices       map[string]*Vertex // Map of vertex ID to Vertex struct
	NumberOfShards int                // Total number of shards
	ShardWorkloads []int              // Current workloads of shards
}

// Struct to hold results of a single epoch
type EpochResult struct {
	Seed               int64
	Fitness            float64 // The fitness score
	WorkloadImbalance  float64
	CrossShardWorkload int
	ConvergenceIter    int           // -1 if no convergence, otherwise the iteration number
	Duration           time.Duration // The time program ran
	Graph              *Graph
}

// OLD Struct to hold the result of a single simulation run (per epoch)
type SeedResults struct {
	Seed        int64
	Graph       *Graph
	EpochResult []EpochResult
}
