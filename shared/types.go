package shared

// The Vertex struct represents an account
type Vertex struct {
	ID                 string         // Address of the vertex used as unique identifier
	Label              int            // Current shard ID of where the vertex resides
	Edges              map[string]int // Map of neighbour vertex IDs to edge weights
	LabelUpdateCounter int            // Number of times the vertex has updated its label
	NewLabel           int            // Used only for synchronous updating mode
	LabelVotes         map[int]int    // Map used for memory voting mechanism
}

// The Graph struct
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
	ConvergenceIter    int // -1 means no convergence, else set to the iteration number of convergence
	Graph              *Graph
	IterationsInfo     *IterationsInfo // Used only in convergence test
}

// Struct to hold results of an iteration in convergence test
type IterationsInfo struct {
	LabelChanged []bool    // indicates whether any label changed during an iteration
	Fitness      []float64 // the fitness of the partitioning in an iteration
}
