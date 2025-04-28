package mylpa

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"

	"example.com/shardinglpa/shared"
)

/*
Function to perform shard allocation

Inputs:
dataset path (for low or high arrival rate dataset),
number of shards,
number of current epoch,
graph from previous epoch,
the weight of the objectives in the fitness function (alpha),
the weight of cross-shard vs workload imbalance in score function (beta),
number of iterations of the algorithm (tau),
number of times/threshold each vertex is allowed to update its label (rho),
the random seeds to be used for parallel runs

Output:
the epoch results for each separate parallel run,
the vertices with no transcations in this epoch
*/
func ShardAllocation(datasetDir string, numberOfShards int, epochNumber int,
	graph *shared.Graph, alpha float64, beta float64, tau int, rho int, seeds []int64) ([]*shared.EpochResult, map[string]*shared.Vertex) {

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Buffered channel to collect results from each seed's run
	results := make(chan *shared.EpochResult, len(seeds))

	// Create a new graph if it was not passed in to function
	if graph == nil {
		graph = &shared.Graph{
			Vertices:       make(map[string]*shared.Vertex),
			NumberOfShards: numberOfShards,
		}
	}

	// Generate the filename dynamically based on the epoch value
	filename := fmt.Sprintf("%sepoch_%d.csv", datasetDir, epochNumber)

	// Load the CSV data once
	rows, err := shared.ReadCSV(filename)
	if err != nil {
		log.Printf("Error reading CSV %s: %v\n", filename, err)
		return nil, nil
	}

	// Update the graph based on the rows of the current epoch
	graph = updateGraphFromRows(rows, graph)

	//The following process can be done on the graph before a copy is provided to each go routine:
	/* inactiveVertices refers to vertices which have no edges in this particular epoch.
	These will be dealt with by being removed since CLPA should ignore them, and then
	after CLPA is run, added back to graph. */
	inactiveVertices := make(map[string]*shared.Vertex)
	for id, vertex := range graph.Vertices {
		if len(vertex.Edges) == 0 {

			// Store the vertex in temporary map before removing them from the graph
			inactiveVertices[id] = vertex
			delete(graph.Vertices, id)
		}
	}

	// Iterate through each seed
	for _, seed := range seeds {

		// Increment the WaitGroup counter by 1 to track a new goroutine
		wg.Add(1)

		// Launch a new goroutine to run the CLPA for a specific seed
		go func(seed int64) {

			// Decrease the WaitGroup counter when the goroutine finishes
			defer wg.Done()

			// Use unique random generator for each parallel run
			randomGen := rand.New(rand.NewSource(seed))

			// Deep copy the graph for this goroutine
			localGraph := shared.DeepCopyGraph(graph)

			// Initialise the graph with random shard labels for new vertices
			localGraph = initialiseNewVertices(localGraph, randomGen)

			// Work out workloads for the first time this epoch
			localGraph.ShardWorkloads = calculateShardWorkloads(localGraph)

			// Now that preparation is ready, the actual CLPA can run and the results recorded
			epochResult := runClpa(alpha, beta, tau, rho, localGraph, randomGen, seed)

			epochResult.Graph = localGraph

			// Send the epoch results for this seed to the results channel
			results <- epochResult

		}(seed)
	}

	// Wait for all Goroutines to finish and close the results channel
	wg.Wait()

	close(results)

	// Collect all the seeds results for the epoch into a slice
	var seedsResultsForEpoch []*shared.EpochResult
	for result := range results {
		seedsResultsForEpoch = append(seedsResultsForEpoch, result)
	}

	// Return the collected results of all the seeds for the epoch
	return seedsResultsForEpoch, inactiveVertices
}

// The CLPA function
func runClpa(alpha float64, beta float64, tau int, rho int, graph *shared.Graph,
	randomGen *rand.Rand, seed int64) *shared.EpochResult {

	//FAILED (attempt to implement passive)
	/*
		// Map to store those vertices that may change their label on the next run
		pendingVertices := make(map[string]struct{})

		// Add each vertex to the pending list initially
		for id := range graph.Vertices {
			pendingVertices[id] = struct{}{}
		}
	*/
	//FAILED ATTEMPT

	// Ensure all vertices have initialized LabelVotes
	for _, vertex := range graph.Vertices {
		if vertex.LabelVotes == nil {
			vertex.LabelVotes = make(map[int]int)
			vertex.LabelVotes[vertex.Label] = 1 // Give an initial vote for current label
		}
	}

	convergenceIter := -1 // Default value if no convergence within iterations

	minIterations := 5 // Run at least 5 iterations before checking convergence

	// Carry out CLPA iterations
	for iter := 0; iter < tau; iter++ {

		// create a map with all old labels - meaning labels of vertices before current CLPA iteration
		oldLabels := make(map[string]int)
		for id, vertex := range graph.Vertices {
			oldLabels[id] = vertex.Label
		}

		// Perform an iteration of CLPA while keeping track of which vertices are pending
		clpaIteration(graph, beta, randomGen, rho) //pendingVertices) //FAILED ATTEMPT

		// CLPA iterations should stop once convergence is reached
		converged := true
		for id, vertex := range graph.Vertices {
			if oldLabels[id] != vertex.Label {
				converged = false
				break
			}
		}
		if converged && iter+1 >= minIterations {
			convergenceIter = iter + 1
			break
		}

	}

	// Calculate the workload imbalance, number of cross shard transactions and fitness of the partitioning
	workloadImbalance, crossShardWorkload, fitness := shared.CalculateFitness(graph, alpha)

	// Return the results of the epoch
	return &shared.EpochResult{
		Seed:               seed,
		Fitness:            fitness,
		WorkloadImbalance:  workloadImbalance,
		CrossShardWorkload: crossShardWorkload,
		ConvergenceIter:    convergenceIter,
	}

}

// The function that performs an iteration through all vertices and assigns shards
func clpaIteration(graph *shared.Graph, beta float64, randomGen *rand.Rand, rho int) {
	// FAILED ATTEMPT
	//, pendingVertices map[string]struct{}) {

	// Get a random order to use for this CLPA iteration
	sortedVertices := setVerticesOrder(graph, randomGen)

	// Iterate through each vertex in some order
	for _, vertex := range sortedVertices {

		//FAILED ATTEMPT#
		/*
			// Skip this vertex if it is not in the pending list (non-passive)
			if _, isPending := pendingVertices[vertex.ID]; !isPending {
				continue
			}
		*/
		//FAILED ATTEMPT

		// Calculate the score of shards with respect to current vertex
		scores := calculateScores(graph, vertex, beta)

		// Get the ID of the best shard with respect to current vertex
		bestShard := getBestShard(scores, randomGen)

		// Instead of moving immediately, add a vote
		vertex.LabelVotes[bestShard]++

		// Find the label with the most votes
		winningShard, maxVotes := vertex.Label, vertex.LabelVotes[vertex.Label]
		for shard, votes := range vertex.LabelVotes {
			if votes > maxVotes {
				winningShard = shard
				maxVotes = votes
			}
		}

		// If winning shard is different and has enough dominance, then move
		voteMargin := 1 // <-- configurable: need at least 1 more votes than current label
		if winningShard != vertex.Label && (vertex.LabelVotes[winningShard]-vertex.LabelVotes[vertex.Label] >= voteMargin) {
			moveVertex(graph, vertex, winningShard, rho)
		}

		//FAILED ATTEMPT
		/*
			// Check if vertex has become passive, if so delete from pendingVertices, otherwise add it
			if isPassive(vertex, graph) {
				delete(pendingVertices, vertex.ID)
			} else {
				pendingVertices[vertex.ID] = struct{}{}
			}

			// Check if neighbouring vertices of current vertex have become passive
			for neighborID := range vertex.Edges {
				neighbor := graph.Vertices[neighborID]

				// If neighbouring vertex has become passive, delete it from the pending vertices if it is there
				// Else, add it to the pending vertices (it is overwritten if it is present already)
				if isPassive(neighbor, graph) {
					delete(pendingVertices, neighborID)
				} else {
					pendingVertices[neighborID] = struct{}{}
				}
			}
		*/
		//FAILED ATTEMPT
	}
}

// FAILED ATTEMPT
func isPassive(vertex *shared.Vertex, graph *shared.Graph) bool {
	for neighborID := range vertex.Edges {
		if graph.Vertices[neighborID].Label != vertex.Label {
			return false
		}
	}
	return true
}

// Function to get seeds from file starting from a specific index
func GetSeeds(filename string, num int, nextSeedIndex int) ([]int64, error) {

	// Read the CSV file
	rows, err := shared.ReadCSV(filename)
	if err != nil {
		return nil, fmt.Errorf("could not read seed CSV: %v", err)
	}

	var seeds []int64

	// Iterate from nextSeedIndex to extract seeds
	for i := nextSeedIndex; i < len(rows); i++ {

		row := rows[i]

		// Skip empty lines
		if len(row) == 0 {
			continue
		}

		if len(seeds) >= num {
			break
		}

		// Convert the string value to int64
		seed, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid seed value at row %d: %v", i+1, err)
		}

		// Append the seed to the result slice
		seeds = append(seeds, seed)
	}

	if len(seeds) < num {
		return nil, fmt.Errorf("not enough seeds starting from index %d: needed %d, found %d", nextSeedIndex, num, len(seeds))
	}

	return seeds, nil
}
