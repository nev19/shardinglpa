package mylpa

import (
	"fmt"
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
		fmt.Printf("Error reading CSV %s: %v\n", filename, err)
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

	convergenceIter := -1 // Default value if no convergence within iterations

	// Carry out CLPA iterations
	for iter := 0; iter < tau; iter++ {

		// create a map with all old labels - meaning labels of vertices before current CLPA iteration
		oldLabels := make(map[string]int)
		for id, vertex := range graph.Vertices {
			oldLabels[id] = vertex.Label
		}

		// Perform an iteration of CLPA
		clpaIteration(graph, beta, randomGen, rho)

		// CLPA iterations should stop once convergence is reached
		converged := true
		for id, vertex := range graph.Vertices {
			if oldLabels[id] != vertex.Label {
				converged = false
				break
			}
		}
		if converged {

			// Record the iteration number when convergence occurred (1-based)
			convergenceIter = iter + 1

			// Stop CLPA iterations in case of convergence
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

	// Get a random order to use for this CLPA iteration
	sortedVertices := setVerticesOrder(graph, randomGen)

	// Iterate through each vertex in some order
	for _, vertex := range sortedVertices {

		// Calculate the score of shards with respect to current vertex
		scores := calculateScores(graph, vertex, beta)

		// Get the ID of the best shard with respect to current vertex
		bestShard := getBestShard(scores, randomGen)

		// Move current vertex to new best shard
		moveVertex(graph, vertex, bestShard, rho)

	}
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
