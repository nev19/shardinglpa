package clpaparallel

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"example.com/shardinglpa/shared"
)

func ShardAllocation(datasetDir string, numberOfShards int, numberOfParallelRuns int, epochNumber int,
	graph *shared.Graph, alpha float64, beta float64, tau int, rho int) ([]*shared.EpochResult, map[string]*shared.Vertex) {

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Buffered channel to collect results from each seed's run
	results := make(chan *shared.EpochResult, numberOfParallelRuns)

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

	// Iterate through the runs
	for parallelRun := 0; parallelRun < numberOfParallelRuns; parallelRun++ {

		// Increment the WaitGroup counter by 1 to track a new goroutine
		wg.Add(1)

		// Create a new variable to capture the current loop value avoiding closure capture issues
		//  in the goroutine below
		pr := parallelRun

		// Launch a new goroutine to run the task for a specific seed
		go func() {

			// Decrease the WaitGroup counter when the goroutine finishes
			defer wg.Done()

			// Use unique random generator for each thread
			// run is used as Offset since time may be same on multiple threads

			randomGen := rand.New(rand.NewSource(time.Now().UnixNano() + int64(pr)))
			// TESTING - randomGen = rand.New(rand.NewSource(0))
			// TESTING - seed = int64(1)

			// TESTING
			//fmt.Println("Start of Epoch ", epochNumber)

			// Deep copy the graph for this goroutine
			localGraph := deepCopyGraph(graph)

			// Initialise the graph with random shard labels for new vertices
			localGraph = initialiseNewVertices(localGraph, randomGen)

			// Work out workloads for the first time this epoch
			localGraph.ShardWorkloads = calculateShardWorkloads(localGraph)

			// Now that preparation is ready, the actual CLPA can run and the results recorded
			epochResult := runClpa(alpha, beta, tau, rho, localGraph, randomGen)

			epochResult.Graph = localGraph

			//epochResult.Duration = x

			// Send the epoch results for this seed to the results channel
			results <- epochResult
		}()
	}

	// Wait for all Goroutines to finish and close the results channel
	wg.Wait()
	/*
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)


			fmt.Println("-------- GC / Memory Stats --------")
			fmt.Printf("Alloc = %v MiB\n", mem.Alloc/1024/1024)
			fmt.Printf("TotalAlloc = %v MiB\n", mem.TotalAlloc/1024/1024)
			fmt.Printf("Sys = %v MiB\n", mem.Sys/1024/1024)
			fmt.Printf("NumGC = %v\n", mem.NumGC)
			fmt.Printf("Last GC Pause = %v ms\n", mem.PauseNs[(mem.NumGC+255)%256]/1e6)
	*/

	close(results)

	// Collect all the seeds results for the epoch into a slice
	var seedsResultsForEpoch []*shared.EpochResult
	for result := range results {
		seedsResultsForEpoch = append(seedsResultsForEpoch, result)
	}

	// Return the collected results of all the seeds for the epoch
	return seedsResultsForEpoch, inactiveVertices
}

func deepCopyGraph(original *shared.Graph) *shared.Graph {
	copy := &shared.Graph{
		Vertices:       make(map[string]*shared.Vertex),
		NumberOfShards: original.NumberOfShards,
	}

	// Copy vertices
	for id, v := range original.Vertices {
		copy.Vertices[id] = &shared.Vertex{
			ID:                 v.ID,
			Label:              v.Label,
			Edges:              v.Edges,
			LabelUpdateCounter: v.LabelUpdateCounter,
			NewLabel:           v.NewLabel,
		}
	}

	return copy
}

func runClpa(alpha float64, beta float64, tau int, rho int, graph *shared.Graph,
	randomGen *rand.Rand) *shared.EpochResult {

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

		// TESTING - Check for convergence

		// If convergenceIter is not -1, then it was already found that the algorithm converged
		// CLPA iterations should still continue, as stipulated in the paper
		if convergenceIter == -1 {
			converged := true
			for id, vertex := range graph.Vertices {
				if oldLabels[id] != vertex.Label {
					converged = false
					break
				}
			}
			if converged {
				// TESTING - fmt.Println("\n\nXXXXXXXXXXXXXXXXXXXXX Converged at iteration (0-based): ", iter)
				// Record the iteration number when convergence occurred (1-based)
				convergenceIter = iter + 1
				// TESTING - used to make deterministic: break
			}
		}

	}

	// TESTING - print workloads at end
	/*
		fmt.Println("\n\nWorkloads at End:")

		// print workload of each shard (from incremental)
		for shard_id, workload := range graph.ShardWorkloads {
			fmt.Printf("\nThe shard %v has workload: %v", shard_id, workload)
		}
		// print workload of each shard (by rework)
		fmt.Println("\n\nWORKLOADS by rework")
		for shard_id, workload := range workloads {
			fmt.Printf("\nThe shard %v has workload: %v", shard_id, workload)
		}
	*/

	// Calculate the workload imabalnce, number of cross shard transactions and fitness of the partitioning
	workloadImbalance, crossShardWorkload, fitness := shared.CalculateFitness(graph, alpha)

	// Return the results of the epoch
	return &shared.EpochResult{
		Seed:               -1, // set to -1 since no seed is used, the algorithm is random
		Fitness:            fitness,
		WorkloadImbalance:  workloadImbalance,
		CrossShardWorkload: crossShardWorkload,
		ConvergenceIter:    convergenceIter,
	}

}

// The main CLPA function that iterates through all vertices and assigns shards
func clpaIteration(graph *shared.Graph, beta float64, randomGen *rand.Rand, rho int) {

	// Get a random order to use for this CLPA iteration
	sortedVertices := setVerticesOrder(graph, randomGen)

	// Iterate through each vertex in some order
	for _, vertex := range sortedVertices {

		// Calculate the score of shards with respect to current vertex
		scores := calculateScores(graph, vertex, beta)

		// TESTING - PRINT OUT SCORES
		/*
			fmt.Println("\nCLPA on VERTEX: ", i, vertex.ID)

			// Dereference and print each value of scores
			fmt.Print("SCORES for this vertex:")
			for _, ptr := range scores {
				if ptr != nil {
					fmt.Print(" ", *ptr, " ") // Access the value via pointer
				} else {
					fmt.Print(" <nil> ")
				}
			}
		*/

		// Get the ID of the best shard with respect to current vertex
		bestShard := getBestShard(scores, randomGen)
		//fmt.Println("Winner: ", bestShard)

		// Move current vertex to new best shard
		moveVertex(graph, vertex, bestShard, rho)

	}

}
