package paperclpa

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"example.com/shardinglpa/shared"
)

func ShardAllocation(datasetDir string, numberOfShards int, epochNumber int, graph *shared.Graph,
	rho int, alpha float64, beta float64, tau int, mode string) *shared.EpochResult {

	//start := time.Now()

	// Prepare rand
	randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))

	// TESTING - randomGen = rand.New(rand.NewSource(0))

	// TESTING
	fmt.Println("Start of Epoch ", epochNumber)

	// Create a new graph if it was not passed in to function
	if graph == nil {
		graph = &shared.Graph{
			Vertices:       make(map[string]*shared.Vertex),
			NumberOfShards: numberOfShards,
		}
	}

	// Generate the filename dynamically based on the epoch value
	filename := fmt.Sprintf("%sepoch_%d.csv", datasetDir, epochNumber)

	// Load the CSV data once per epoch
	rows, err := shared.ReadCSV(filename)
	if err != nil {
		fmt.Printf("Error reading CSV %s: %v\n", filename, err)
	}

	// Initialise the graph with random shard labels for new vertices
	graph = InitialiseGraphFromRows(rows, graph, randomGen)

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

	// TESTING - Print the graph for debugging
	/*
		// Create a slice to hold the vertex IDs
		var ids []string
		for id := range graph.Vertices {
			ids = append(ids, id)
		}
		// Sort the vertex IDs alphabetically
		sort.Strings(ids)
		fmt.Println(("\nGraph at start"))
		for _, id := range ids {
			vertex := graph.Vertices[id]
			fmt.Printf("Vertex %s (Shard %d):\n", id, vertex.Label)
			for neighbour, weight := range vertex.Edges {
				fmt.Printf("  -> %s (weight %d)\n", neighbour, weight)
			}
		}
	*/
	// END OF TESTING

	// Work out workloads for the first time this epoch
	graph.ShardWorkloads = calculateShardWorkloads(graph)

	// Now that preparation is ready, the actual CLPA can run and the results recorded
	result := runCLPA(alpha, beta, tau, rho, graph, randomGen, mode)

	// Add inactive vertices back to graph for the next epoch
	for id, vertex := range inactiveVertices {
		graph.Vertices[id] = vertex
	}

	result.Graph = graph

	//result.Duration = x

	return result

}

func runCLPA(alpha, beta float64, tau int, rho int, graph *shared.Graph,
	randomGen *rand.Rand, mode string) *shared.EpochResult {

	convergenceIter := -1 // Default value if no convergence

	// Carry out CLPA iterations
	for iter := 0; iter < tau; iter++ {

		// create a map with all old labels - meaning labels of vertices before current CLPA iteration
		oldLabels := make(map[string]int)
		for id, vertex := range graph.Vertices {
			oldLabels[id] = vertex.Label
		}

		// Perform an iteration of CLPA according to the mode (sync or async)
		if mode == "async" {
			clpaIterationAsync(graph, beta, randomGen, rho)
		} else if mode == "sync" {
			clpaIterationSync(graph, beta, randomGen, rho)
		} else {
			log.Printf("Invalid mode: '%s'. Must be 'sync' or 'async'.\n", mode)
			return &shared.EpochResult{
				Seed:            -1,
				Fitness:         -1,
				ConvergenceIter: -1,
			}
		}

		// TESTING - Call CLPA in sync mode
		//CLPAIterationSync(graph, beta, randomGen, rho)

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

	workloadImbalance, crossShardWorkload, fitness := shared.CalculateFitness(graph, alpha)
	return &shared.EpochResult{
		Seed:               -1,
		Fitness:            fitness,
		WorkloadImbalance:  workloadImbalance,
		CrossShardWorkload: crossShardWorkload,
		ConvergenceIter:    convergenceIter,
	}

}
