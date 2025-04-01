package paperclpa

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"example.com/shardinglpa/shared"
)

/*
Function to perform shard allocation

Inputs:
the dataset (low or high arrival rate),
the number of shards,
the number of parallel runs,
the number of epochs,
the number of times/threshold each vertex is allowed to update its label (rho),
the weight of the objectives in the fitness function (alpha),
the weight of cross-shard vs workload imbalance - 0 to 1 (beta),
the number of iterations of CLPA (tau),
the mode of updating labels (async or sync)

Output:
The epoch results for each seed
*/
func ShardAllocation(datasetDir string, numShards int, numberOfRuns int, numberOfEpochs int,
	rho int, alpha float64, beta float64, tau int, mode string) []shared.SeedResults {

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Buffered channel to collect results from each seed's run
	results := make(chan shared.SeedResults, numberOfRuns)

	// Iterate through the runs
	for run := 0; run < numberOfRuns; run++ {

		// Increment the WaitGroup counter by 1 to track a new goroutine
		wg.Add(1)

		// Launch a new goroutine to run the task for a specific seed
		go func() {

			// Decrease the WaitGroup counter when the goroutine finishes
			defer wg.Done()

			// Use unique random generator for each thread
			// run is used as Offset since time may be same on multiple threads
			seed := time.Now().UnixNano() + int64(run)
			// TESTING - seed = int64(run)
			randomGen := rand.New(rand.NewSource(seed))

			// Create a new graph
			graph := &shared.Graph{
				Vertices:       make(map[string]*shared.Vertex),
				NumberOfShards: numShards,
			}

			// Slice to store epoch results for this seed
			var epochResults []shared.Result

			// Iterate over the epochs
			for epoch := 1; epoch <= numberOfEpochs; epoch++ {

				// TESTING
				fmt.Println("Start of Epoch ", epoch)

				// Start clock
				start := time.Now()

				// Generate the filename dynamically based on the epoch value
				filename := fmt.Sprintf("%sepoch_%d.csv", datasetDir, epoch)

				// Load the CSV data once per epoch
				rows, err := shared.ReadCSV(filename)
				if err != nil {
					fmt.Printf("Error reading CSV %s: %v\n", filename, err)
					return
				}

				// Initialise the graph with random shard labels for new vertices
				graph := initialiseGraphFromRows(rows, graph, randomGen)

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
				result := runCLPA(seed, alpha, beta, tau, rho, graph, inactiveVertices, randomGen, mode)

				// Add the time program ran
				result.Duration = time.Since(start)

				// Append the result for the current epoch to the epochResults slice
				epochResults = append(epochResults, result)
			}

			// Send all epoch results for this seed to the results channel as a single SeedResults struct
			results <- shared.SeedResults{Seed: seed, Results: epochResults}
		}()
	}

	// Wait for all Goroutines to finish and close the results channel
	wg.Wait()
	close(results)

	// Collect results into a slice
	var groupedResults []shared.SeedResults
	for i := 0; i < numberOfRuns; i++ {
		seedResult := <-results
		groupedResults = append(groupedResults, seedResult)
	}

	// Return the results
	return groupedResults
}

func runCLPA(seed int64, alpha, beta float64, tau int, rho int, graph *shared.Graph,
	inactiveVertices map[string]*shared.Vertex, randomGen *rand.Rand, mode string) shared.Result {

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
			return shared.Result{
				Seed:            seed,
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

	// Add inactive vertices back to graph for the next epoch
	for id, vertex := range inactiveVertices {
		graph.Vertices[id] = vertex
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
	return shared.Result{
		Seed:               seed,
		Fitness:            fitness,
		WorkloadImbalance:  workloadImbalance,
		CrossShardWorkload: crossShardWorkload,
		ConvergenceIter:    convergenceIter,
	}

}
