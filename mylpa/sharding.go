package mylpa

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
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
the number of iterations of LPA (tau)

Output:
The epoch results for each seed
*/
func ShardAllocation(datasetDir string, numShards int, numberOfRuns int, numberOfEpochs int,
	rho int, alpha float64, beta float64, tau int) []shared.SeedResults {

	// Get the random seeds
	seeds, err := getSeeds("mylpa/seeds.csv", numberOfRuns)
	if err != nil {
		log.Fatalf("Failed to load seeds: %v", err)
	}

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Buffered channel to collect results from each seed's run
	results := make(chan shared.SeedResults, numberOfRuns)

	// Iterate through each seed
	for _, seed := range seeds {

		// Increment the WaitGroup counter by 1 to track a new goroutine
		wg.Add(1)

		// Launch a new goroutine to run the task for a specific seed
		go func(seed int64) {

			// Decrease the WaitGroup counter when the goroutine finishes
			defer wg.Done()

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
				result := runCLPA(seed, alpha, beta, tau, rho, graph, inactiveVertices, randomGen)

				// Add the time program ran
				result.Duration = time.Since(start)

				// Append the result for the current epoch to the epochResults slice
				epochResults = append(epochResults, result)
			}

			// Send all epoch results for this seed to the results channel as a single SeedResults struct
			results <- shared.SeedResults{Seed: seed, Results: epochResults}
		}(seed)
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

func runCLPA(seed int64, alpha, beta float64, tau int, rho int, graph *shared.Graph, inactiveVertices map[string]*shared.Vertex, randomGen *rand.Rand) shared.Result {

	convergenceIter := -1 // Default value if no convergence

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

		// CLPA iterations should stop once convergence is reached
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

			// Stop iterations
			break
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

// Function to get seeds from file
func getSeeds(filename string, num int) ([]int64, error) {

	// Read the CSV file
	rows, err := shared.ReadCSV(filename)
	if err != nil {
		return nil, fmt.Errorf("could not read seed CSV: %v", err)
	}

	var seeds []int64

	// Iterate over all rows and values to extract seed numbers
	for _, row := range rows {
		for _, value := range row {

			if len(seeds) >= num {
				return seeds, nil
			}

			// Convert the string value to int64
			seed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid seed value: %v", err)
			}

			// Append the parsed seed to the result slice
			seeds = append(seeds, seed)
		}
	}

	// Check if we gathered enough seeds
	if len(seeds) < num {
		return nil, fmt.Errorf("not enough seeds: needed %d, found %d", num, len(seeds))
	}

	// Return the slice of seeds
	return seeds, nil
}

// TESTING - Function to generate sequential seeds used to compare with old implementation
// this is never useful since I compare my implementation (using seeds from file) with their implementation
// which is totally random (no seeds)
/*
func generateSeeds(num int) []int64 {
	seeds := make([]int64, num)
	for i := 0; i < num; i++ {
		seeds[i] = int64(i)
	}
	return seeds
}
*/
