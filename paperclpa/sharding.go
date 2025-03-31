package paperclpa

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"time"

	"example.com/shardinglpa/shared"
)

func PaperShardAllocation() {

	// TESTING - cpu profiling
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil)) // Start pprof server
	}()

	// Get the number of logical CPU cores available on the machine
	numCores := runtime.NumCPU()

	// Set the number of parallel runs
	numRuns := numCores

	// This ensures full CPU core utilization during parallel execution
	runtime.GOMAXPROCS(numCores)

	fmt.Printf("Number of CPU cores available: %d\n", numCores)
	fmt.Printf("Number of runs: %d\n", numRuns)

	// Start clock
	start := time.Now()

	// The number of epochs
	numberOfEpochs := 3

	// The weight of the objectives in the fitness function
	alpha := 0.5

	// The number of shards
	numShards := 8

	// The weight of cross-shard vs workload imbalance (0 to 1)
	beta := 0.5

	// The number of iterations of CLPA
	tau := 100

	// The number of times (threshold) each vertex is allowed to update its label
	rho := 50

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Buffered channel to collect results from each seed's run
	results := make(chan shared.SeedResults, numRuns)

	// Iterate through the runs
	for run := 0; run < numRuns; run++ {

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

				// Generate the filename dynamically based on the epoch value
				filename := fmt.Sprintf("output/epoch%d.csv", epoch)

				// Load the CSV data once per epoch
				rows, err := shared.ReadCSV(filename)
				if err != nil {
					fmt.Printf("Error reading CSV %s: %v\n", filename, err)
					return
				}

				// Initialize the graph with random shard labels for new vertices
				graph := InitializeGraphFromRows(rows, graph, randomGen)

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
				graph.ShardWorkloads = CalculateShardWorkloads(graph)

				// Now that preparation is ready, the actual CLPA can run and the results recorded
				result := runCLPA(seed, alpha, beta, tau, rho, graph, inactiveVertices, randomGen)

				// Append the result for the current epoch to the epochResults slice
				epochResults = append(epochResults, result)
			}

			// Send all epoch results for this seed to the results channel as a single SeedResults struct
			results <- shared.SeedResults{Seed: seed, Results: epochResults}
		}()
	}

	// Wait for all Goroutines to finish
	wg.Wait()

	// Close the results channel to signal that no more values will be sent
	close(results)

	// Collect results into a slice
	var groupedResults []shared.SeedResults
	for i := 0; i < numRuns; i++ {
		seedResult := <-results
		groupedResults = append(groupedResults, seedResult)
	}

	// Sort by Seed
	sort.Slice(groupedResults, func(i, j int) bool {
		return groupedResults[i].Seed < groupedResults[j].Seed
	})

	fmt.Println("\nFinal Results Per Seed and Epoch:")
	fmt.Printf("Seed\tEpoch\tFitness\t\tWorkload Imbalance\tCross Shard Workload\tConvergence Iterations\n")

	for _, seedResult := range groupedResults {
		for epochIdx, result := range seedResult.Results {
			fmt.Printf("%d\t%d\t%.3f\t\t%.3f\t\t\t%d\t\t\t%d\n",
				seedResult.Seed, epochIdx+1, result.Fitness, result.WorkloadImbalance, result.CrossShardWorkload, result.ConvergenceIter)
		}
	}

	fmt.Println("\nGlobal Epoch Statistics:")

	for epochIdx := 0; epochIdx < numberOfEpochs; epochIdx++ {
		var epochResults []shared.Result

		// Collect results for this epoch from all seeds
		for _, seedResult := range groupedResults {

			// Safety check to not go over bounds
			if epochIdx < len(seedResult.Results) {
				epochResults = append(epochResults, seedResult.Results[epochIdx])
			}
		}

		// Safety check
		if len(epochResults) == 0 {
			continue
		}

		// Initialize min, max, total
		min := epochResults[0]
		max := epochResults[0]
		total := 0.0

		for _, r := range epochResults {
			total += r.Fitness
			if r.Fitness < min.Fitness {
				min = r
			}
			if r.Fitness > max.Fitness {
				max = r
			}
		}

		mean := total / float64(len(epochResults))

		// Compute median
		sort.Slice(epochResults, func(i, j int) bool {
			return epochResults[i].Fitness < epochResults[j].Fitness
		})

		var median float64
		mid := len(epochResults) / 2
		if len(epochResults)%2 == 0 {
			median = (epochResults[mid-1].Fitness + epochResults[mid].Fitness) / 2.0
		} else {
			median = epochResults[mid].Fitness
		}

		// Percent difference: how much smaller min is compared to mean
		percentSmallerThanMean := ((mean - min.Fitness) / mean) * 100

		// Variance and standard deviation
		var variance float64
		for _, r := range epochResults {
			diff := r.Fitness - mean
			variance += diff * diff
		}
		variance /= float64(len(epochResults))
		stdDev := math.Sqrt(variance)

		// Print stats
		fmt.Printf("Epoch %d Stats:\n", epochIdx+1)
		fmt.Printf("  ▸ Min Fitness: %.3f (Seed %d)\n", min.Fitness, min.Seed)
		fmt.Printf("  ▸ Max Fitness: %.3f (Seed %d)\n", max.Fitness, max.Seed)
		fmt.Printf("  ▸ Mean Fitness: %.3f\n", mean)
		fmt.Printf("  ▸ Median Fitness: %.3f\n", median)
		fmt.Printf("  ▸ Min is %.2f%% smaller than mean\n", percentSmallerThanMean)
		fmt.Printf("  ▸ Variance: %.6f\n", variance)
		fmt.Printf("  ▸ Std Dev: %.6f\n\n", stdDev)
	}

	timeRan := time.Since(start)
	fmt.Printf("\nProgram ran for %s\n", timeRan)

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
		CLPAIteration(graph, beta, randomGen, rho)

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
