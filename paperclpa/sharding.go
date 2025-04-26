package paperclpa

import (
	"fmt"
	"math/rand"
	"time"

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
function call to run CLPA iteration (decides updating mode)
function call to run CLPA (decides convergence mode)
function call to run scoring function (decides penalty formula)

Output:
the epoch results
*/
func ShardAllocation(datasetDir string, shards int, epoch int, graph *shared.Graph, alpha float64, beta float64,
	tau int, rho int, runClpaIter ClpaIterationMode, clpaCall ClpaCall, scoringPenalty ScoringPenalty) *shared.EpochResult {

	// Prepare rand
	randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))

	// TESTING - randomGen = rand.New(rand.NewSource(0))

	// TESTING
	//fmt.Println("Start of Epoch ", epochNumber)

	// Create a new graph if it was not passed in to function
	if graph == nil {
		graph = &shared.Graph{
			Vertices:       make(map[string]*shared.Vertex),
			NumberOfShards: shards,
		}
	}

	// Generate the filename dynamically based on the epoch value
	filename := fmt.Sprintf("%sepoch_%d.csv", datasetDir, epoch)

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

	// Work out workloads for the first time this epoch
	graph.ShardWorkloads = calculateShardWorkloads(graph)

	// Now that preparation is ready, the actual CLPA can run and the results recorded
	result := clpaCall(alpha, beta, tau, rho, graph, randomGen, runClpaIter, scoringPenalty)

	// Add inactive vertices back to graph for the next epoch
	for id, vertex := range inactiveVertices {
		graph.Vertices[id] = vertex
	}

	result.Graph = graph

	return result

}

// The CLPA function that continues iterations for all tau iterations irrespective of convergence, as per paper
func RunClpaPaper(alpha float64, beta float64, tau int, rho int, graph *shared.Graph,
	randomGen *rand.Rand, runClpaIter ClpaIterationMode, scoringPenalty ScoringPenalty) *shared.EpochResult {

	convergenceIter := -1 // Default value if no convergence within iterations

	// Carry out CLPA iterations
	for iter := 0; iter < tau; iter++ {

		// Create a map with all old labels - meaning labels of vertices before current CLPA iteration
		oldLabels := make(map[string]int)
		for id, vertex := range graph.Vertices {
			oldLabels[id] = vertex.Label
		}

		// Perform an iteration of CLPA according to the mode (sync or async)
		runClpaIter(graph, beta, randomGen, rho, scoringPenalty)

		// If convergenceIter is not -1, then it was already found that the algorithm converged
		// CLPA iterations should still continue, as stipulated in the paper
		if convergenceIter == -1 {
			converged := true

			// Check if any vertex's label changed
			for id, vertex := range graph.Vertices {
				if oldLabels[id] != vertex.Label {
					converged = false
					break
				}
			}
			if converged {

				// Record the iteration number when convergence occurred (1-based)
				convergenceIter = iter + 1
			}
		}
	}

	// Calculate the workload imbalance, number of cross shard transactions and fitness of the partitioning
	workloadImbalance, crossShardWorkload, fitness := shared.CalculateFitness(graph, alpha)

	// Return the results of the epoch
	return &shared.EpochResult{
		Seed:               -1,
		Fitness:            fitness,
		WorkloadImbalance:  workloadImbalance,
		CrossShardWorkload: crossShardWorkload,
		ConvergenceIter:    convergenceIter,
	}

}

// The CLPA function that stops iterations upon convergence
func RunClpaConvergenceStop(alpha float64, beta float64, tau int, rho int, graph *shared.Graph,
	randomGen *rand.Rand, runClpaIter ClpaIterationMode, scoringPenalty ScoringPenalty) *shared.EpochResult {

	convergenceIter := -1 // Default value if no convergence within iterations

	// Carry out CLPA iterations
	for iter := 0; iter < tau; iter++ {

		// Create a map with all old labels - meaning labels of vertices before current CLPA iteration
		oldLabels := make(map[string]int)
		for id, vertex := range graph.Vertices {
			oldLabels[id] = vertex.Label
		}

		// Perform an iteration of CLPA according to the mode (sync or async)
		runClpaIter(graph, beta, randomGen, rho, scoringPenalty)

		// CLPA iterations should stop once convergence is reached
		converged := true

		// Check if any vertex's label changed
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
		Seed:               -1,
		Fitness:            fitness,
		WorkloadImbalance:  workloadImbalance,
		CrossShardWorkload: crossShardWorkload,
		ConvergenceIter:    convergenceIter,
	}

}

// The CLPA function that tests convergence behaviour
func RunClpaConvergenceTest(alpha float64, beta float64, tau int, rho int, graph *shared.Graph,
	randomGen *rand.Rand, runClpaIter ClpaIterationMode, scoringPenalty ScoringPenalty) *shared.EpochResult {

	// Create slice to store boolean indicating whether a vertex changed label or not in each iteration
	// By default all values are false in the beginning
	labelChanged := make([]bool, tau)

	// Create slice to store the fitness of the partioning in each iteration
	fitness := make([]float64, tau)

	// Carry out CLPA iterations
	for iter := 0; iter < tau; iter++ {

		// Create a map with all old labels - meaning labels of vertices before current CLPA iteration
		oldLabels := make(map[string]int)
		for id, vertex := range graph.Vertices {
			oldLabels[id] = vertex.Label
		}

		// Perform an iteration of CLPA according to the mode (sync or async)
		runClpaIter(graph, beta, randomGen, rho, scoringPenalty)

		// Calculate the fitness of the partitioning for the current iteration
		_, _, iterationfitness := shared.CalculateFitness(graph, alpha)
		fitness[iter] = iterationfitness

		// Check if any vertex's label changed
		for id, vertex := range graph.Vertices {
			if oldLabels[id] != vertex.Label {
				labelChanged[iter] = true
				break
			}
		}

	}

	// Create IterationsInfo struct and populate it
	iterationsInfo := &shared.IterationsInfo{
		LabelChanged: labelChanged,
		Fitness:      fitness,
	}

	// Return the epoch result with only the information about the iterations
	return &shared.EpochResult{
		IterationsInfo: iterationsInfo,
	}
}

// The function that performs an iteration through all vertices and assigns shards
func ClpaIterationAsync(graph *shared.Graph, beta float64, randomGen *rand.Rand,
	rho int, scoringPenalty ScoringPenalty) {

	// TESTING - PART OF CHECK FOR MONOTONIC QUESTION
	// Initialise an array to store fitness values for each iteration
	//var fitnessValues []float64

	// Get a random order to use for this CLPA iteration
	sortedVertices := setVerticesOrder(graph, randomGen)

	// Iterate through each vertex in some order
	for _, vertex := range sortedVertices {

		// Calculate the score of shards with respect to current vertex
		scores := scoringPenalty(graph, vertex, beta)

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

		// TESTING - PART OF CHECK FOR MONOTONIC QUESTION
		// Calculate fitness and append to the array
		//_, _, fitness := CalculateFitness(graph, 0.5) // Adjust alpha value as needed
		//fitnessValues = append(fitnessValues, fitness)
	}

	/*
		// Write fitness values to a CSV file
		err := WriteFitnessToCSV(fitnessValues, "fitness_values.csv")
		if err != nil {
			panic(err) // Handle error as needed
		}
	*/
}

// Alternative function for a CLPA iteration with sync mode of updating instead of async
func ClpaIterationSync(graph *shared.Graph, beta float64, randomGen *rand.Rand,
	rho int, scoringPenalty ScoringPenalty) {

	// Get a random order to use for this CLPA iteration
	sortedVertices := setVerticesOrder(graph, randomGen)

	// Iterate through each vertex in some order
	for _, vertex := range sortedVertices {

		// Calculate the score of shards with respect to current vertex
		scores := scoringPenalty(graph, vertex, beta)

		// Get the ID of the best shard with respect to current vertex
		bestShard := getBestShard(scores, randomGen)

		// Store the ID of the shard which the vertex should set its label to
		vertex.NewLabel = bestShard
	}

	// Only at the end of the CLPA iteration are the vertex labels updated
	for _, vertex := range sortedVertices {

		// move vertex to new best shard
		moveVertex(graph, vertex, vertex.NewLabel, rho)
	}
}
