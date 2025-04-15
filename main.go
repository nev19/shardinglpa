package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
)

func main() {

	//Set up logging
	logFile, err := os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	multi := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multi)

	// Run this commented out function to extract the epochs from the original dataset
	//extractEpochs()

	// Run this commented out function to get statistics for the datasets
	//writeEpochStatistics(30, "shared/epochs/low_arrival_rate/", "datastats/low_arrival_rate_statistics.csv")
	//writeEpochStatistics(12, "shared/epochs/high_arrival_rate/", "datastats/high_arrival_rate_statistics.csv")

	// TESTING - cpu profiling
	/*
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil)) // Start pprof server
		}()
	*/

	// The following section runs different tests, for the amount of times passed in to the function

	// Run the Test Suite 'CLPA vs Concurrent CLPA'
	// This tests CLPA as in paper vs parallel CLPA for 50 times each test
	//concurrent.RunTestSuite(50)

	// Run the Test 'Update Mode of CLPA'
	// This tests async vs sync update modes of CLPA for 20 times
	//updatemode.RunTest(20)

	// Run the Test 'Convergence of CLPA'
	// This tests the convergence of CLPA when rho is not used to stop label updates for XXXXXXXX times
	//convergence.RunTest(1 XXXX)

	// Run the Test Suite 'Paper Penalty vs New Penalty'
	// This tests performance of the new penalty compared to the paper's penalty formula for 50 times each test
	// penalty.RunTestSuite(50)

	// FINAL TEST
	// CHECK OUT GetSeeds func to see that is working well

	// The number of seeds passed in to the sharding function determines the number of parallel runs
	/*
		numberOfParallelRuns := 32
		nextSeedIndex := 0
			// Get the random seeds
			seeds, err := mylpa.GetSeeds("mylpa/seeds.csv", numberOfParallelRuns)
			if err != nil {
				log.Fatalf("Failed to load seeds: %v", err)
			}

			nextSeedIndex += numberOfParallelRuns

			mylpa.ShardAllocation()
	*/

}

// Function to generate epochs
func extractEpochs() {

	// Set paths to datasets
	datasets := []string{
		"shared/originaldataset/0_to_1_Block_Transactions.csv",
		"shared/originaldataset/1_to_2_Block_Transactions.csv",
	}

	// Set the maximum number of transactions needed
	maxTransactions := 3_000_000

	// Call functions to split dataset into epochs according to the transaction arrival rate
	shared.SplitMultipleDatasets(datasets, "shared/epochs/low_arrival_rate/", 100_000, maxTransactions)
	shared.SplitMultipleDatasets(datasets, "shared/epochs/high_arrival_rate/", 250_000, maxTransactions)
}

func writeEpochStatistics(numberOfEpochs int, datasetDir string, outputFilePath string) {

	// Create the output CSV file
	outFile, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output CSV file: %v\n", err)
		return
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// The random generator and numberOfShards below are needed by InitialiseGraphFromRows function,
	// but are irrelevant for the purpose of this function
	randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Initialise the graph
	graph := &shared.Graph{
		Vertices:       make(map[string]*shared.Vertex),
		NumberOfShards: 8,
	}

	// Write CSV header
	writer.Write([]string{
		"Epoch", "Vertices", "ActiveVertices", "InactiveVertices", "Edges", "TotalWeight", "SelfLoops",
	})

	for epoch := 1; epoch <= numberOfEpochs; epoch++ {
		filename := fmt.Sprintf("%sepoch_%d.csv", datasetDir, epoch)

		rows, err := shared.ReadCSV(filename)
		if err != nil {
			fmt.Printf("Error reading CSV for epoch %d: %v\n", epoch, err)
			continue
		}

		graph = paperclpa.InitialiseGraphFromRows(rows, graph, randomGen)

		numVertices := len(graph.Vertices)
		numEdges := 0
		totalEdgeWeight := 0
		inactiveVertices := 0

		selfLoops := 0

		for _, vertex := range graph.Vertices {
			if len(vertex.Edges) == 0 {
				inactiveVertices++
				continue
			}

			for neighborID, weight := range vertex.Edges {
				if vertex.ID == neighborID {
					selfLoops += weight
				} else {
					numEdges++
					totalEdgeWeight += weight
				}
			}
		}

		// Each regular edge was counted twice (once from each endpoint), so divide by 2
		numEdges /= 2
		totalEdgeWeight = (totalEdgeWeight / 2) + selfLoops // Add self-loop weight once

		// Calculate number of active vertices
		activeVertices := numVertices - inactiveVertices

		// Write row to CSV
		writer.Write([]string{
			strconv.Itoa(epoch),
			strconv.Itoa(numVertices),
			strconv.Itoa(activeVertices),
			strconv.Itoa(inactiveVertices),
			strconv.Itoa(numEdges + selfLoops),
			strconv.Itoa(totalEdgeWeight),
			strconv.Itoa(selfLoops),
		})

	}

	// Log success message
	log.Println("Dataset Statistics written to CSV successfully")

}

/*
func memStat() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fmt.Println("-------- GC / Memory Stats --------")
	fmt.Printf("Alloc = %v MiB\n", mem.Alloc/1024/1024)
	fmt.Printf("TotalAlloc = %v MiB\n", mem.TotalAlloc/1024/1024)
	fmt.Printf("Sys = %v MiB\n", mem.Sys/1024/1024)
	fmt.Printf("NumGC = %v\n", mem.NumGC)
	fmt.Printf("Last GC Pause = %v ms\n", mem.PauseNs[(mem.NumGC+255)%256]/1e6)
}
*/
