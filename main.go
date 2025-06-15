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

	// Run this commented out function call to extract the epochs from the original dataset
	//shared.ExtractEpochs()

	// Run these commented out function calls to get statistics for the datasets
	//writeEpochStatistics(30, "shared/epochs/low_arrival_rate/", "datastats/low_arrival_rate_statistics.csv")
	//writeEpochStatistics(12, "shared/epochs/high_arrival_rate/", "datastats/high_arrival_rate_statistics.csv")

	// TESTING - cpu profiling
	/*
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil)) // Start pprof server
		}()
	*/

	// The following section runs different tests, for the amount of times passed in to the function

	// Run the Test Suite 'Half vs All Threads'
	// This tests CLPA as in paper vs parallel CLPA for half and full (no of threads) goroutines launched
	// for 50 times each test
	//threads.RunTestSuite(50)

	// Run the Test 'Update Mode of CLPA'
	// This tests async vs sync update modes of CLPA for 20 times
	//updatemode.RunTest(20)

	/* Run the Mini Test Suite 'Paper Penalty vs New Penalty (mini)'
	This test was done when the new penalty formula was first designed, and led to the development of the
	'Convergence' and 'Paper Penalty vs New Penalty' tests called below */
	//penalty.RunMiniTestSuite(5)

	// Run the Test Suite 'Convergence of CLPA with Paper Penalty and New Penalty'
	// This tests the convergence behaviour of CLPA with the two different penalties for 50 times each test
	//convergence.RunTestSuite(50)

	// Run the Test Suite 'Paper Penalty vs New Penalty'
	// This tests performance of the new penalty compared to the paper's penalty formula for 50 times each test
	//penalty.RunTestSuite(50)

	// Run the Test Suite 'CLPA vs Parallel CLPA vs My LPA'
	// This tests CLPA as in paper vs Parallel CLPA vs My LPA for 30 times each test
	//threepart.RunTestSuite(30)

}

// Generates graph statistics for each epoch and writes them to a CSV file
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
