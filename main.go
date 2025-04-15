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

	"example.com/shardinglpa/clpaparallel"
	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
	"example.com/shardinglpa/tests"
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

	// Run the Test Suite 'CLPA vs Concurrent CLPA'
	// This tests CLPA as in paper vs parallel CLPA for 50 times each test
	//concurrent.RunTestSuite()

	// Run the Test 'Update Mode of CLPA'
	// This tests async vs sync update modes of CLPA 50 times
	//updatemode.RunTest()

	// Run the Test 'Convergence of CLPA'
	// This tests the convergence of CLPA when rho is not used to stop label updates XXXXXXXXXXXXXXXXXXXXXXXXXXXX
	//convergence.RunTest()

	/*
		log.Println("*********** TEST SUITE 'Paper's Penalty vs New Penalty' STARTED *********** (3 Tests in total)")

		writerPaper, filePaper := tests.CreateResultsWriter("concurrent/paper_CLPA")
		defer writerPaper.Flush()
		defer filePaper.Close()

		writerPaperParallel, filePaperParallel := tests.CreateResultsWriter("concurrent/paper_CLPA_parallel")
		defer writerPaperParallel.Flush()
		defer filePaperParallel.Close()

		writerTimes, fileTimes := tests.CreateTimesWriter("concurrent/test_times")
		defer writerTimes.Flush()
		defer fileTimes.Close()

		// The number of epochs to be run
		numberOfEpochs := 30

		// The number of times/threshold each vertex is allowed to update its label (rho)
		rho := 50

		// The weight of cross-shard vs workload imbalance in fitness calculation
		alpha := 0.5

		// The weight of cross-shard vs workload imbalance - 0 to 1
		beta := 0.5

		// The number of iterations of CLPA
		tau := 100

		// The number of shards
		numberOfShards := 8

		// The transaction arrival rate
		arrivalRate := "low"

		// The number of times the experiment is repeated
		runs := 50

		// Set CLPA iteration call to be made with update mode set to async
		var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

		// Set CLPA to be called with 'stop on convergence' set to off, as in paper
		var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaPaper

		// Set CLPA scoring penalty to be same as the one in the paper
		var scoringPenalty paperclpa.ScoringPenalty = paperclpa.CalculateScoresPaper

		// END OF SETUP

		// NOW FOR THE TESTS:

		test := 1

		numberOfParallelRuns := int(runtime.NumCPU())
		halfNumberOfParallelRuns := int(runtime.NumCPU() / 2)

		log.Println("Test Suite 'CLPA vs Concurrent CLPA' - Test CLPA as in paper vs parallel CLPA for 50 times each test")

		numberOfShards = 8
		arrivalRate = "low"

		//TEST 1
		log.Println("Started Test " + strconv.Itoa(test) + "/8 - shards = 8, tx arrival rate = low, full parallel runs")
		runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
			tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
		test++

		log.Println("*********** TEST SUITE 'CLPA vs Concurrent CLPA' FINISHED ***********")
	*/
}

func runTest(test int, runs int, shards int, arrivalRate string, numberOfEpochs int, parallelRuns int,
	alpha float64, beta float64, tau int, rho int, runClpaIter paperclpa.ClpaIterationMode, clpaCall paperclpa.ClpaCall,
	scoringPenalty paperclpa.ScoringPenalty, writerPaper *csv.Writer, writerPaperParallel *csv.Writer, testTimesWriter *csv.Writer) {

	for run := 1; run <= runs; run++ {

		var graphSingle *shared.Graph = nil
		var graphParallel *shared.Graph = nil

		var timeSingle []float64
		var timeParallel []float64

		var paperclpaResults []*shared.EpochResult
		var paperParallelResults [][]*shared.EpochResult

		// Iterate over the epochs
		for epoch := 1; epoch <= numberOfEpochs; epoch++ {

			// CLPA as in paper
			start := time.Now()

			epochResult := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphSingle, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

			// Carry the graph forward for the next epoch
			graphSingle = epochResult.Graph

			timeSingle = append(timeSingle, time.Since(start).Seconds())

			// Append the result for the current epoch to the epochResults slice
			paperclpaResults = append(paperclpaResults, epochResult)

			// Parallel CLPA
			start = time.Now()

			seedsResults, inactiveVertices := clpaparallel.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, parallelRuns, epoch, graphParallel, alpha, beta, tau, rho)

			// Get the best graph from all of the parallel runs
			graphParallel = tests.GetBestGraph(seedsResults)

			// Add inactive vertices back to graph for the next epoch
			// Only the best graph has the vertices added back to save resources
			for id, vertex := range inactiveVertices {
				graphParallel.Vertices[id] = vertex
			}

			timeParallel = append(timeParallel, time.Since(start).Seconds())

			paperParallelResults = append(paperParallelResults, seedsResults)

		}
		tests.WriteSingleResults(paperclpaResults, writerPaper, test, run)
		tests.WriteResults(paperParallelResults, writerPaperParallel, test, run)

		tests.WriteTimes(testTimesWriter, test, run, timeSingle, timeParallel)
	}
	log.Printf("Test finished")
}

// Function to generate epochs
func extractEpochs() {

	datasets := []string{
		"shared/originaldataset/0_to_1_Block_Transactions.csv",
		"shared/originaldataset/1_to_2_Block_Transactions.csv",
	}

	maxTransactions := 8_000_000

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
