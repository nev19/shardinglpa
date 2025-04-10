package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	"example.com/shardinglpa/clpaparallel"
	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
)

func main() {

	// Run this commented out function to extract the epochs from the original dataset
	//extractEpochs()

	// Run this commented out function to get statistics for the datasets
	//writeEpochStatistics(30, "shared/epochs/low_arrival_rate/", "results/dataset_statistics/low_arrival_rate_statistics.csv")
	//writeEpochStatistics(12, "shared/epochs/high_arrival_rate/", "results/dataset_statistics/high_arrival_rate_statistics.csv")

	// TESTING - cpu profiling
	/*
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil)) // Start pprof server
		}()
	*/

	//Set up logging
	f, err := os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	multi := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multi)

	log.Println("PROGRAM STARTED ***********")

	// Set up files
	writerMylpa, fileMylpa := createCSVWriter("my_LPA")
	defer writerMylpa.Flush()
	defer fileMylpa.Close()

	writerPaper, filePaper := createCSVWriter("paper_CLPA")
	defer writerPaper.Flush()
	defer filePaper.Close()

	writerPaperParallel, filePaperParallel := createCSVWriter("paper_CLPA_parallel")
	defer writerPaperParallel.Flush()
	defer filePaperParallel.Close()

	// Set up time file
	testTimesFile, err := os.Create("results/times_test_A.csv")
	if err != nil {
		log.Fatalf("Could not create times_test_A.csv: %v", err)
	}
	defer testTimesFile.Close()

	testTimesWriter := csv.NewWriter(testTimesFile)
	defer testTimesWriter.Flush()

	// Write header
	if err := testTimesWriter.Write([]string{"test", "run", "epoch", "timeBaseline", "timeNew"}); err != nil {
		log.Fatalf("Error writing times_test_A header: %v", err)
	}

	log.Println("Maximum number of cores: ", runtime.NumCPU())

	// The number of epochs
	numberOfEpochs := 30

	// The number of times/threshold each vertex is allowed to update its label (rho)
	rho := 50

	alpha := 0.5

	// The weight of cross-shard vs workload imbalance - 0 to 1 (beta)
	beta := 0.2

	tau := 100

	// The number of shards
	numberOfShards := 8

	// The transaction arrival rate
	arrivalRate := "low"

	experimentRuns := 3

	// END OF SETUP

	// NOW FOR THE TESTS:

	test := 1

	log.Println("Start Test Suite A - Test CLPA as in paper vs parallel CLPA for 100 times")

	numberOfParallelRuns := int(runtime.NumCPU())

	// TEST 1
	arrivalRate = "low"
	beta = 0.2

	log.Println("Start Test " + strconv.Itoa(test) + "/ - beta = 0.2, shards = 8, tx arrival rate = low")

	for run := 1; run <= experimentRuns; run++ {

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
				numberOfShards, epoch, graphSingle, rho, alpha, beta, tau, "async")

			graphSingle = epochResult.Graph

			timeSingle = append(timeSingle, time.Since(start).Seconds())

			// Append the result for the current epoch to the epochResults slice
			paperclpaResults = append(paperclpaResults, epochResult)

			// Parallel CLPA
			start = time.Now()

			seedsResults := clpaparallel.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				numberOfShards, numberOfParallelRuns, epoch, graphParallel, rho, alpha, beta, tau, "async")

			// Get the best graph from all of the parallel runs
			graphParallel = getBestGraph(seedsResults)

			timeParallel = append(timeParallel, time.Since(start).Seconds())

			paperParallelResults = append(paperParallelResults, seedsResults)

		}
		writeSingleResults(paperclpaResults, writerPaper, beta, numberOfShards, arrivalRate, run)
		writeResults(paperParallelResults, writerPaperParallel, beta, numberOfShards, arrivalRate, run)

		writeTimes(testTimesWriter, test, run, timeSingle, timeParallel)
	}
	log.Printf("Test finished")
	test++

	/*

		//TEST 2
		beta = 0.5

		log.Println("Start Test " + strconv.Itoa(test) + "/ - beta = 0.5, shards = 8, tx arrival rate = low")

		for run := 1; run <= 100; run++ {

			start := time.Now()

			paperclpaResults := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/", numberOfShards,
				numberOfEpochs, rho, 0.5, beta, 100, "async")

			processSingleResults(paperclpaResults, writerPaper, beta, numberOfShards, arrivalRate, run)

			recordTime(testTimesWriter, test, run, time.Since(start).Seconds())
		}
		log.Printf("Test finished")
		test++

		//TEST 3
		beta = 0.8

		log.Println("Start Test " + strconv.Itoa(test) + "/ - beta = 0.8, shards = 8, tx arrival rate = low")

		for run := 1; run <= 100; run++ {

			start := time.Now()

			paperclpaResults := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/", numberOfShards,
				numberOfEpochs, rho, 0.5, beta, 100, "async")

			processSingleResults(paperclpaResults, writerPaper, beta, numberOfShards, arrivalRate, run)

			recordTime(testTimesWriter, test, run, time.Since(start).Seconds())
		}
		log.Printf("Test finished")
		test++

		// TEST 4
		arrivalRate = "high"
		beta = 0.2

		log.Println("Start Test " + strconv.Itoa(test) + "/ - beta = 0.2, shards = 8, tx arrival rate = high")

		for run := 1; run <= 100; run++ {

			start := time.Now()

			paperclpaResults := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/", numberOfShards,
				numberOfEpochs, rho, 0.5, beta, 100, "async")

			processSingleResults(paperclpaResults, writerPaper, beta, numberOfShards, arrivalRate, run)

			recordTime(testTimesWriter, test, run, time.Since(start).Seconds())
		}
		log.Printf("Test finished")
		test++

		//TEST 5
		beta = 0.5

		log.Println("Start Test " + strconv.Itoa(test) + "/ - beta = 0.5, shards = 8, tx arrival rate = high")

		for run := 1; run <= 100; run++ {

			start := time.Now()

			paperclpaResults := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/", numberOfShards,
				numberOfEpochs, rho, 0.5, beta, 100, "async")

			processSingleResults(paperclpaResults, writerPaper, beta, numberOfShards, arrivalRate, run)

			recordTime(testTimesWriter, test, run, time.Since(start).Seconds())
		}
		log.Printf("Test finished")
		test++

		//TEST 6
		beta = 0.8

		log.Println("Start Test " + strconv.Itoa(test) + "/ - beta = 0.8, shards = 8, tx arrival rate = high")

		for run := 1; run <= 100; run++ {

			start := time.Now()

			paperclpaResults := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/", numberOfShards,
				numberOfEpochs, rho, 0.5, beta, 100, "async")

			processSingleResults(paperclpaResults, writerPaper, beta, numberOfShards, arrivalRate, run)

			recordTime(testTimesWriter, test, run, time.Since(start).Seconds())
		}
		log.Printf("Test finished")
		test++
	*/

	/*

		log.Println("Start Test 2/ - Test My LPA as in paper for different seeds in parallel")
		start = time.Now()

		clpaParallelResults := clpaparallel.ShardAllocation("shared/epochs/low_arrival_rate/", numberOfShards,
			8, numberOfEpochs, rho, 0.5, beta, 100, "async")

		log.Printf("Test ran for %s\n", time.Since(start))

		processResults(clpaParallelResults, writerPaperParallel, beta, numberOfShards)

		// serial time comparison XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX


			start := time.Now()

			for i := 0; i < 100; i++ {
				// How to call function: mylpa.ShardAllocation
				// (datasetDir string, numberOfShards int, numberOfParallelRuns int, numberOfEpochs int, rho int,
				// alpha float64, beta float64, tau int)

				//rho set to 500 so it does not affect
				myResults := mylpa.ShardAllocation("shared/epochs/low_arrival_rate/", numberOfShards,
					100, numberOfEpochs, 500, 0.5, beta, 100)

				processResults(myResults, writerMylpa, beta, numberOfShards)
			}

			fmt.Printf("\nMy LPA parallel ran for %s\n", time.Since(start))
	*/

	//BEGIN old TESTS XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
	/*
		start := time.Now()

		runtime.GOMAXPROCS(1)
		// How to call function: paperclpa.ShardAllocation
		// (datasetDir string, numberOfShards int, numberOfEpochs int, rho int,
		// alpha float64, beta float64, tau int, mode string)

		paperclpaResults := paperclpa.ShardAllocation("shared/epochs/low_arrival_rate/", numberOfShards,
			numberOfEpochs, rho, 0.5, beta, 100, "async")

		fmt.Printf("\nCLPA single ran for %s\n", time.Since(start))

		processSingleResults(paperclpaResults, writerPaper, beta, numberOfShards)

		start = time.Now()

		runtime.GOMAXPROCS(4)
		// How to call function: clpaparallel.ShardAllocation
		// (datasetDir string, numberOfShards int, numberOfParallelRuns int, numberOfEpochs int, rho int,
		// alpha float64, beta float64, tau int, mode string)
		clpaParallelResults := clpaparallel.ShardAllocation("shared/epochs/low_arrival_rate/", numberOfShards,
			4, numberOfEpochs, rho, 0.5, beta, 100, "async")

		fmt.Printf("\nCLPA parallel ran for %s\n", time.Since(start))

		processResults(clpaParallelResults, writerPaperParallel, beta, numberOfShards)
	*/
}

// createCSVWriter creates a CSV file, writes the header, and returns the CSV writer.
func createCSVWriter(filename string) (*csv.Writer, *os.File) {

	// CSV header
	header := []string{"Run", "Seed", "Epoch", "Fitness", "WorkloadImbalance", "CrossShardWorkload", "ConvergenceIterations", "TimeRan", "beta", "transactionArrivalRate", "numberOfShards"}

	filePath := fmt.Sprintf("results/%s.csv", filename)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create CSV file '%s': %v\n", filename, err)
	}

	writer := csv.NewWriter(file)

	// Write the CSV header
	if err := writer.Write(header); err != nil {
		log.Fatalf("Error writing header to '%s': %v\n", filename, err)
	}

	return writer, file
}

func getBestGraph(seedResults []*shared.EpochResult) *shared.Graph {

	// Safety check: return an empty graph if there are no results to compare
	if len(seedResults) == 0 {
		fmt.Println("No results to process.")
		return &shared.Graph{}

	}

	//var bestSeed int64
	var bestFitness float64 = math.MaxFloat64
	var bestGraph *shared.Graph

	for _, result := range seedResults {
		if result.Fitness < bestFitness {
			bestFitness = result.Fitness
			//bestSeed = result.Seed
			bestGraph = result.Graph
		}
	}

	// TESTING
	//fmt.Printf("Best seed: %d with fitness: %.3f\n", bestSeed, bestFitness)
	return bestGraph
}

func writeSingleResults(results []*shared.EpochResult, writer *csv.Writer, beta float64,
	numberOfShards int, arrivalRate string, run int) {

	// Create a 2D slice where each result is its own inner slice
	groupedResults := make([][]*shared.EpochResult, len(results))
	for i, result := range results {
		groupedResults[i] = []*shared.EpochResult{result}
	}

	// Call the main writer
	writeResults(groupedResults, writer, beta, numberOfShards, arrivalRate, run)
}

func writeResults(groupedResults [][]*shared.EpochResult, writer *csv.Writer, beta float64,
	numberOfShards int, arrivalRate string, run int) {

	for epochIndex, seedsResults := range groupedResults {
		for _, result := range seedsResults {
			record := []string{
				strconv.Itoa(run),
				// Force csv to keep precision, not round
				"\t" + strconv.FormatInt(result.Seed, 10),
				strconv.Itoa(epochIndex + 1),
				fmt.Sprintf("%.3f", result.Fitness),
				fmt.Sprintf("%.3f", result.WorkloadImbalance),
				strconv.Itoa(result.CrossShardWorkload),
				strconv.Itoa(result.ConvergenceIter),
				fmt.Sprintf("%.3f", result.Duration.Seconds()),
				fmt.Sprintf("%.1f", beta),
				arrivalRate,
				strconv.Itoa(numberOfShards),
			}

			if err := writer.Write(record); err != nil {
				log.Printf("Error writing record to CSV: %v", err)
			}
		}
	}
	writer.Flush()

	if err := writer.Error(); err != nil {
		log.Printf("Error flushing CSV writer: %v", err)
	}

	//fmt.Println("Results saved successfully from run", run)

	/*

		// Safety check in case there are no results
		if len(groupedResults) == 0 {
			return
		}

		numEpochs := len(groupedResults[0].Results)

		for epochIdx := 0; epochIdx < numEpochs; epochIdx++ {
			var epochResults []shared.Result

			// Collect results for this epoch from all seeds
			for _, seedResult := range groupedResults {

				// Safety check to not go over bounds
				if epochIdx < len(seedResult.Results) {
					epochResults = append(epochResults, seedResult.Results[epochIdx])
				}
			}

			// Safety check: skip epoch if no results are available for this index
			if len(epochResults) == 0 {
				continue
			}

			// Initialise min, max, total
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


		}*/
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

func writeTimes(writer *csv.Writer, test int, run int, timesBaseline []float64, timesNew []float64) {
	if len(timesBaseline) != len(timesNew) {
		log.Printf("Mismatched slice lengths: baseline=%d, new=%d", len(timesBaseline), len(timesNew))
		return
	}

	for i := 0; i < len(timesBaseline); i++ {
		record := []string{
			strconv.Itoa(test),
			strconv.Itoa(run),
			strconv.Itoa(i + 1), // epoch index (1-based)
			fmt.Sprintf("%.6f", timesBaseline[i]),
			fmt.Sprintf("%.6f", timesNew[i]),
		}

		if err := writer.Write(record); err != nil {
			log.Printf("Error writing row to Time CSV: %v", err)
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		log.Printf("Error flushing Time CSV writer: %v", err)
	}
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
