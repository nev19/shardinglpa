package allthreads

import (
	"encoding/csv"
	"log"
	"runtime"
	"strconv"
	"time"

	"example.com/shardinglpa/clpaparallel"
	"example.com/shardinglpa/mylpa"
	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
	"example.com/shardinglpa/tests"
)

func RunTestSuite(runs int) {

	totalTests := 8

	log.Printf("*********** TEST SUITE 'Half vs All Threads' STARTED (%d Tests in total) ***********", totalTests)

	writerPaper, filePaper := tests.CreateResultsWriter("threads/paper_CLPA")
	defer writerPaper.Flush()
	defer filePaper.Close()

	writerPaperParallel, filePaperParallel := tests.CreateResultsWriter("threads/paper_CLPA_parallel")
	defer writerPaperParallel.Flush()
	defer filePaperParallel.Close()

	writerTimes, fileTimes := tests.CreateTimesWriter("threads/test_times")
	defer writerTimes.Flush()
	defer fileTimes.Close()

	// The number of epochs to be run
	numberOfEpochsLow := 30
	numberOfEpochsHigh := 12

	// The number of times/threshold each vertex is allowed to update its label (rho)
	rho := 50

	// The weight of cross-shard vs workload imbalance in fitness calculation
	alpha := 0.5

	// The weight of cross-shard vs workload imbalance in score function
	beta := 0.5

	// The number of iterations of CLPA
	tau := 100

	// The number of shards
	numberOfShards := 8

	// The transaction arrival rate
	arrivalRate := "low"

	// Set CLPA iteration call to be made with update mode set to async
	var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

	// Set CLPA to be called with 'stop on convergence' set to off, as in paper
	var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaPaper

	// Set CLPA scoring penalty to be same as the one in the paper
	var scoringPenalty paperclpa.ScoringPenalty = paperclpa.CalculateScoresPaper

	// Set number of parallel runs to maximum number of cores available
	numberOfParallelRuns := int(runtime.NumCPU())

	// Set number of parallel runs to half the number of cores available
	halfNumberOfParallelRuns := int(runtime.NumCPU() / 2)

	// END OF SETUP

	// NOW FOR THE TESTS:

	test := 1

	numberOfShards = 8
	arrivalRate = "low"

	//TEST 1
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = low, full parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	//TEST 2
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = low, half parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, halfNumberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	arrivalRate = "high"
	//TEST 3
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = high, full parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	//TEST 4
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = high, half parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, halfNumberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	numberOfShards = 16
	arrivalRate = "low"
	//TEST 5
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = low, full parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	//TEST 6
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = low, half parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, halfNumberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	arrivalRate = "high"
	//TEST 7
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = high, full parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	//TEST 8
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = high, half parallel runs", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, halfNumberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, clpaCall, scoringPenalty, writerPaper, writerPaperParallel, writerTimes)
	test++

	log.Println("*********** TEST SUITE 'Half vs All Threads' FINISHED ***********")
}

func runTest(test int, runs int, shards int, arrivalRate string, numberOfEpochs int, parallelRuns int,
	alpha float64, beta float64, tau int, rho int, runClpaIter paperclpa.ClpaIterationMode, clpaCall paperclpa.ClpaCall,
	scoringPenalty paperclpa.ScoringPenalty, writerPaper *csv.Writer, writerPaperParallel *csv.Writer, writerTimes *csv.Writer) {

	// Counter to store the index of the next unused seed
	nextSeedIndex := 0

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

			// Append the time and epoch results to the slices
			timeSingle = append(timeSingle, time.Since(start).Seconds())
			paperclpaResults = append(paperclpaResults, epochResult)

			// Parallel CLPA

			// Get the random seeds
			seeds, err := mylpa.GetSeeds("mylpa/seeds.csv", parallelRuns, nextSeedIndex)
			if err != nil {
				log.Fatalf("Failed to load seeds: %v", err)
			}

			nextSeedIndex += parallelRuns

			start = time.Now()

			seedsResults, inactiveVertices := clpaparallel.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphParallel, alpha, beta, tau, rho, seeds)

			// Get the best graph from all of the parallel runs
			graphParallel = shared.GetBestGraph(seedsResults)

			// Add inactive vertices back to graph for the next epoch
			// Only the best graph has the vertices added back to save resources
			for id, vertex := range inactiveVertices {
				graphParallel.Vertices[id] = vertex
			}

			// Append the time and epoch results to the slices
			timeParallel = append(timeParallel, time.Since(start).Seconds())
			paperParallelResults = append(paperParallelResults, seedsResults)

		}
		tests.WriteSingleResults(paperclpaResults, writerPaper, test, run)
		tests.WriteResults(paperParallelResults, writerPaperParallel, test, run)

		tests.WriteTimes(writerTimes, test, run, timeSingle, timeParallel)
	}
	log.Printf("Test finished")
}
