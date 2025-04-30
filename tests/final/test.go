package final

import (
	"encoding/csv"
	"log"
	"runtime"
	"time"

	"example.com/shardinglpa/clpaparallel"
	"example.com/shardinglpa/mylpa"
	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
	"example.com/shardinglpa/tests"
)

func RunTestSuite(runsHalfCores int, runsFullCores int) {

	totalTests := 40

	log.Printf("*********** TEST SUITE 'CLPA vs Parallel CLPA vs My LPA' STARTED (%d Tests in total) ***********", totalTests)

	writerPaper, filePaper := tests.CreateResultsWriter("final/paper_CLPA")
	defer writerPaper.Flush()
	defer filePaper.Close()

	writerPaperParallel, filePaperParallel := tests.CreateResultsWriter("final/paper_CLPA_parallel")
	defer writerPaperParallel.Flush()
	defer filePaperParallel.Close()

	writerFinal, fileFinal := tests.CreateResultsWriter("final/my_LPA")
	defer writerFinal.Flush()
	defer fileFinal.Close()

	writerTimes, fileTimes := tests.CreateTimesWriter("final/test_times")
	defer writerTimes.Flush()
	defer fileTimes.Close()

	// The number of epochs to be run
	numberOfEpochsLow := 30
	numberOfEpochsHigh := 12

	// The number of times/threshold each vertex is allowed to update its label (rho)
	rho := 50

	// The weight of cross-shard vs workload imbalance in fitness calculation
	alpha := 0.5

	// The number of iterations of CLPA
	tau := 100

	// Set CLPA iteration call to be made with update mode set to async
	var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

	// END OF SETUP

	// NOW FOR THE TESTS:

	/* 60 tests are run in total
	The following settings are varied:
		beta (0.1, 0.3, 0.5, 0.7, 0.9) - The weight of cross-shard vs workload imbalance in score function
		transaction arrival rate - low or high
		the number of cores running the program simultaneously
		the number of shards
	*/

	test := 1
	betas := []float64{0.1, 0.3, 0.5, 0.7, 0.9}

	// Set number of parallel runs to half the number of cores available
	numberOfParallelRuns := int(runtime.NumCPU() / 2)
	halfCores := true
	runs := runsHalfCores

	// 8 shards
	test = runBatchTests(test, totalTests, runs, 8, "low", numberOfEpochsLow, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
	test = runBatchTests(test, totalTests, runs, 8, "high", numberOfEpochsHigh, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)

	// 16 shards
	test = runBatchTests(test, totalTests, runs, 16, "low", numberOfEpochsLow, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
	test = runBatchTests(test, totalTests, runs, 16, "high", numberOfEpochsHigh, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)

	// 24 shards
	test = runBatchTests(test, totalTests, runs, 24, "low", numberOfEpochsLow, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
	test = runBatchTests(test, totalTests, runs, 24, "high", numberOfEpochsHigh, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)

	// Set number of parallel runs to maximum number of cores available
	numberOfParallelRuns = int(runtime.NumCPU())
	halfCores = false
	runs = runsFullCores

	/*
		// 8 shards
		test = runBatchTests(test, totalTests, runs, 8, "low", numberOfEpochsLow, betas, numberOfParallelRuns,
			halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
		test = runBatchTests(test, totalTests, runs, 8, "high", numberOfEpochsHigh, betas, numberOfParallelRuns,
			halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
	*/

	// 16 shards
	test = runBatchTests(test, totalTests, runs, 16, "low", numberOfEpochsLow, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
	test = runBatchTests(test, totalTests, runs, 16, "high", numberOfEpochsHigh, betas, numberOfParallelRuns,
		halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)

	/*
		// 24 shards
		test = runBatchTests(test, totalTests, runs, 24, "low", numberOfEpochsLow, betas, numberOfParallelRuns,
			halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
		test = runBatchTests(test, totalTests, runs, 24, "high", numberOfEpochsHigh, betas, numberOfParallelRuns,
			halfCores, alpha, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)
	*/

	log.Println("*********** TEST SUITE 'CLPA vs Parallel CLPA vs My LPA' FINISHED ***********")
}

// runBatchTests executes a batch of tests with specific configurations
// It loops over the provided beta values, logging and invoking runTest for each.
// The function returns the updated test counter after all tests are completed.
func runBatchTests(startTest int, totalTests int, runs int, shards int, arrival string, epochs int, betas []float64,
	numberOfParallelRuns int, halfCores bool, alpha float64, tau int, rho int, runClpaIter paperclpa.ClpaIterationMode,
	writerPaper, writerPaperParallel, writerFinal, writerTimes *csv.Writer) int {

	test := startTest

	// Iterate through all beta values and run a test for each
	for _, beta := range betas {

		// Log the start of the test with its parameters
		log.Printf("Started Test %d/%d - shards = %d, tx arrival rate = %s, beta = %.1f", test, totalTests, shards, arrival, beta)

		// Run the actual test with the current configuration
		runTest(test, runs, shards, arrival, epochs, numberOfParallelRuns, halfCores,
			alpha, beta, tau, rho, runClpaIter, writerPaper, writerPaperParallel, writerFinal, writerTimes)

		// Increment test counter for the next test
		test++
	}

	// Return the updated test counter so the caller can continue numbering correctly
	return test
}

func runTest(test int, runs int, shards int, arrivalRate string, numberOfEpochs int, parallelRuns int,
	halfCores bool, alpha float64, beta float64, tau int, rho int, runClpaIter paperclpa.ClpaIterationMode,
	writerPaper *csv.Writer, writerPaperParallel *csv.Writer, writerFinal *csv.Writer, writerTimes *csv.Writer) {

	// Counter to store the index of the next unused seed
	nextSeedIndex := 0

	for run := 1; run <= runs; run++ {

		var graphPaper *shared.Graph = nil
		var graphParallel *shared.Graph = nil
		var graphFinal *shared.Graph = nil

		var timePaper []float64
		var timeParallel []float64
		var timeFinal []float64

		var paperClpaResults []*shared.EpochResult
		var paperParallelResults [][]*shared.EpochResult
		var myLpaResults [][]*shared.EpochResult

		// Iterate over the epochs
		for epoch := 1; epoch <= numberOfEpochs; epoch++ {

			// CLPA as in paper

			// Set CLPA to be called with 'stop on convergence' set to off, as in paper
			var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaPaper

			// Set CLPA scoring penalty to be same as the one in the paper
			var scoringPenalty paperclpa.ScoringPenalty = paperclpa.CalculateScoresPaper

			start := time.Now()

			epochResult := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphPaper, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

			// Check if result is nil, which can happen when epoch file is not found. If so, continue to next iteration
			if epochResult == nil {
				continue
			}

			// Append the time and epoch results to the slices
			timePaper = append(timePaper, time.Since(start).Seconds())
			paperClpaResults = append(paperClpaResults, epochResult)

			// Parallel CLPA and My LPA

			// Get the random seeds
			seeds, err := mylpa.GetSeeds("mylpa/seeds.csv", parallelRuns, nextSeedIndex)
			if err != nil {
				log.Fatalf("Failed to load seeds: %v", err)
			}

			// Increment the index where to find the next unused seed by how many seeds were used
			// If halfCores is true, then double the increment to be made, to stay comparable with full cores tests
			if halfCores {
				nextSeedIndex = nextSeedIndex + (parallelRuns * 2)
			} else {
				nextSeedIndex += parallelRuns
			}

			// Parallel CLPA

			start = time.Now()

			seedsResultsParallel, inactiveVerticesParallel := clpaparallel.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphParallel, alpha, beta, tau, rho, seeds)

			// Get the best graph from all of the parallel runs
			graphParallel = tests.GetBestGraph(seedsResultsParallel)

			// Add inactive vertices back to graph for the next epoch
			// Only the best graph has the vertices added back to save resources
			for id, vertex := range inactiveVerticesParallel {
				graphParallel.Vertices[id] = vertex
			}

			// Append the time and epoch results to the slices
			timeParallel = append(timeParallel, time.Since(start).Seconds())
			paperParallelResults = append(paperParallelResults, seedsResultsParallel)

			// My LPA

			// Record time of start
			start = time.Now()

			// The number of seeds passed in to the sharding function determines the number of parallel runs
			seedsResultsFinal, inactiveVerticesFinal := mylpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphFinal, alpha, beta, tau, rho, seeds)

			// Get the best graph from all of the parallel runs
			graphFinal = tests.GetBestGraph(seedsResultsFinal)

			// Add inactive vertices back to graph for the next epoch
			// Only the best graph has the vertices added back to save resources
			for id, vertex := range inactiveVerticesFinal {
				graphFinal.Vertices[id] = vertex
			}

			// Append the time and epoch results to the slices
			timeFinal = append(timeFinal, time.Since(start).Seconds())
			myLpaResults = append(myLpaResults, seedsResultsFinal)

		}
		tests.WriteSingleResults(paperClpaResults, writerPaper, test, run)
		tests.WriteResults(paperParallelResults, writerPaperParallel, test, run)
		tests.WriteResults(myLpaResults, writerFinal, test, run)

		tests.WriteThreeTimes(writerTimes, test, run, timePaper, timeParallel, timeFinal)
	}
	log.Printf("Test finished")
}
