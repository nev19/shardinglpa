package final

import (
	"encoding/csv"
	"log"
	"runtime"
	"strconv"
	"time"

	"example.com/shardinglpa/mylpa"
	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
	"example.com/shardinglpa/tests"
)

func RunTestSuite(runs int) {

	totalTests := 30

	log.Printf("*********** TEST SUITE 'CLPA vs My LPA' STARTED (%d Tests in total) ***********", totalTests)

	writerPaper, filePaper := tests.CreateResultsWriter("final/paper_CLPA")
	defer writerPaper.Flush()
	defer filePaper.Close()

	writerFinal, filePaperParallel := tests.CreateResultsWriter("final/my_LPA")
	defer writerFinal.Flush()
	defer filePaperParallel.Close()

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

	// Set number of parallel runs to maximum number of cores available
	numberOfParallelRuns := int(runtime.NumCPU() / 2)

	// END OF SETUP

	// NOW FOR THE TESTS:

	test := 1

	numberOfShards = 8
	arrivalRate = "low"

	beta = 0.1
	//TEST 1
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.3
	//TEST 2
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.5
	//TEST 3
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.7
	//TEST 4
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.9
	//TEST 5
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	arrivalRate = "high"

	beta = 0.1
	//TEST 6
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.3
	//TEST 7
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.5
	//TEST 8
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.7
	//TEST 9
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.9
	//TEST 10
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 8, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	numberOfShards = 16
	arrivalRate = "low"

	beta = 0.1
	//TEST 11
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.3
	//TEST 12
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.5
	//TEST 13
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.7
	//TEST 14
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.9
	//TEST 15
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	arrivalRate = "high"

	beta = 0.1
	//TEST 16
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.3
	//TEST 17
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.5
	//TEST 18
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.7
	//TEST 19
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.9
	//TEST 20
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 16, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	numberOfShards = 24
	arrivalRate = "low"

	beta = 0.1
	//TEST 21
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.3
	//TEST 22
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.5
	//TEST 23
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.7
	//TEST 24
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.9
	//TEST 25
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = low, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsLow, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	arrivalRate = "high"

	beta = 0.1
	//TEST 26
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.3
	//TEST 27
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.5
	//TEST 28
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.7
	//TEST 29
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	beta = 0.9
	//TEST 30
	log.Printf("Started Test "+strconv.Itoa(test)+"/%d - shards = 24, tx arrival rate = high, beta = ", totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochsHigh, numberOfParallelRuns, alpha, beta,
		tau, rho, runClpaIter, writerPaper, writerFinal, writerTimes)
	test++

	log.Println("*********** TEST SUITE 'CLPA vs My LPA' FINISHED ***********")
}

func runTest(test int, runs int, shards int, arrivalRate string, numberOfEpochs int, parallelRuns int,
	alpha float64, beta float64, tau int, rho int, runClpaIter paperclpa.ClpaIterationMode,
	writerPaper *csv.Writer, writerFinal *csv.Writer, writerTimes *csv.Writer) {

	// Counter to store the index of the next unused seed
	nextSeedIndex := 0

	for run := 1; run <= runs; run++ {

		var graphPaper *shared.Graph = nil
		var graphFinal *shared.Graph = nil

		var timePaper []float64
		var timeFinal []float64

		var paperClpaResults []*shared.EpochResult
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

			// Carry the graph forward for the next epoch
			graphPaper = epochResult.Graph

			// Append the time and epoch results to the slices
			timePaper = append(timePaper, time.Since(start).Seconds())
			paperClpaResults = append(paperClpaResults, epochResult)

			// My LPA

			// Set new penalty formula to be called with 'stop on convergence' set to on
			clpaCall = paperclpa.RunClpaConvergenceStop

			// Set scoring penalty to be the newly proposed penalty formula
			scoringPenalty = paperclpa.CalculateScoresNew

			// Get the random seeds
			seeds, err := mylpa.GetSeeds("mylpa/seeds.csv", parallelRuns, nextSeedIndex)
			if err != nil {
				log.Fatalf("Failed to load seeds: %v", err)
			}

			// Record time of start
			start = time.Now()

			// The number of seeds passed in to the sharding function determines the number of parallel runs
			seedsResults, inactiveVertices := mylpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphFinal, alpha, beta, tau, rho, seeds)

			// Increment the index where to find the next unused seed by how many seeds were used
			nextSeedIndex += parallelRuns

			// Get the best graph from all of the parallel runs
			graphFinal = tests.GetBestGraph(seedsResults)

			// Add inactive vertices back to graph for the next epoch
			// Only the best graph has the vertices added back to save resources
			for id, vertex := range inactiveVertices {
				graphFinal.Vertices[id] = vertex
			}

			// Append the time and epoch results to the slices
			timeFinal = append(timeFinal, time.Since(start).Seconds())
			myLpaResults = append(myLpaResults, seedsResults)

		}
		tests.WriteSingleResults(paperClpaResults, writerPaper, test, run)
		tests.WriteResults(myLpaResults, writerFinal, test, run)

		tests.WriteTimes(writerTimes, test, run, timePaper, timeFinal)
	}
	log.Printf("Test finished")
}
