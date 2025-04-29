package penalty

import (
	"encoding/csv"
	"log"
	"time"

	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
	"example.com/shardinglpa/tests"
)

func RunTestSuite(runs int) {

	totalTests := 15

	log.Printf("*********** TEST SUITE 'Paper Penalty vs New Penalty' STARTED (%d Tests in total) ***********", totalTests)

	writerPaperPen, filePaperPen := tests.CreateResultsWriter("penalty/paper_penalty")
	defer writerPaperPen.Flush()
	defer filePaperPen.Close()

	writerNewPen, fileNewPen := tests.CreateResultsWriter("penalty/new_penalty")
	defer writerNewPen.Flush()
	defer fileNewPen.Close()

	writerTimes, fileTimes := tests.CreateTimesWriter("penalty/test_times")
	defer writerTimes.Flush()
	defer fileTimes.Close()

	// The number of epochs to be run
	numberOfEpochs := 30

	// The number of times/threshold each vertex is allowed to update its label (rho)
	rho := 50

	// The weight of cross-shard vs workload imbalance in fitness calculation
	alpha := 0.5

	// The weight of cross-shard vs workload imbalance in score function
	beta := 0.1

	// The number of iterations of CLPA
	tau := 100

	// The number of shards
	numberOfShards := 8

	// The transaction arrival rate
	arrivalRate := "low"

	// Set CLPA iteration call to be made with update mode set to async, as in paper
	var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

	// Set CLPA using new penalty formula to be called with 'stop on convergence' set to on
	var newPenaltyClpaCall paperclpa.ClpaCall = paperclpa.RunClpaConvergenceStop

	// END OF SETUP

	// NOW FOR THE TESTS:

	test := 1

	numberOfShards = 8
	beta = 0.1

	//TEST 1
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.3

	//TEST 2
	log.Printf("Started Test %d/%d- beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.5

	//TEST 3
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.7

	//TEST 4
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.9

	//TEST 5
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	numberOfShards = 16
	beta = 0.1

	//TEST 6
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 16", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.3

	//TEST 7
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 16", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.5

	//TEST 8
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 16", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.7

	//TEST 9
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 16", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.9

	//TEST 10
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 16", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	numberOfShards = 24
	beta = 0.1

	//TEST 11
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.3

	//TEST 12
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.5

	//TEST 13
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.7

	//TEST 14
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	beta = 0.9

	//TEST 15
	log.Printf("Started Test %d/%d - beta = %.1f, shards = 8", test, totalTests, beta)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, newPenaltyClpaCall)
	test++

	log.Println("*********** TEST SUITE 'Paper Penalty vs New Penalty' FINISHED ***********")
}

func RunMiniTestSuite(runs int) {

	totalTests := 2

	log.Printf("*********** MINI TEST SUITE 'Paper Penalty vs New Penalty (mini)' STARTED (%d Tests in total) ***********", totalTests)

	writerPaperPen, filePaperPen := tests.CreateResultsWriter("penalty/mini/paper_penalty")
	defer writerPaperPen.Flush()
	defer filePaperPen.Close()

	writerNewPen, fileNewPen := tests.CreateResultsWriter("penalty/mini/new_penalty")
	defer writerNewPen.Flush()
	defer fileNewPen.Close()

	writerTimes, fileTimes := tests.CreateTimesWriter("penalty/mini/test_times")
	defer writerTimes.Flush()
	defer fileTimes.Close()

	// The number of epochs to be run
	numberOfEpochs := 30

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

	// Set CLPA iteration call to be made with update mode set to async, as in paper
	var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

	// Set CLPA for the new penalty to be called with 'stop on convergence' set to off, as in paper
	var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaPaper

	// END OF SETUP

	// NOW FOR THE TESTS:

	test := 1

	numberOfShards = 8

	//TEST 1
	log.Printf("Started Test %d/%d - shards = 8", test, totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, clpaCall)
	test++

	numberOfShards = 16

	//TEST 2
	log.Printf("Started Test %d/%d - shards = 16", test, totalTests)
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes, clpaCall)
	test++

	log.Println("*********** MINI TEST SUITE 'Paper Penalty vs New Penalty (mini)' FINISHED ***********")
}

func runTest(test int, runs int, shards int, arrivalRate string, numberOfEpochs int, alpha float64,
	beta float64, tau int, rho int, runClpaIter paperclpa.ClpaIterationMode, writerPaperPen *csv.Writer,
	writerNewPen *csv.Writer, writerTimes *csv.Writer, newPenaltyClpaCall paperclpa.ClpaCall) {

	for run := 1; run <= runs; run++ {

		var graphPaperPen *shared.Graph = nil
		var graphNewPen *shared.Graph = nil

		var timePaperPen []float64
		var timePaperNew []float64

		var paperPenResults []*shared.EpochResult
		var newPenResults []*shared.EpochResult

		// Iterate over the epochs
		for epoch := 1; epoch <= numberOfEpochs; epoch++ {

			// Penalty as in paper

			// Set CLPA scoring penalty to be same as the one in the paper
			var scoringPenalty paperclpa.ScoringPenalty = paperclpa.CalculateScoresPaper

			// Set CLPA to be called with 'stop on convergence' set to off, as in paper
			var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaPaper

			// Start timer
			start := time.Now()

			epochResult := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/", shards,
				epoch, graphPaperPen, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

			// Carry the graph forward for the next epoch
			graphPaperPen = epochResult.Graph

			// Append the time and epoch results to the slices
			timePaperPen = append(timePaperPen, time.Since(start).Seconds())
			paperPenResults = append(paperPenResults, epochResult)

			// New Penalty and stop on convergence enabled

			// Set CLPA scoring penalty to be the newly proposed penalty formula
			scoringPenalty = paperclpa.CalculateScoresNew

			// newPenaltyClpaCall is inputted to the function as an argument, so that CLPA with the new penalty
			// formula can be called with the 'stop on convergence' toggled on or off according to the test
			// It is passed on to ShardAllocation below

			// Start timer
			start = time.Now()

			epochResult = paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/", shards,
				epoch, graphNewPen, alpha, beta, tau, rho, runClpaIter, newPenaltyClpaCall, scoringPenalty)

			// Carry the graph forward for the next epoch
			graphNewPen = epochResult.Graph

			// Append the time and epoch results to the slices
			timePaperNew = append(timePaperNew, time.Since(start).Seconds())
			newPenResults = append(newPenResults, epochResult)
		}
		tests.WriteSingleResults(paperPenResults, writerPaperPen, test, run)
		tests.WriteSingleResults(newPenResults, writerNewPen, test, run)

		tests.WriteTimes(writerTimes, test, run, timePaperPen, timePaperNew)
	}
	log.Printf("Test finished")
}
