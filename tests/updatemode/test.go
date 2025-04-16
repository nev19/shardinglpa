package updatemode

import (
	"log"

	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
	"example.com/shardinglpa/tests"
)

func RunTest(runs int) {

	log.Println("*********** TEST 'Update Mode of CLPA' STARTED (1/1) ***********")

	writerAsync, fileAsync := tests.CreateResultsWriter("updatemode/paper_CLPA_async")
	defer writerAsync.Flush()
	defer fileAsync.Close()

	writerSync, fileSync := tests.CreateResultsWriter("updatemode/paper_CLPA_sync")
	defer writerSync.Flush()
	defer fileSync.Close()

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

	// Set CLPA to be called with 'stop on convergence' set to off, as in paper
	var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaPaper

	// Set CLPA scoring penalty to be same as the one in the paper
	var scoringPenalty paperclpa.ScoringPenalty = paperclpa.CalculateScoresPaper

	// END OF SETUP

	// NOW FOR THE TEST:

	test := 1

	for run := 1; run <= runs; run++ {

		// Graph pointers initialized to nil; updated after each epoch for cumulative evolution
		var graphAsync *shared.Graph = nil
		var graphSync *shared.Graph = nil

		// Stores results per mode across all epochs for the current run
		var asyncResults []*shared.EpochResult
		var syncResults []*shared.EpochResult

		// Iterate over the epochs
		for epoch := 1; epoch <= numberOfEpochs; epoch++ {

			// Async Update Mode

			// Set CLPA iteration call to be made with update mode set to async
			var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

			epochResult := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				numberOfShards, epoch, graphAsync, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

			// Carry the graph forward for the next epoch
			graphAsync = epochResult.Graph

			// Append the result for the current epoch to the epochResults slice
			asyncResults = append(asyncResults, epochResult)

			// Sync Update Mode

			// Set CLPA iteration call to be made with update mode set to sync
			runClpaIter = paperclpa.ClpaIterationSync

			epochResult = paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				numberOfShards, epoch, graphSync, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

			// Carry the graph forward for the next epoch
			graphSync = epochResult.Graph

			// Append the result for the current epoch to the epochResults slice
			syncResults = append(syncResults, epochResult)

		}
		tests.WriteSingleResults(asyncResults, writerAsync, test, run)
		tests.WriteSingleResults(syncResults, writerSync, test, run)
	}
	log.Printf("Test finished")
}
