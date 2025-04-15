package penalty

import (
	"encoding/csv"
	"log"
	"strconv"
	"time"

	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
	"example.com/shardinglpa/tests"
)

func RunTestSuite(runs int) {
	log.Println("*********** TEST SUITE 'Paper Penalty vs New Penalty' STARTED *********** (5 Tests in total)")

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

	// The weight of cross-shard vs workload imbalance - 0 to 1
	beta := 0.1

	// The number of iterations of CLPA
	tau := 100

	// The number of shards
	numberOfShards := 8

	// The transaction arrival rate
	arrivalRate := "low"

	// Set CLPA iteration call to be made with update mode set to async, as in paper
	var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

	// END OF SETUP

	// NOW FOR THE TESTS:

	test := 1

	beta = 0.1

	//TEST 1
	log.Println("Started Test " + strconv.Itoa(test) + "/5 - beta = 0.1")
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes)
	test++

	beta = 0.3

	//TEST 2
	log.Println("Started Test " + strconv.Itoa(test) + "/5 - beta = 0.3")
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes)
	test++

	beta = 0.5

	//TEST 3
	log.Println("Started Test " + strconv.Itoa(test) + "/5 - beta = 0.5")
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes)
	test++

	beta = 0.7

	//TEST 4
	log.Println("Started Test " + strconv.Itoa(test) + "/5 - beta = 0.7")
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes)
	test++

	beta = 0.9

	//TEST 5
	log.Println("Started Test " + strconv.Itoa(test) + "/5 - beta = 0.9")
	runTest(test, runs, numberOfShards, arrivalRate, numberOfEpochs, alpha, beta,
		tau, rho, runClpaIter, writerPaperPen, writerNewPen, writerTimes)
	test++

	log.Println("*********** TEST SUITE 'Paper Penalty vs New Penalty' FINISHED ***********")
}

func runTest(test int, runs int, shards int, arrivalRate string, numberOfEpochs int, alpha float64,
	beta float64, tau int, rho int, runClpaIter paperclpa.ClpaIterationMode, writerPaperPen *csv.Writer,
	writerNewPen *csv.Writer, writerTimes *csv.Writer) {

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

			epochResult := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphPaperPen, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

			// Carry the graph forward for the next epoch
			graphPaperPen = epochResult.Graph

			// Append the time and epoch results to the slices
			timePaperPen = append(timePaperPen, time.Since(start).Seconds())
			paperPenResults = append(paperPenResults, epochResult)

			// New Penalty and early break

			// Set CLPA scoring penalty to be the newly proposed penalty formula
			scoringPenalty = paperclpa.CalculateScoresNew

			// Set CLPA to be called with 'stop on convergence' set to on, so early stopping is enabled
			clpaCall = paperclpa.RunClpaConvergenceStop

			// Start timer
			start = time.Now()

			epochResult = paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				shards, epoch, graphNewPen, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

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
