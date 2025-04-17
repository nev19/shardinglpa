package convergence

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
)

func RunTestSuite(runs int) {

	log.Println("****** TEST SUITE 'Convergence of CLPA with Paper Penalty and New Penalty' STARTED (2 Tests in Total) ******")

	// TEST 1
	// Set the name of the file to write to
	filename := "iterations_info_paper_penalty"

	// Set CLPA scoring penalty to be same as the one in the paper
	var scoringPenalty paperclpa.ScoringPenalty = paperclpa.CalculateScoresPaper

	// Call test 1
	log.Println("Started Test 1/2 - Paper Penalty Convergence")
	RunTest(runs, filename, scoringPenalty)

	// TEST 2
	// Set the name of the file to write to
	filename = "iterations_info_new_penalty"

	// Set CLPA scoring penalty to be the newly proposed penalty formula
	scoringPenalty = paperclpa.CalculateScoresNew

	// Call test 2
	log.Println("Started Test 2/2 - New Penalty Convergence")
	RunTest(runs, filename, scoringPenalty)

	log.Println("****** TEST SUITE 'Convergence of CLPA with Paper Penalty and New Penalty' FINISHED ******")
}

func RunTest(runs int, filename string, scoringPenalty paperclpa.ScoringPenalty) {

	// CSV header for recording test times
	header := []string{"run", "epoch", "iteration", "labelChanged", "fitness"}

	filePath := fmt.Sprintf("tests/convergence/%s.csv", filename)
	fileConvergence, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create CSV file '%s': %v\n", filename, err)
	}
	// Defer after successful creation
	defer fileConvergence.Close()

	// Create the writer
	writerConvergence := csv.NewWriter(fileConvergence)

	// Defer after successful writer initialisation
	defer writerConvergence.Flush()

	// Write the header
	if err := writerConvergence.Write(header); err != nil {
		log.Fatalf("Error writing header to: '%s': %v\n", filename, err)
	}

	// The number of epochs to be run
	numberOfEpochs := 30

	// The number of iterations of CLPA is set to a greater number than 100, in the paper tau = 100
	tau := 500

	// The number of times/threshold each vertex is allowed to update its label (rho)
	rho := 50

	// The weight of cross-shard vs workload imbalance in fitness calculation
	alpha := 0.5

	// The weight of cross-shard vs workload imbalance in score function
	beta := 0.5

	// The number of shards
	numberOfShards := 8

	// The transaction arrival rate
	arrivalRate := "low"

	// END OF SETUP

	// Set CLPA iteration call to be made with update mode set to async
	var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

	// Set CLPA to be called with the test for convergence
	var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaConvergenceTest

	// NOW FOR THE TEST:

	for run := 1; run <= runs; run++ {

		var graph *shared.Graph = nil

		var paperclpaResults []*shared.EpochResult

		// Iterate over the epochs
		for epoch := 1; epoch <= numberOfEpochs; epoch++ {

			epochResult := paperclpa.ShardAllocation("shared/epochs/"+arrivalRate+"_arrival_rate/",
				numberOfShards, epoch, graph, alpha, beta, tau, rho, runClpaIter, clpaCall, scoringPenalty)

			// Carry the graph forward for the next epoch
			graph = epochResult.Graph

			// Append the result for the current epoch to the epochResults slice
			paperclpaResults = append(paperclpaResults, epochResult)
		}

		// Write CSV rows: each row corresponds to an iteration in an epoch
		for epochIndex, result := range paperclpaResults {

			// Safety check: make sure IterationsInfo exists
			if result.IterationsInfo == nil {
				log.Printf("Warning: IterationsInfo is nil for run %d, epoch %d", run, epochIndex+1)
				continue
			}

			for iter, changed := range result.IterationsInfo.LabelChanged {

				// Get the corresponding fitness for this iteration
				fitness := 0.0
				if iter < len(result.IterationsInfo.Fitness) {
					fitness = result.IterationsInfo.Fitness[iter]
				} else {
					log.Printf("Warning: missing fitness value for run %d, epoch %d, iter %d", run, epochIndex+1, iter+1)
				}

				// Prepare row for writing to csv
				record := []string{
					strconv.Itoa(run),            // Run number
					strconv.Itoa(epochIndex + 1), // Epoch number (1-based)
					strconv.Itoa(iter + 1),       // Iteration number (1-based)
					strconv.FormatBool(changed),  // "TRUE" or "FALSE"
					fmt.Sprintf("%.3f", fitness), // Fitness of partitioning
				}

				if err := writerConvergence.Write(record); err != nil {
					log.Printf("Error writing record to CSV: %v", err)
				}
			}
		}

		// Flush after each run to ensure everything is written
		writerConvergence.Flush()

		// Log any possible error
		if err := writerConvergence.Error(); err != nil {
			log.Printf("Error flushing CSV writer: %v", err)
		}
	}
	log.Printf("Test finished")
}
