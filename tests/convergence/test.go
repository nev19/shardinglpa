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

func RunTest(runs int) {

	log.Println("*********** TEST SUITE 'Convergence of CLPA' STARTED ***********")

	// CSV header for recording test times
	header := []string{"run", "epoch", "iter", "labelChanged"}

	filename := "iterations_label_change"

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

	// The number of iterations of CLPA
	tau := 1000

	// The number of times/threshold each vertex is allowed to update its label (rho)
	// For this test, rho is set equal to the number of iterations so it becomes ineffective
	rho := tau

	// The weight of cross-shard vs workload imbalance in fitness calculation
	alpha := 0.5

	// The weight of cross-shard vs workload imbalance - 0 to 1 (beta)
	beta := 0.5

	// The number of shards
	numberOfShards := 8

	// The transaction arrival rate
	arrivalRate := "low"

	// END OF SETUP

	// NOW FOR THE TEST:

	// Set CLPA iteration call to be made with update mode set to async
	var runClpaIter paperclpa.ClpaIterationMode = paperclpa.ClpaIterationAsync

	// Set CLPA to be called with the test for convergence
	var clpaCall paperclpa.ClpaCall = paperclpa.RunClpaConvergenceTest

	// Set CLPA scoring penalty to be same as the one in the paper
	var scoringPenalty paperclpa.ScoringPenalty = paperclpa.CalculateScoresPaper

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
			for iter, changed := range result.LabelChanged {
				record := []string{
					strconv.Itoa(run),            // Run number
					strconv.Itoa(epochIndex + 1), // Epoch number (1-based)
					strconv.Itoa(iter + 1),       // Iteration number (1-based)
					strconv.FormatBool(changed),  // "true" or "false"
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
