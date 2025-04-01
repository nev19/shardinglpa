package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"time"

	"example.com/shardinglpa/paperclpa"
	"example.com/shardinglpa/shared"
)

func main() {

	// Run this commented out function to extract the epochs from the original dataset
	//extractEpochs()

	// TESTING - cpu profiling
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil)) // Start pprof server
	}()

	// Get the number of logical CPU cores available on the machine
	numberOfCores := runtime.NumCPU()

	numberOfRuns := numberOfCores

	fmt.Printf("Number of CPU cores available: %d\n", numberOfCores)
	fmt.Printf("Number of runs: %d\n", numberOfRuns)

	// The number of epochs
	numberOfEpochs := 3

	// The number of times/threshold each vertex is allowed to update its label (rho)
	rho := 50

	// The weight of cross-shard vs workload imbalance - 0 to 1 (beta)
	beta := 0.5

	// The number of shards
	numberOfShards := 8

	// Create file for results
	filePath := fmt.Sprintf("results/%s.csv", "paperCLPA_lowArrivalRate")
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v\n", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	header := []string{"Seed", "Epoch", "Fitness", "WorkloadImbalance", "CrossShardWorkload", "ConvergenceIterations", "TimeRan", "beta", "numberOfShards"}
	if err := writer.Write(header); err != nil {
		log.Fatalf("Error writing header: %v\n", err)
	}

	// Start clock
	start := time.Now()

	paperResults := paperclpa.ShardAllocation("shared/epochs/low_arrival_rate/", numberOfShards,
		numberOfRuns, numberOfEpochs, rho, 0.5, beta, 100, "async")
	//myResults := mylpa.ShardAllocation("shared/epochs/low_arrival_rate/", numberOfShards,
	//  numberOfRuns, numberOfEpochs, rho, 0.5, beta, 100)

	timeRan := time.Since(start)

	fmt.Printf("\nProgram ran for %s\n", timeRan)

	processResults(paperResults, writer, beta, numberOfShards)
}

func processResults(groupedResults []shared.SeedResults, writer *csv.Writer, beta float64, numberOfShards int) {

	// Sort by Seed
	sort.Slice(groupedResults, func(i, j int) bool {
		return groupedResults[i].Seed < groupedResults[j].Seed
	})

	for _, seedResult := range groupedResults {
		for epochIdx, result := range seedResult.Results {
			// TESTING - print results
			fmt.Printf("%d\t%d\t%.3f\t\t%.3f\t\t%d\t\t%d\t%.1f\n",
				seedResult.Seed, epochIdx+1, result.Fitness, result.WorkloadImbalance, result.CrossShardWorkload, result.ConvergenceIter, result.Duration.Seconds())
		}
	}

	// Write data rows
	for _, seedResult := range groupedResults {
		for epochIdx, result := range seedResult.Results {
			record := []string{
				// Force csv to keep precision, not round
				"\t" + strconv.FormatInt(seedResult.Seed, 10),
				strconv.Itoa(epochIdx + 1),
				fmt.Sprintf("%.3f", result.Fitness),
				fmt.Sprintf("%.3f", result.WorkloadImbalance),
				strconv.Itoa(result.CrossShardWorkload),
				strconv.Itoa(result.ConvergenceIter),
				fmt.Sprintf("%.2f", result.Duration.Seconds()),
				fmt.Sprintf("%.1f", beta),
				strconv.Itoa(numberOfShards),
			}
			if err := writer.Write(record); err != nil {
				fmt.Printf("Error writing record: %v\n", err)
				return
			}
		}
	}

	fmt.Println("CSV file 'results.csv' created successfully.")

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
