package tests

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"

	"example.com/shardinglpa/shared"
)

// createResultsWriter creates a CSV file, writes the header, and returns the CSV writer
func CreateResultsWriter(filename string) (*csv.Writer, *os.File) {

	// CSV header for recording epoch results
	header := []string{"test", "run", "seed", "epoch", "fitness", "workloadImbalance", "crossShardWorkload", "convergenceIterations"}
	//"TimeRan" is removed

	filePath := fmt.Sprintf("tests/%s.csv", filename)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create CSV file '%s': %v\n", filename, err)
	}

	// Create the writer, so it can then be passed on
	writer := csv.NewWriter(file)

	// Write the header
	if err := writer.Write(header); err != nil {
		log.Fatalf("Error writing header to '%s': %v\n", filename, err)
	}

	return writer, file
}

// createTimesWriter creates a CSV file, writes the header, and returns the CSV writer
func CreateTimesWriter(filename string) (*csv.Writer, *os.File) {

	// CSV header for recording test times
	header := []string{"test", "run", "epoch", "timeBaseline", "timeNew"}

	filePath := fmt.Sprintf("tests/%s.csv", filename)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create CSV file '%s': %v\n", filename, err)
	}

	// Create the writer, so it can then be passed on
	writer := csv.NewWriter(file)

	// Write the header
	if err := writer.Write(header); err != nil {
		log.Fatalf("Error writing header to: '%s': %v\n", filename, err)
	}

	return writer, file
}

func GetBestGraph(seedResults []*shared.EpochResult) *shared.Graph {

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
		// Free up memory by deleting the graphs
		result.Graph = nil
	}

	// TESTING
	//fmt.Printf("Best seed: %d with fitness: %.3f\n", bestSeed, bestFitness)
	return bestGraph
}

// Wrapper function used to prepare the results in the right format for the WriteResults function
func WriteSingleResults(results []*shared.EpochResult, writer *csv.Writer, test int, run int) {

	// Create a 2D slice where each result is its own inner slice
	groupedResults := make([][]*shared.EpochResult, len(results))
	for i, result := range results {
		groupedResults[i] = []*shared.EpochResult{result}
	}

	// Call the main writer
	WriteResults(groupedResults, writer, test, run)
}

// Function to write the results of each epoch within each run to csv
func WriteResults(groupedResults [][]*shared.EpochResult, writer *csv.Writer, test int, run int) {

	// Iterate over the results (each epoch within each run) and write to csv
	for epochIndex, seedsResults := range groupedResults {
		for _, result := range seedsResults {
			record := []string{
				strconv.Itoa(test),
				strconv.Itoa(run),
				// Force csv to keep precision, not round the seed
				"\t" + strconv.FormatInt(result.Seed, 10),
				strconv.Itoa(epochIndex + 1),
				fmt.Sprintf("%.3f", result.Fitness),
				fmt.Sprintf("%.3f", result.WorkloadImbalance),
				strconv.Itoa(result.CrossShardWorkload),
				strconv.Itoa(result.ConvergenceIter),
				//fmt.Sprintf("%.3f", result.Duration.Seconds()),
				//fmt.Sprintf("%.1f", beta),
				//arrivalRate,
				//strconv.Itoa(numberOfShards),
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

}

func WriteTimes(writer *csv.Writer, test int, run int, timesBaseline []float64,
	timesNew []float64) {
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
