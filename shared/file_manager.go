package shared

import (
	"encoding/csv"
	"fmt"
	"os"
)

// Columns to retain
var columnsToKeep = map[string]bool{
	"blockNumber": true,
	"from":        true,
	"to":          true,
	"timestamp":   true,
	"toCreate":    false,
}

// Function to read whole CSV file (such as epoch files)
func ReadCSV(filename string) ([][]string, error) {
	// Open the CSV file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the CSV file and read all rows
	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func SplitMultipleDatasets(datasets []string, outputDir string, chunkSize int, maxTransactions int) {

	transactionsRemaining := maxTransactions
	var leftover [][]string
	chunkNumber := 1

	for _, dataset := range datasets {
		fmt.Printf("Processing dataset: %s\n", dataset)
		var err error
		leftover, chunkNumber, transactionsRemaining, err = splitEpochs(dataset, outputDir, chunkNumber, leftover, chunkSize, transactionsRemaining)
		if err != nil {
			fmt.Printf("Error processing dataset %s: %v\n", dataset, err)
			break // stop the creation of epochs
		}
		if transactionsRemaining <= 0 {
			fmt.Println("Reached maxTransactions limit.")
			break
		}
	}

	// Handle any final leftover
	if len(leftover) > 1 {
		saveChunk(outputDir, leftover, chunkNumber)
	}
}

// Function to split the entire transactions dataset into multiple epoch files
// Inputs: Dataset, Directory to save split files
func splitEpochs(inputFilePath string, outputDir string, startChunkNumber int, leftover [][]string,
	chunkSize int, transactionsRemaining int) ([][]string, int, int, error) {

	// Ensure the output directory exists
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return nil, startChunkNumber, transactionsRemaining, fmt.Errorf("error creating output directory: %w", err)
	}

	// Open the input file
	file, err := os.Open(inputFilePath)
	if err != nil {
		return nil, startChunkNumber, transactionsRemaining, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		return nil, startChunkNumber, transactionsRemaining, fmt.Errorf("error reading header: %w", err)
	}

	// Map to store indices of required columns
	columnIndices := make(map[int]bool)
	for i, col := range header {
		if columnsToKeep[col] {
			columnIndices[i] = true
		}
	}

	chunkNumber := startChunkNumber
	currentChunk := [][]string{}

	// Use leftover if available
	if len(leftover) > 0 {
		currentChunk = leftover
	} else {
		currentChunk = append(currentChunk, filterColumns(header, columnIndices, header)) // Add filtered header
	}

	rowCount := len(currentChunk) - 1 // Subtract header

	// Read and process each row
	for transactionsRemaining > 0 {
		row, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, chunkNumber, transactionsRemaining, fmt.Errorf("error reading row: %w", err)
		}

		filteredRow := filterColumns(row, columnIndices, header)
		currentChunk = append(currentChunk, filteredRow)
		rowCount++
		transactionsRemaining--

		if rowCount == chunkSize {
			saveChunk(outputDir, currentChunk, chunkNumber)
			chunkNumber++
			currentChunk = [][]string{filterColumns(header, columnIndices, header)} // reset chunk with header
			rowCount = 0
		}
	}

	// Return leftover (if any)
	if len(currentChunk) > 1 {
		return currentChunk, chunkNumber, transactionsRemaining, nil
	}

	// No leftover
	return nil, chunkNumber, transactionsRemaining, nil
}

// filterColumns filters a row to keep only the required columns and replaces "None" in "to" with "toCreate"
func filterColumns(row []string, columnIndices map[int]bool, header []string) []string {
	var filteredRow []string

	// Find the indices for "to" and "toCreate" columns
	toIndex := -1
	toCreateIndex := -1
	for i, col := range header {
		if col == "to" {
			toIndex = i
		}
		if col == "toCreate" {
			toCreateIndex = i
		}
	}

	// Replace "None" in "to" with the value from "toCreate"
	if toIndex != -1 && toCreateIndex != -1 && row[toIndex] == "None" {
		row[toIndex] = row[toCreateIndex]
	}

	// Process the columns based on columnsToKeep
	for i, value := range row {
		if columnIndices[i] {
			filteredRow = append(filteredRow, value)
		}
	}

	return filteredRow
}

// saveChunk saves a chunk to a CSV file
func saveChunk(outputDir string, chunk [][]string, chunkNumber int) {
	// Create the output file
	fileName := fmt.Sprintf("%sepoch_%d.csv", outputDir, chunkNumber)
	outputFile, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	// Write the chunk to the file
	writer := csv.NewWriter(outputFile)
	err = writer.WriteAll(chunk)
	if err != nil {
		fmt.Println("Error writing to output file:", err)
		return
	}
	writer.Flush()

	fmt.Printf("Chunk %d saved to %s\n", chunkNumber, fileName)
}
