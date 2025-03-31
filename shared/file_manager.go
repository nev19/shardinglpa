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
	"toCreate":    true, // Add to process replacement
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

// Function to split the entire transactions dataset into multiple epoch files
func SplitEpochs(inputFilePath string) {

	outputDir := "output/" // Directory to save split files

	// Ensure the output directory exists
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		fmt.Println("Error creating output directory:", err)
		return
	}

	// Open the input file
	file, err := os.Open(inputFilePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading header:", err)
		return
	}

	// Map to store indices of required columns
	columnIndices := make(map[int]bool)
	for i, col := range header {
		if columnsToKeep[col] {
			columnIndices[i] = true
		}
	}

	// Constants for splitting
	const chunkSize = 100000
	rowCount := 0
	chunkNumber := 1

	// Prepare a new chunk
	var currentChunk [][]string
	currentChunk = append(currentChunk, filterColumns(header, columnIndices, header)) // Add filtered header

	// Read and process rows
	for {
		row, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Println("Error reading row:", err)
			return
		}

		// Filter the row based on required columns
		filteredRow := filterColumns(row, columnIndices, header)
		currentChunk = append(currentChunk, filteredRow)
		rowCount++

		// If the chunk size limit is reached, save the chunk
		if rowCount%chunkSize == 0 {
			saveChunk(outputDir, currentChunk, chunkNumber)
			currentChunk = [][]string{filterColumns(header, columnIndices, header)} // Reset chunk with filtered header
			chunkNumber++
		}
	}

	// Save any remaining rows
	if len(currentChunk) > 1 {
		saveChunk(outputDir, currentChunk, chunkNumber)
	}
	fmt.Println("CSV file successfully split into chunks.")
}

// filterColumns filters a row to keep only the required columns and replaces "None" in "to"
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
	fileName := fmt.Sprintf("%schunk_%d.csv", outputDir, chunkNumber)
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
