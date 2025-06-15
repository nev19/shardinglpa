package shared

import (
	"log"
	"math"
)

// DeepCopyGraph creates a deep copy of the graph structure
// Used to allow parallel goroutines to work independently on separate graph copies
func DeepCopyGraph(original *Graph) *Graph {
	copy := &Graph{
		Vertices:       make(map[string]*Vertex),
		NumberOfShards: original.NumberOfShards,
	}

	// Copy vertices
	for id, v := range original.Vertices {
		copy.Vertices[id] = &Vertex{
			ID:                 v.ID,
			Label:              v.Label,
			Edges:              v.Edges,
			LabelUpdateCounter: v.LabelUpdateCounter,
			NewLabel:           v.NewLabel,
		}
	}

	return copy
}

// Function that finds the best graph in terms of fitness and returns it
// The rest of the graphs are discarded
func GetBestGraph(seedResults []*EpochResult) *Graph {

	// Safety check: if there are no results, return an empty Graph instance and log the issue
	if len(seedResults) == 0 {
		log.Println("No results to process.")
		return &Graph{}

	}

	// Initialize bestFitness to the maximum possible float64 value
	// This ensures any actual fitness value will be smaller
	var bestFitness float64 = math.MaxFloat64

	// Variable to store the graph with the best (lowest) fitness.
	var bestGraph *Graph

	for _, result := range seedResults {

		// Update the bestFitness and bestGraph if the current result is better
		if result.Fitness < bestFitness {
			bestFitness = result.Fitness
			bestGraph = result.Graph
		}

		// Free up memory by discarding the reference to the graph
		result.Graph = nil
	}

	return bestGraph
}
