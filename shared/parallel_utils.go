package shared

import (
	"log"
	"math"
)

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

	// Safety check: return an empty graph if there are no results to compare
	if len(seedResults) == 0 {
		log.Println("No results to process.")
		return &Graph{}

	}

	//var bestSeed int64
	var bestFitness float64 = math.MaxFloat64
	var bestGraph *Graph

	for _, result := range seedResults {
		if result.Fitness < bestFitness {
			bestFitness = result.Fitness
			bestGraph = result.Graph
		}
		// Free up memory by deleting the graphs
		result.Graph = nil
	}

	return bestGraph
}
