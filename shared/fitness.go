package shared

import "math"

func CalculateWorkloadImbalance(graph *Graph) float64 {
	// Step 1: Calculate the total workload
	totalWorkload := 0
	for _, workload := range graph.ShardWorkloads {
		totalWorkload += workload
	}

	// Step 2: Calculate the average workload
	averageWorkload := float64(totalWorkload) / float64(len(graph.ShardWorkloads))

	// Step 3: Find the maximum difference between a shard's workload and the average
	maxDifference := 0.0
	for _, workload := range graph.ShardWorkloads {
		difference := math.Abs(float64(workload) - averageWorkload)
		if difference > maxDifference {
			maxDifference = difference
		}
	}

	// Step 4: Return the maximum difference
	return maxDifference
}

func CalculateCrossShardWorkload(graph *Graph) int {
	crossShardWorkload := 0

	// Iterate over all vertices in the graph
	for _, v := range graph.Vertices {
		for neighbour, weight := range v.Edges {
			// Count edges where the vertices are in different shards
			if v.Label != graph.Vertices[neighbour].Label {
				// Only process each edge once
				if v.ID < neighbour {
					crossShardWorkload += weight
				}
			}
		}
	}

	return crossShardWorkload
}

func CalculateFitness(graph *Graph, alpha float64) (float64, int, float64) {
	// Step 1: Calculate workload imbalance
	workloadImbalance := CalculateWorkloadImbalance(graph)

	// Step 2: Calculate cross-shard workload
	crossShardWorkload := CalculateCrossShardWorkload(graph)

	// Step 3: Compute fitness
	fitness := alpha*float64(crossShardWorkload) + (1-alpha)*workloadImbalance

	return workloadImbalance, crossShardWorkload, fitness
}
