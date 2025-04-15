package shared

import "math"

func calculateWorkloadImbalance(graph *Graph) float64 {
	// Calculate the total workload
	totalWorkload := 0
	for _, workload := range graph.ShardWorkloads {
		totalWorkload += workload
	}

	// Calculate the average workload
	averageWorkload := float64(totalWorkload) / float64(len(graph.ShardWorkloads))

	// Find the maximum difference between a shard's workload and the average
	maxDifference := 0.0
	for _, workload := range graph.ShardWorkloads {
		difference := math.Abs(float64(workload) - averageWorkload)
		if difference > maxDifference {
			maxDifference = difference
		}
	}

	// Return the maximum difference
	return maxDifference
}

func calculateCrossShardWorkload(graph *Graph) int {
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
	// Calculate workload imbalance
	workloadImbalance := calculateWorkloadImbalance(graph)

	// Calculate cross-shard workload
	crossShardWorkload := calculateCrossShardWorkload(graph)

	// Compute fitness
	fitness := alpha*float64(crossShardWorkload) + (1-alpha)*workloadImbalance

	return workloadImbalance, crossShardWorkload, fitness
}
