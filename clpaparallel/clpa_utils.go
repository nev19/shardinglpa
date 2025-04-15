package clpaparallel

// Import necessary packages
import (
	"fmt"
	"math"
	"math/rand"
	"sort"

	"example.com/shardinglpa/shared"
)

// Function to initialise the graph from the data
func updateGraphFromRows(rows [][]string, graph *shared.Graph) *shared.Graph {

	if len(rows) == 0 {
		return graph
	}

	// Parse header row to find "from" and "to" indices
	header := rows[0]
	fromIdx, toIdx := -1, -1
	for i, col := range header {
		if col == "from" {
			fromIdx = i
		}
		if col == "to" {
			toIdx = i
		}
	}
	if fromIdx == -1 || toIdx == -1 {
		fmt.Println("Error: 'from' or 'to' column not found in header.")
		return graph
	}

	// The vertices from previous epoch are kept in the graph, but the edges and number of time updated are cleared
	for _, vertex := range graph.Vertices {
		vertex.Edges = make(map[string]int) // Reset edges
		vertex.LabelUpdateCounter = 0
	}

	for i, row := range rows {
		// Skip the header
		if i == 0 {
			continue
		}
		if len(row) <= max(fromIdx, toIdx) {
			continue // Skip invalid/malformed rows
		}
		from := row[fromIdx]
		to := row[toIdx]

		// Add vertices if they do not already exist in the graph
		if _, exists := graph.Vertices[from]; !exists {
			graph.Vertices[from] = &shared.Vertex{
				ID:    from,
				Label: -1,
				Edges: make(map[string]int),
			}
		}
		if _, exists := graph.Vertices[to]; !exists {
			graph.Vertices[to] = &shared.Vertex{
				ID:    to,
				Label: -1,
				Edges: make(map[string]int),
			}
		}

		// Add the edge between "from" and "to" vertices to the map of edges of both vertices
		// If the edge forms a self-loop, then only add it once
		graph.Vertices[from].Edges[to]++
		if from != to {
			graph.Vertices[to].Edges[from]++
		}
	}

	return graph
}

// Function to set the label of the new vertices in the graph
func initialiseNewVertices(graph *shared.Graph, randomGen *rand.Rand) *shared.Graph {

	// Collect keys and sort them, necessary to be deterministic
	keys := make([]string, 0, len(graph.Vertices))
	for k := range graph.Vertices {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Assign random shards to each new vertex
	for _, k := range keys {
		vertex := graph.Vertices[k]
		if vertex.Label == -1 {
			vertex.Label = randomGen.Intn(graph.NumberOfShards)
			vertex.LabelUpdateCounter = 0
		}
	}
	return graph
}

// Function to calculate from sratch the workload of each shard
func calculateShardWorkloads(graph *shared.Graph) []int {

	workloads := make([]int, graph.NumberOfShards)

	for _, v := range graph.Vertices { // Iterate through all vertices
		for neighbourID, weight := range v.Edges { // Iterate through all neighbours
			if v.Label == graph.Vertices[neighbourID].Label {
				if v.ID < neighbourID { // Ensure each edge is processed only once
					workloads[v.Label] += weight // Intra-shard tx
				} else if v.ID == neighbourID {
					workloads[v.Label] += weight // Special Intra-shard tx, tx with itself
				}
			} else {
				workloads[v.Label] += weight // Cross-shard tx
			}
		}
	}
	return workloads
}

func moveVertex(graph *shared.Graph, vertex *shared.Vertex, newShard int, rho int) {

	// Old shard refers to the shard the vertex was in before the current CLPA iteration
	oldShard := vertex.Label

	// Exit the function if new shard is same as old, since vertex does not need to move
	// Also, Exit the function if current vertex has reached its threshold for updating its label
	if oldShard == newShard || vertex.LabelUpdateCounter >= rho {
		return
	}

	// The shard workloads are not calculated from scratch but rather updated since this is more efficient
	intra, crossWithNew, crossWithOthers, special_intra := 0, 0, 0, 0

	for neighbour, weight := range vertex.Edges {
		if graph.Vertices[neighbour].Label == oldShard {
			// special_intra keeps track of txs that create self-loops
			if graph.Vertices[neighbour].ID == vertex.ID {
				special_intra += weight
			} else {
				intra += weight
			}
		} else if graph.Vertices[neighbour].Label == newShard {
			crossWithNew += weight
		} else {
			crossWithOthers += weight
		}
	}

	// Update the label of the vertex to the new shard
	vertex.Label = newShard

	// Increment the counter for number of times the vertex has updated its label
	vertex.LabelUpdateCounter++

	// Update the workloads of shards
	graph.ShardWorkloads[oldShard] -= (crossWithNew + crossWithOthers + special_intra)
	graph.ShardWorkloads[newShard] += crossWithOthers + intra + special_intra

}

// Score function: calculate how much a shard scores with respect to a vertex
func calculateScores(graph *shared.Graph, v *shared.Vertex, beta float64) []*float64 {

	// Find the minimum workload of a shard
	minWorkload := graph.ShardWorkloads[0]
	for _, w := range graph.ShardWorkloads {
		if w < minWorkload {
			minWorkload = w
		}
	}

	// scores is a slice that will hold the score of each shard for this vertex
	scores := make([]*float64, len(graph.ShardWorkloads))

	// shard represents the variable 'k' in the equation (8) from the paper
	for shard := 0; shard < graph.NumberOfShards; shard++ {

		// Calculate the normalised edge weight contribution to the shard
		edgeWeightWithShard := 0
		totalEdgeWeight := 0

		/* The weight of edges between the vertex being considered (v) and other vertices that reside in
		the shard being considered (shard) is calculated.
		Also, the Total weight of all edges incident to v is also calculated as specified in the paper.
		v represents 'i' in the equation (8) */
		for neighbour, weight := range v.Edges {
			totalEdgeWeight += weight
			if graph.Vertices[neighbour].Label == shard {
				edgeWeightWithShard += weight
			}
		}

		// Calculate the final score for the shard with respect to the vertex

		/* The weight of edges with vertices from the shard being considered (the numerator of the first term
		in the score function provided by the paper) can never be zero, since as stated by the paper:
		the score function is undefined for that particular vertex and shard.
		Therefore, in this implementation, the score is set to nil for that shard, which means it will be
		ignored when later a shard is assigned to the vertex */
		if edgeWeightWithShard <= 0 {
			scores[shard] = nil
		} else {

			// Calculate the first term of the score function
			firstTerm := float64(edgeWeightWithShard) / float64(totalEdgeWeight)

			// Calculate penalty term (second term of the score function)
			penalty := 1 - (beta * (float64(graph.ShardWorkloads[shard]) / float64(minWorkload)))

			// The score of the shard with respect to the vertex is calculated and saved
			scoreValue := firstTerm * penalty
			scores[shard] = &scoreValue
		}
	}
	return scores
}

// If there is a tie in the scores of the shards with respect to a vertex, the winning shard is
// chosen randomly from the highest scoring shards
func getBestShard(scores []*float64, randomGen *rand.Rand) int {

	maxScore := math.Inf(-1)
	var candidateShards []int

	// Find the shard/s with the highest score
	for shard, score := range scores {
		if score == nil {
			continue
		}
		if *score > maxScore {
			maxScore = *score
			candidateShards = []int{shard} // reset with new max
		} else if *score == maxScore {
			candidateShards = append(candidateShards, shard) // in case of tie, more than one shard is returned
		}
	}

	// Pick a shard randomly from candidates
	if len(candidateShards) == 1 {
		return candidateShards[0]
	}
	return candidateShards[randomGen.Intn(len(candidateShards))]
}

// Function to set the random order of traversal of vertices
func setVerticesOrder(graph *shared.Graph, randomGen *rand.Rand) []*shared.Vertex {

	// Extract vertex ids from the map
	ids := make([]string, 0, len(graph.Vertices))
	for id := range graph.Vertices {
		ids = append(ids, id)
	}

	// Sort the keys to enforce an order
	sort.Strings(ids)

	// Create a slice of Vertex pointers in sorted order
	vertices := make([]*shared.Vertex, 0, len(ids))
	for _, key := range ids {
		vertices = append(vertices, graph.Vertices[key])
	}

	// Shuffle the slice randomly
	randomGen.Shuffle(len(vertices), func(i, j int) {
		vertices[i], vertices[j] = vertices[j], vertices[i]
	})

	return vertices
}
