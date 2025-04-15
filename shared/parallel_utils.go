package shared

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
