package main

import "example.com/shardinglpa/paperclpa"

func main() {

	/*
		// Run these commented out lines to extract the epochs from the original dataset

		datasets := []string{
			"shared/originaldataset/0_to_1_Block_Transactions.csv",
			"shared/originaldataset/1_to_2_Block_Transactions.csv",
		}

		maxTransactions := 8_000_000

		shared.SplitMultipleDatasets(datasets, "shared/epochs/low_arrival_rate/", 100_000, maxTransactions)
		shared.SplitMultipleDatasets(datasets, "shared/epochs/high_arrival_rate/", 250_000, maxTransactions)
	*/

	paperclpa.PaperShardAllocation("shared/epochs/low_arrival_rate/", "async")
}
