# Improving Shard Allocation in Account-Based Blockchain State Sharding via Label Propagation Algorithm

This repository contains the implementation and experimentation framework for the B.Sc. Software Development Final Year Project by **Neville Grech**, supervised by **Prof. John Abela**. 

---

## Project Overview

This project investigates how graph-based algorithms can improve shard allocation in **account-based blockchain systems**. It introduces and evaluates adaptations of the **Community Label Propagation Algorithm (CLPA)** for dynamic transaction graphs, with the goal of reducing **cross-shard communication** and **balancing shard workloads**.

### Key Contributions

- **Paper CLPA** — A baseline implementation based on existing academic work
- **Parallel CLPA** — A concurrent version leverging Go’s goroutines for scalability
- **MyLPA** — A novel variant including among other optimisations and additions:
  - A voting-based label update strategy
  - An improved penalty scoring formula for shard selection
- **Experimentation Framework** — Includes multi-seed testing, convergence tracking, and detailed fitness metrics

---

## What This Repo Does

- Reads blockchain transaction graphs divided by epoch
- Performs graph partitioning using different CLPA variants
- Evaluates results using custom fitness, workload imbalance, and cross-shard communication metrics
- Outputs statistics to CSV and logs to file
- Supports profiling, testing modes, and dataset statistics generation

---

## Project Structure

```
├── main.go                  # Entry point for all experiments and analysis
├── paperclpa/               # Implementation of the original CLPA from literature
├── mylpa/                   # Custom MyLPA variant with enhanced logic
├── shared/                  # Graph structures, utilities, and common logic
├── datastats/               # Output directory for dataset statistics CSVs
├── log.txt                  # Combined output + error logging file
```

---

## Example Usage

### Generate Statistics
In `main.go`, uncomment:
```go
writeEpochStatistics(30, "shared/epochs/low_arrival_rate/", "datastats/low_arrival_rate_statistics.csv")
```

### Run a Test Suite
In `main.go`, uncomment:
```go
// penalty.RunTestSuite(50)
// threepart.RunTestSuite(30)
```

---

## Final Report

See the full methodology, design rationale, and evaluation results in the [Final Year Project Report (PDF)](./FYP_Report.pdf).

---

## Requirements

- Go 1.20+
- Epoch-based CSV datasets (see `shared/epochs/`)
- CSV seed file for repeatable experiments

---

## Author

**Neville Grech**  
For B.Sc. (Hons.) in Software Development  
University of Malta, 2025