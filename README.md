# Mock Export Challenge

## Overview

You are provided with a mock server that simulates healthcare data exports. Each export
consists of one or more downloadable datasets in CSV format. Your task is to write a
program that processes these datasets and computes record counts.

The goal of this exercise is not only correctness but also clarity, reasoning about
trade-offs, and handling performance constraints. You are encouraged to use internet and
AI resources as part of your process. Be ready to explain and justify your approach in
the follow-up discussion.

## Setup

* Install dependencies using [uv](https://github.com/astral-sh/uv).
* Sync dependencies:

  ```bash
  uv sync
  ```
* Run the server:

  ```bash
  uv run server
  ```
* Run your code:

  ```bash
  uv run cli
  ```
* Add any additional dependencies with:

  ```bash
  uv add <package>
  ```

## Problem Statement

Each export contains multiple downloadable CSV files. Each row represents a simulated
patient event, with the following columns:

* `patient_id`
* `event_time`
* `event_type`
* `value`

Your task is to build a program that:

1. **Discovers exports and their downloads** using the server API.
2. **Processes CSV files** efficiently, taking into account file size and multiple
   downloads.
3. **Produces counts of records** across patients and totals, output as formatted JSON
   printed to stdout.

The expected JSON structure should look like this (aggregated across *all* downloads of
an export):

```json
{
  "patients": {
    "P001": {
      "heart_rate": 1520,
      "spo2": 1470
    }
  },
  "totals": {
    "heart_rate": 8000,
    "spo2": 6000
  }
}
```

### Notes

* Your CLI should accept an **export ID** (`demo`, `small`, or `large`) as an argument
  and run the analysis for that export.
* All counts must be aggregated across *all downloads* belonging to the chosen export.
* Download time ranges are guaranteed to be non-overlapping.

## Constraints

* DO NOT use Pandas or Numpy.
* This exercise is designed for roughly 1-2 hours of focused work.
* The full dataset may be large (millions of rows per download).
* Your solution should be mindful of performance and memory usage.
* Aim for readability and maintainability of code.

## Conclusion

The goal of this challenge is to demonstrate how you approach practical data processing:
discovering data, handling performance trade-offs, producing accurate results, and
presenting them clearly. There is no single “correct” solution-what matters is the
reasoning behind your choices and how you communicate them. We will review and discuss
your results together over a video call, so be prepared to explain and justify your
decisions.

## Submission Instructions

When you have completed the assessment, please submit your work as a **public GitHub
repository**.

* Ensure the repository includes all source code, supporting files, and this README.
* Commit the final JSON output for each export as `demo.json`, `small.json`, and
  `large.json`.
* **DO NOT** submit a pull request to the company’s repositories.
* Provide the link to your public repository to your recruiter or hiring contact.
* During the interview, you will be asked to show off your solution running and do an
  interactive code review. Be ready to share screen and have the project ready.
