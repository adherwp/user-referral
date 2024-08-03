# Data Processing

This project processes multiple CSV files, performs data transformations and validations.

## Overview

The main tasks of this project include:
1. Reading CSV files from a specified directory and loading them into DataFrames.
2. Handling null values, converting data types, and checking for duplicates.
3. Merging multiple DataFrames based on specified join conditions.
4. Applying business logic to validate referral rewards.
5. Saving the processed DataFrames to a local directory.

## Project Structure

- `main.py`: Main script that executes the data processing and upload functions.
- `run.sh`: Shell script for setting up the virtual environment, installing dependencies, and running the `main.py` script.
- `requirements.txt`: List of required Python packages.
- `csv_source/`: Directory containing the source CSV files.
- `csv_cleaned/`: Directory where cleaned CSV files will be saved.

## Setup Instructions

1. **Clone the Repository**

   ```sh
   git clone <repository-url>
   cd <repository-directory>

2. **Run Project**

    ```sh
    run on bash terminal: ./run.sh
    run on zsh terminal: source run.sh
