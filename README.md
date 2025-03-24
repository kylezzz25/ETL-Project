# ECommerce & COVID-19 ETL Pipeline

## Project Overview

This repository provides a complete end-to-end ETL pipeline that integrates:
- ECommerce event data (Parquet format)
- Global COVID-19 case and death data (CSV format)

Using **Apache Spark (PySpark)** for data transformation, **Luigi** for pipeline orchestration, and **DuckDB** as the analytical serving layer, the pipeline produces a unified view of consumer activity and pandemic trends during the early COVID-19 period.

## Prerequisites and Setup

### Python Environment

- **Python 3.9** is recommended for compatibility.
- Install required dependencies:
  ```bash
  pip install luigi pyspark duckdb pandas
  ```
- If you are using Python 3.12, you may also need to install `python3-distutils` separately.

### Required Data Files

Download the following datasets:

- **ECommerce Events Data (Parquet)**  
  [Download Link](https://drive.google.com/file/d/1IJl-OD_9zcZJZ-05dR2tXurExYpA5nYC/view?usp=drive_link)

- **COVID-19 Global Cases and Deaths Data (CSV)**  
  [Download Link](https://drive.google.com/file/d/1ti20dlXyRj1tFm8jdW94dnnQ7vtgn1hS/view?usp=drive_link)

**Important:** Place both data files in the same directory as `etl_pipeline.py`, unless you modify the script to specify custom paths.

## Directory Structure

A typical project layout should resemble the following:

```
project-root/
├── etl_pipeline.py
├── eCommerce_behavior_cleaned_for_etl_no_view.parquet
├── cleaned_covid_data.csv
├── intermediate/                # Intermediate outputs
│   ├── ecommerce_transformed/
│   └── covid_transformed/
├── output/                      # Final outputs
│   ├── retail_covid_analysis.db
│   ├── etl_complete.txt
│   └── pipeline_complete.txt
├── README.md
```

## Running the Pipeline Locally

1. Clone the repository:
   ```bash
   git clone https://github.com/Prof-Rosario-UCLA/team23.git
   cd team23
   ```

2. Install the required dependencies:
   ```bash
   pip install luigi pyspark duckdb pandas
   ```

3. Download and place the raw data files in the project root directory.

4. Run the pipeline:
   ```bash
   python etl_pipeline.py
   ```

5. Monitor the console output for successful task completions.

## Output Verification

Once the pipeline runs successfully, verify the outputs:

- **Intermediate Results**  
  - Transformed ECommerce data: `intermediate/ecommerce_transformed/`  
  - Transformed COVID-19 data: `intermediate/covid_transformed/`

- **Final Database Output**  
  - DuckDB file: `output/retail_covid_analysis.db`

- **Verification via DuckDB CLI**  
  ```bash
  duckdb output/retail_covid_analysis.db
  ```

  Example queries:
  ```sql
  SELECT COUNT(*) FROM ecommerce;
  SELECT COUNT(*) FROM covid;
  SELECT * FROM ecommerce_covid_joined LIMIT 10;
  ```

- **Success Markers**  
  - `output/etl_complete.txt`  
  - `output/pipeline_complete.txt`

## Troubleshooting and Common Issues

- **Spark Py4JNetworkError / “Answer from Java side is empty”**  
  May occur due to insufficient memory, especially on smaller EC2 instances. Try reducing memory settings:
  ```python
  .config("spark.driver.memory", "2g")
  .config("spark.executor.memory", "1g")
  ```

- **NaN Summations in DuckDB**  
  Missing values in `total_cases` and `total_deaths` are replaced with `0.0` to avoid `NaN` results during aggregation.

- **Missing `distutils` on Python 3.12**  
  `distutils` has been deprecated in Python 3.12. Use Python 3.9 or install `python3-distutils` if available on your system.

## Data Sources

- **ECommerce Events Data:**  
  [Kaggle Dataset – ECommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

- **COVID-19 Global Data:**  
  [Our World in Data GitHub Repository](https://github.com/owid/covid-19-data/tree/master/public/data)

## Visualization

Explore our final visualization dashboard on Tableau:  
[View on Tableau Public](https://public.tableau.com/app/profile/wenyan.zhang1722/viz/Visualization-v2_17424101313210/Dashboard1?publish=yes)
