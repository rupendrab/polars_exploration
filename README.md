# Polars: A Practical Introduction to Fast DataFrames in Python

I have been analyzing data in Python for more than 10 years. Early on, I wrote a lot of custom Python functions for data
processing. That approach was flexible and much easier to develop than lower-level languages such as Java or C++, but it
was also easy to get wrong. As datasets grew larger, performance and memory usage became real concerns, especially when
working with millions of rows and many columns. Even routine operations such as sorting, grouping, deduplication, joins,
and window calculations could take a lot of code and careful testing.

Later, I started using dataframe libraries such as pandas and PySpark. They made many tasks easier, but each came with
tradeoffs. In my experience, pandas is productive for small to medium-sized workloads, but large datasets can quickly put
pressure on memory, and it does not generally take advantage of multiple CPU cores for many operations. PySpark is very
powerful for distributed data processing, but it also brings the complexity of a Spark-based environment, which can feel
heavy for local or moderate-scale workflows.

More recently, I started exploring Polars, a newer dataframe library written in Rust. Polars is designed for high
performance, supports a lazy execution model through `LazyFrame`, and can parallelize many operations. That combination
makes it a strong option for building data transformation pipelines that are both expressive and efficient.

So far, I have found Polars to be a very useful library both for ad-hoc data exploration and analysis, and for building critical data APIs and pipelines. I have been impressed by it and wanted to share that journey here.

In this article, I will walk through some basic features of Polars using the NYC TLC Trip Record Data, which is publicly available at [the official TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). While I will share code snippets directly in the article for clarity, the entire codebase is available on [GitHub](https://github.com/rupendrab/polars_exploration/tree/data_analysis_initial).

## 1. Download the data

You can download these data files:
1. [Yellow Taxi Trip Data Jan 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet)
2. [Yellow Taxi Trip Data Feb 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet)
3. [Green Taxi Trip Data Jan 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-01.parquet)
4. [Green Taxi Trip Data Feb 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-02.parquet)
5. [Location Reference Data](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

You can download them directly, use `curl`, or run the included module. For example:

```bash
python -m polars_exploration.nyctaxi.download_data 2026 1
python -m polars_exploration.nyctaxi.download_data 2026 2
```

## 2. Set up the Python environment
If you use Poetry or uv, you can use the provided `pyproject.toml` and run `poetry install` or `uv sync`.
If you prefer `pip` instead, run:

```bash
pip install polars
pip install jupyterlab
pip install matplotlib
```
The current scripts were tested using Python 3.12. Polars is the only library that you strictly need, but I have added JupyterLab and Matplotlib for notebooks and relevant plots.

## 3. Reading Data with Lazy Scans

Use the `scan_parquet()` function to create a lazy reference to one or more Parquet files. Unlike `read_parquet()`, which immediately loads data into memory, `scan_parquet()` returns a `LazyFrame` and defers execution until a result is actually needed.

Polars supports a wide variety of data sources, including CSV, Excel, JSON, NDJSON, Avro, Parquet, and ODS (OpenDocument Spreadsheet) files. It can also read from databases and Delta Lake tables. For many file formats, Polars provides both eager and lazy APIs. For example:

* `read_csv()` vs. `scan_csv()`
* `read_parquet()` vs. `scan_parquet()`
* `read_ndjson()` vs. `scan_ndjson()`

The `scan_*` functions are often preferred when working with large datasets because they enable Polars' query optimizer and lazy execution engine. Instead of materializing the entire dataset up front, Polars builds a logical execution plan and postpones execution until an action such as `collect()` or `sink_parquet()` is invoked.

This approach provides several important benefits:

### Predicate Pushdown

Filters are pushed as close to the data source as possible, reducing the amount of data that must be read.

```python
df = (
    pl.scan_parquet("Your Parquet File Name")
    .filter(pl.col("fare_amount") > 50)
)
```

Rather than loading all rows and then filtering them, Polars attempts to apply the filter while reading the data.

### Projection Pushdown

Only the columns required by the query are loaded.

```python
df = (
    pl.scan_parquet("Your Parquet File Name")
    .select(["VendorID", "fare_amount"])
)
```

If the source file contains 50 columns but only 2 are needed, Polars can avoid reading the other 48 columns.

### Query Optimization

Polars analyzes the entire query plan before execution and can reorder or combine operations to improve performance.

### Streaming Execution

Some queries can be executed with Polars' streaming engine, which processes data in chunks and can reduce memory pressure for large workloads.

```python
result = (
    pl.scan_parquet("../data/yellow_tripdata_2026-0[1-2].parquet")
    .group_by("VendorID")
    .agg(pl.len().alias("rides"))
    .collect()
)
```

### When Does the Data Get Read?

When you call `scan_parquet()`, Polars creates a query plan. It does not materialize the full dataset at that point.

The query executes only when an action such as the following is called:

```python
.collect()
.sink_parquet("output.parquet")
```

For formats such as Parquet, this can significantly reduce both memory usage and execution time because Polars can often read only the columns and row groups needed for the final result.


With that, let's explore the data using Polars DataFrames.

Start a Jupyter notebook by running `jupyter lab`, then create a new notebook in the `notebooks/` directory. The relative paths below assume the notebook is running from there.

Import Polars:
```python
import polars as pl
import matplotlib.pyplot as plt  # For plot visualizations

# Read the January and February yellow taxi trip data files
# and display the total row count and the first few records.

counts = (
    pl.scan_parquet("../data/yellow_tripdata_2026-0[1-2].parquet")
    .select(pl.len().alias("Count"))
    .collect()
)

# Display the number of records
display(counts)

# Display the first 5 records
df = (
    pl.scan_parquet("../data/yellow_tripdata_2026-0[1-2].parquet")
    .head(5)
    .collect()
)

display(df)
```

The output looks something like the following. There are more columns in the dataset, but they are excluded here for readability.

![Chart](images/Preview_Dataframe.png)

Note that the `scan_parquet()` method accepts a file name with a glob pattern. The pattern `"2026-0[1-2]"` matches both `2026-01` and `2026-02`, so it reads both the January and February 2026 files. It also accepts a list of file names.

These two monthly files contain more than 7 million records, but `scan_parquet()` still returns quickly because it creates a lazy query plan instead of materializing the full dataset immediately.

## 4. Exploring the schema

You would have noticed that Polars has also picked up the schema embedded in the Parquet file. For other file types such as CSV, Polars can infer the schema from sampled data. We can inspect the schema using `collect_schema()` for scanned DataFrames, or the `schema` property for in-memory DataFrames.

Let's use these functions to see if the yellow taxi and the green taxi datasets are compatible, i.e., do they have the same columns or are they completely different?

```python
# Read the dataframes and extract the column names from the schema
yellow_cols = pl.scan_parquet("../data/yellow_tripdata_2026-01.parquet").collect_schema().names()
green_cols = pl.scan_parquet("../data/green_tripdata_2026-01.parquet").collect_schema().names()

# Check columns only in yellow, not in green
print("Only in Yellow")
print(set(yellow_cols) - set(green_cols))
print()

# Check columns only in green, not in yellow
print("Only in Green")
print(set(green_cols) - set(yellow_cols))
print()

# Check which columns appear in both
print("Common to both")
print(set(yellow_cols).intersection(set(green_cols)))
```

You will notice that while there are some columns that differ, a majority of columns are common between the two. This means we can concatenate the datasets together and analyze the common columns.

## 5. Creating a combined dataframe from multiple dataframes

Polars lets you vertically stack multiple DataFrames even when their schemas do not match exactly. It can create a new DataFrame that combines all columns from the input DataFrames, aligns matching columns by name, and fills missing values with null where a column does not exist in one of the inputs.

```python
yellow_df = pl.scan_parquet("../data/yellow_tripdata_2026-01.parquet")
green_df = pl.scan_parquet("../data/green_tripdata_2026-01.parquet")

combined_df = pl.concat(
    [
        yellow_df,
        green_df
    ],
    how="diagonal"
)

combined_df.head(5).collect()
```

This code scans the January 2026 yellow and green taxi trip Parquet files. The two lazy DataFrames are then combined using `pl.concat()` with `how="diagonal"`.

The diagonal option is useful when the two datasets do not have exactly the same columns. Polars creates a combined schema containing all columns from both files, aligns matching columns by name, and fills missing values with null where a column exists in one dataset but not the other.

Finally, `combined_df.head(5).collect()` executes the lazy query and returns the first five rows of the combined result. You will notice that all columns from the two datasets coexist in the final dataframe.

## 6. Filtering Data

Filtering is one of the most common operations in any data workflow. In Polars, you typically use `.filter()` together with one or more column expressions created with `pl.col(...)`.

For example, suppose we want to find yellow taxi rides that meet all of the following conditions:

* the fare amount is greater than 20 dollars
* the tip amount is greater than 3 dollars
* the trip distance is greater than 2 miles

Here is one way to write that query:

```python
filtered_count = (
    pl.scan_parquet("../data/yellow_tripdata_2026-0[1-2].parquet")
    .filter(
        (pl.col("fare_amount") > 20)
        & (pl.col("tip_amount") > 3)
        & (pl.col("trip_distance") > 2)
    )
    .select(pl.len().alias("matching_rides"))
    .collect()
)

display(filtered_count)
```

A column is referenced as `pl.col("column_name")` in Polars. Multiple filter conditions can be combined using `&` for AND and `|` for OR. The `~` operator is used for negation (NOT).

You can also select one or more columns using the `select()` function, and each column may also be given an alias. In this case, `select(pl.len().alias("matching_rides"))` is selecting the count of records with `pl.len()` and aliasing the column as `"matching_rides"`.

Also notice the method chaining with the `.` operator. This lets you build a data pipeline in a single statement while keeping each transformation readable. For many Python users, this can feel more intuitive than writing the equivalent SQL.

The output will look something like this:

```text
shape: (1, 1)
┌────────────────┐
│ matching_rides │
│ ---            │
│ u32            │
╞════════════════╡
│ 883596         │
└────────────────┘
```

You can make the filter more selective by adding more conditions, or broaden it by removing some of them. In practice, this pattern is a simple and readable way to answer questions such as "How many rides match a business rule?" before moving on to deeper analysis.
