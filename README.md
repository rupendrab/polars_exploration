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

In this article, I will walk through some basic features of Polars using the NYC TLC Trip Record Data, which is publicly available at [the official TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). While I will share code snippets directly in the article for clarity, the entire codebase will be available on Github. (Link here).

### 1.Download the data

You can download these data files:
1. [Yellow Taxi Trip Data Jan 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet)
2. [Yellow Taxi trip Data Feb 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet)
3. [Green Taxi Trip Data Jan 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-01.parquet)
4. [Green Taxi Trip Data Feb 2026](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-02.parquet)
5. [Location Reference Data](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

You can download them directly, use curl or even use the python script module polars_exploration.nyctaxi.download_data

### 2. Set up the Python environment
If you use poetry or uv, you can use the provided pyproject.toml and run `poetry install` or `uv init`. 
If you prefer pip instead, just run these:
```
pip install polars
pip install jupyterlab
pip install matplotlib
```
The current scripts were tested usinfg Python 3.12 but anything over 3.9 should work. Polars is the only library that you strictly need, but I have added the other two to use the Jupyter notebook and to show some relevant plots.



