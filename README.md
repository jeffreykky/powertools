# PowerTools

PowerTools is a utility library designed to simplify and enhance your experience with Python, Apache Spark, and AWS Glue Spark. It provides a collection of tools and functions to streamline your data processing workflows.

# Table of Contents

- [PowerTools](#powertools)
- [Table of Contents](#table-of-contents)
- [Installation](#installation)
- [Usage](#usage)
  - [Quick Start](#quick-start)
  - [Python Utilities](#python-utilities)
  - [Spark Utilities](#spark-utilities)
  - [Glue Spark Utilities](#glue-spark-utilities)
    - [1. Read](#1-read)
      - [CSV](#csv)
        - [PARQUET](#parquet)
      - [HUDI](#hudi)
      - [DELTA LAKE](#delta-lake)
    - [2. Tran](#2-tran)
    - [3. Write](#3-write)
    - [4. Log](#4-log)
    - [5. AWS](#5-aws)
  - [Contributing](#contributing)
  - [License](#license)

# Installation

You can install PowerTools using pip:

```bash
pip install powertools
```

# Usage

## Quick Start
```py

from lps_glue import LPSGlue

with LPSGlue(spark_shell=True) as lpsglue:
    df = lpsglue.read.csv(path)   # Read data from CSV
    df = lpsglue.tran.add_column(df, 'example_col1', f.lit('example'))  # Add column
    lpsglue.write.hudi(
        df=df,
        path=path,
        primary_key='pk1',
        partition_by=["part1", "part2"]
        order_by='ts',
        dedup=False
    ) # Write df in HUDI format
```

## Python Utilities
<span style="color:yellow">
*Work In Progress:*
</span>

1. data manipulation using `pandas`
2. parallelization using `concurrent.futures`
3. and more. Stay tuned for updates!

## Spark Utilities

<span style="color:red">
*Coming Soon*
</span>

## Glue Spark Utilities
</span>

There are 5 main modules available in Glue Spark Utilities.

### 1. Read
Read data in **ANY** format using Spark without dependencies installation.

#### CSV
```py
  lpsglue.read.csv(path=filename)
```

##### PARQUET
```py
  lpsglue.read.parquet(path=filename)
```
#### HUDI
```py
  lpsglue.read.hudi(path=filename)
```
#### DELTA LAKE
```py
  lpsglue.read.delta(path=filename)
```
   
### 2. Tran
  
### 3. Write
### 4. Log
### 5. AWS
## Contributing

We welcome contributions to PowerTools! If you have any ideas, suggestions, or bug reports, please open an issue or submit a pull request on our [GitHub repository](https://github.com/your-repo/powertools).

## License

PowerTools is licensed under the [MIT License](https://opensource.org/licenses/MIT).

