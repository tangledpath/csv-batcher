# csv-batcher
A python-based, multiprocess CSV batcher suitable for
use with dataframes or other tools that deal with large CSV files (or those that require timely processing).

## Installation
pip install csv-batcher

## GitHub


## Documentation
https://tangledpath.github.io/csv-batcher/csv_batcher.html

## Usage
Arguments sent to callback function can be controlled by
creating pooler with `callback_with` and the CallbackWith enum
values:

### As dataframe row
```python
  from csv_batcher.csv_pooler import CSVPooler, CallbackWith

  # Callback function passed to pooler; accepts a dataframe row
  #   as a pandas Series (via apply)
  def process_dataframe_row(row):
    return row.iloc[0]

  pooler = CSVPooler(
    "5mSalesRecords.csv",
    process_dataframe_row,
    callback_with=CallbackWith.DATAFRAME_ROW,
    pool_size=16
  )
  pooler.process()

### As dataframe
```python
  from csv_batcher.csv_pooler import CSVPooler, CallbackWith

  # Used in DataFrame.apply:
  def process_dataframe_row(row):
    return row.iloc[0]

  # Callback function passed to pooler; accepts a dataframe:
  def process_dataframe(df):
    foo = df.apply(process_dataframe_row, axis=1)
    # Or do something more complicated....
    return len(df)

  pooler = CSVPooler(
    "5mSalesRecords.csv",
    process_dataframe,
    callback_with=CallbackWith.DATAFRAME,
    pool_size=16
  )
  pooler.process()

### As CSV filename
```python
  from csv_batcher.csv_pooler import CSVPooler, CallbackWith

  def process_csv_filename(csv_chunk_filename):
      # print("processing ", csv_chunk_filename)
      df = pd.read_csv(csv_chunk_filename, skipinitialspace=True, index_col=None)
      foo = df.apply(process_dataframe_row, axis=1)
      return len(df)

  def process_as_dataframe(df):
      foo = df.apply(process_dataframe_row, axis=1)
      return len(df)

  def process_dataframe_row(row):
      return row.iloc[0]

  pooler = CSVPooler(
    "5mSalesRecords.csv",
    process_dataframe,
    callback_with=CallbackWith.CSV_FILENAME
    chunk_lines=10000,
    pool_size=16
  )
  pooler.process()
```
## Development
### Linting
```bash
   ruff check . # Find linting errors
   ruff check . --fix # Auto-fix linting errors (where possible)
```

### Documentation
```
  # Shows in browser
  poetry run pdoc csv_batcher
  # Generates to ./docs
  poetry run pdoc csv_batcher -o ./docs
```

### Testing
```bash
  clear; pytest
```

### Publishing
`poetry publish --build -u __token__ -p $PYPI_TOKEN`
