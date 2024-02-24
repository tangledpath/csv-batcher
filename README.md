# csv-batcher

## Installation
TBD (python package)
pip install csv-batcher

## Documentation
https://tangledpath.github.io/csv-batcher/csv_batcher.html

## Usage
Arguments sent to callback function can be controlled by
creating pooler with `callback_with` and the CallbackWith enum
values:

### As dataframe row
```python
  from csv_batcher.pooler import Pooler, CallbackWith

  # Callback function passed to pooler; accepts a dataframe row
  #   as a pandas Series (via apply)
  def process_dataframe_row(row):
    return row.iloc[0]

  pooler = Pooler(
    "5mSalesRecords.csv",
    process_dataframe_row,
    callback_with=CallbackWith.DATAFRAME_ROW,
    pool_size=16
  )
  pooler.process()

### As dataframe
```python
  from csv_batcher.pooler import Pooler, CallbackWith

  # Used in DataFrame.apply:
  def process_dataframe_row(row):
    return row.iloc[0]

  # Callback function passed to pooler; accepts a dataframe:
  def process_dataframe(df):
    foo = df.apply(process_dataframe_row, axis=1)
    # Or do something more complicated....
    return len(df)

  pooler = Pooler(
    "5mSalesRecords.csv",
    process_dataframe,
    callback_with=CallbackWith.DATAFRAME,
    pool_size=16
  )
  pooler.process()

### As CSV filename
```python
  from csv_batcher.pooler import Pooler, CallbackWith

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

  pooler = Pooler(
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
