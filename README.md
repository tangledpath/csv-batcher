# csv-batcher

## Installation
TBD (python package)
pip install csv-batcher

## Usage
```python
  # Callback function passed to pooler:
  def process_dataframe(df):
    foo = df.apply(df_apply, axis=1)
    # Or do something more complicated....
    return len(df)

  pooler = Pooler(
    "5mSalesRecords.csv",
    process_dataframe,
    as_dataframe=True,
    pool_size=16
  )
  pooler.process()