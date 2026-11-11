# get_data.py
from datasets import load_dataset

ds = load_dataset("fancyzhx/ag_news")
ds["train"].to_parquet("data/ag_news_train.parquet")
print(ds["train"].num_rows)  # should be 120000