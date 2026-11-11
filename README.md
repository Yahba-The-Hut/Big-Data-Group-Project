# CS4371 Group Project 1

> Team README. Each owner adds their own section. This file currently contains **Part 3 (ML model)** and the **handoff notes for Owner 4**.

---

## Part 3: MLlib Text Classifier (AG News)

Trains one Spark MLlib `PipelineModel` on the AG News training set and saves it to `models/ag_news_pipeline`. Owner 4 loads this exact model for both batch and streaming classification. **Do not retrain it.**

### Files

| File | Purpose |
|---|---|
| `get_data.py` | Downloads AG News (train split) from Hugging Face and writes `data/ag_news_train.parquet` |
| `train_model.py` | Builds and fits the MLlib pipeline, prints class counts, and saves the model |
| `smoke_test.py` | Loads the saved model and predicts on 4 sample sentences |
| `models/ag_news_pipeline/` | Saved `PipelineModel` (`metadata/` + `stages/`) |
| `evidence/` | Training log and screenshots |

### Pipeline

`RegexTokenizer` (pattern `\W+`, lowercase) → `StopWordsRemover` → `HashingTF` (2^16 features) → `IDF` → `LogisticRegression` (multinomial, `maxIter=30`, `regParam=0.1`)

### Label map

| Label | Class |
|---|---|
| 0 | World |
| 1 | Sports |
| 2 | Business |
| 3 | Sci/Tech |

### Environment used

- Windows 11, cmd terminal in VS Code
- Python 3.12 with a project venv (`.venv`)
- PySpark **4.3.0** (pip-installed, provides `spark-submit`)
- Java **17.0.12**
- Hadoop Windows binaries (`winutils.exe`, `hadoop.dll`) in `C:\hadoop\bin`, needed on Windows only

### Setup

```cmd
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install pyspark datasets pyarrow "pandas<3"
```

### Windows-only fixes

Spark on Windows needs a few extra settings. Without them you get the errors listed below.

1. **Hadoop binaries**: download `winutils.exe` and `hadoop.dll` (newest `hadoop-3.x` folder from the `cdarlint/winutils` GitHub repo) into `C:\hadoop\bin`. Copy `hadoop.dll` into `C:\Windows\System32` (needs admin).
2. **Environment variables**: set these in every new terminal, or permanently with `setx` followed by a full VS Code restart:

```cmd
set SPARK_HOME=<project folder>\.venv\Lib\site-packages\pyspark
set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python
set SPARK_LOCAL_HOSTNAME=localhost
set HADOOP_HOME=C:\hadoop
set PATH=%PATH%;C:\hadoop\bin
```

| Error | Cause | Fix |
|---|---|---|
| `Missing Python executable 'python3'` / `Failed to find Spark jars directory` | Windows has no `python3` command | Set `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, `SPARK_HOME` |
| `Invalid Spark URL: spark://HeartbeatReceiver@<name>` | Computer name contains an underscore | Set `SPARK_LOCAL_HOSTNAME=localhost` |
| `Did not find winutils.exe` / `HADOOP_HOME ... unset` | Hadoop binaries missing | Install `winutils.exe` and set `HADOOP_HOME` |
| `UnsatisfiedLinkError ... NativeIO$Windows.access0` | `hadoop.dll` not loading | Copy `hadoop.dll` to `System32`, or add `--driver-java-options "-Djava.library.path=C:\hadoop\bin"` |

Mac, Linux, and WSL users can skip all of this.

### Run

```cmd
:: 1. Download data (once)
mkdir data models evidence
python get_data.py

:: 2. Train and save the model
spark-submit --driver-memory 4g train_model.py --train-parquet ./data/ag_news_train.parquet --model-out ./models/ag_news_pipeline

:: 3. Smoke test
spark-submit smoke_test.py
```

If `models/ag_news_pipeline` already exists, the training script overwrites it.

### Results

- **Training rows:** 120,000 (see `evidence/train_log.txt`, line starting `AG NEWS COUNT`)
- **Class distribution:** 4 classes, 30,000 each: World (0) 30,000 · Sports (1) 30,000 · Business (2) 30,000 · Sci/Tech (3) 30,000
- **Smoke test:** all 4 sample sentences classified correctly

| Text | Prediction | Top probability |
|---|---|---|
| The team won the championship | 1 (Sports) | 0.450 |
| Stocks fell after the earnings report | 2 (Business) | 0.421 |
| New smartphone chip announced by Apple | 3 (Sci/Tech) | 0.490 |
| Leaders meet for peace talks in Geneva | 0 (World) | 0.477 |

Top-class probabilities are moderate (about 0.42 to 0.49) because `regParam=0.1` regularizes strongly and flattens the probability distribution. Predictions are still correct. Expect the batch `confidence` column to look similar.

---

## Handoff to Owner 4 (Batch + Streaming Classification)

### What you're getting

- **Model path:** `./models/ag_news_pipeline`
- **Load it with:**

  ```python
  from pyspark.ml import PipelineModel
  model = PipelineModel.load("./models/ag_news_pipeline")
  pred = model.transform(df)   # df needs a string column named "text"
  ```

- **Output columns:** `prediction` (double, 0.0 to 3.0) and `probability` (vector of 4)
- **Label map:** 0 World, 1 Sports, 2 Business, 3 Sci/Tech
- **Trained with:** Spark 4.3.0, Java 17, Windows 11

### Next steps for Owner 4

1. **Check your Spark version** with `spark-submit --version`. Use Spark 4.3.0 if you can, because a model saved in one Spark version may not load in another. If you can't match it, run `spark-submit smoke_test.py` first to confirm the model loads.
2. **Run the smoke test** on your machine before writing anything else. If it prints 4 predictions, the model works on your setup.
3. **On Windows,** apply the fixes in the Windows section above before running anything.
4. **Batch classification (M4.1 and M4.2):** read `topic1` from Kafka (or Owner 1's saved file), keep `id` and `text`, drop empty and duplicate texts, then call `model.transform(clean)`. Map `prediction` to class names using the label map. `confidence` = max of `probability`.
5. **Manual labels (M4.3):** sample 50 rows with `F.rand(seed=4371)`, then hand-label each one as World, Sports, Business, Sci/Tech, or None of these. Compute accuracy on the non-None rows.
6. **Streaming classifier (M4.4 and M4.5):** load the same model in `streaming_classifier.py` and use its own checkpoint folder (for example `./checkpoints/classifier`). Never call `fit()`.

### Things to know

- Hacker News titles are mostly tech, so expect most predictions to be **Sci/Tech** and some messages that fit no class (this is what "None of these" is for in the manual labels).
- Moderate confidence values are expected (see Results above).
- The model was trained on news articles, so short or jargon-heavy HN titles may be misclassified. Collect a few examples for the report's error discussion.

---

## Not committed

`.venv/`, `data/*.parquet` (re-download with `get_data.py`), `checkpoints/`, and any local env scripts with machine-specific paths.
