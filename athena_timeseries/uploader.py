from typing import Optional, Dict
import pandas as pd
import awswrangler
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
import time


def upload(
    *,
    boto3_session,
    glue_db_name: str,
    s3_path: str,
    table_name: str,
    df: pd.DataFrame,
    dtype: Optional[Dict[str, str]] = None,
):
    _dtype = {
        "partition_dt": "date",
        "dt": "timestamp",
        "symbol": "string",
    }

    for (key, value) in _dtype.items():
        if key not in df.columns:
            raise ValueError(f"Column {key} must be given with dtype {value}")

    if dtype is not None:
        for k, v in dtype.items():
            _dtype[k] = v

    console = Console()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(description="Uploading data...", total=None)

        start_time = time.time()

        result = awswrangler.s3.to_parquet(
            df=df,
            partition_cols=["partition_dt", "symbol"],
            dataset=True,
            database=glue_db_name,
            table=table_name,
            path=f"{s3_path}/{table_name}",
            boto3_session=boto3_session,
            mode="overwrite_partitions",
            concurrent_partitioning=True,
            dtype=_dtype,
            compression="snappy",  # Snappy圧縮を適用
        )

        end_time = time.time()

        progress.update(task, completed=True, description="Upload completed!")

    # テーブルサイズの計算
    table_size = df.memory_usage(deep=True).sum()
    table_size_mb = table_size / (1024 * 1024)  # バイトからメガバイトに変換

    # アップロード時間の計算
    upload_time = end_time - start_time

    console.print(f"Table size: {table_size_mb:.2f} MB")
    console.print(f"Upload time: {upload_time:.2f} seconds")

    return result
