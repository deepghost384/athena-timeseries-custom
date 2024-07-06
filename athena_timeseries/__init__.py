from typing import Optional, Dict, List, Literal
import pandas as pd

from .sql.resample import resample_query, Expr
from .sql.basic import query
from .sql.basic import query as sql_query
from .sql.resample import resample_query as sql_resample_query
from .uploader import upload


__all__ = ["AthenaTimeSeries"]


class AthenaTimeSeries:
    def __init__(self, boto3_session, glue_db_name: str, s3_path: str):
        self.boto3_session = boto3_session
        self.glue_db_name = glue_db_name
        self.s3_path = s3_path

    def query(
        self,
        *,
        table_name: str,
        fields: str,
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        max_cache_expires: Optional[int] = None,
    ) -> pd.DataFrame:
        return query(
            boto3_session=self.boto3_session,
            glue_db_name=self.glue_db_name,
            table_name=table_name,
            fields=fields,
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            max_cache_expires=max_cache_expires,
        )

    def resample_query(self, table_name, fields, start_dt=None, end_dt=None, symbols=None, interval='day', tz=None, ops=None, where=None, cast=None, verbose=0, fast=True, offset_repr=None):
        return sql_resample_query(
            boto3_session=self.boto3_session,
            glue_db_name=self.glue_db_name,
            table_name=table_name,
            fields=fields,
            start_dt=start_dt,
            end_dt=end_dt,
            symbols=symbols,
            interval=interval,
            tz=tz,
            ops=ops,
            where=where,
            cast=cast,
            verbose=verbose,
            fast=fast,
            offset_repr=offset_repr
        )

    def upload(
        self,
        *,
        table_name: str,
        df: pd.DataFrame,
        dtype: Optional[Dict[str, str]] = None,
        mode: Literal['append', 'overwrite', 'overwrite_partitions'] = 'overwrite_partitions'
    ):
        return upload(
            boto3_session=self.boto3_session,
            glue_db_name=self.glue_db_name,
            s3_path=self.s3_path,
            table_name=table_name,
            df=df,
            dtype=dtype,
            mode=mode,
        )
