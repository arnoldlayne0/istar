from dagster import IOManager, io_manager, OutputContext, InputContext
import pandas as pd


class SimplePandasIOManager(IOManager):
    def __init__(self, base_path, bucket_name="datalake", prefix=""):
        self.base_path = base_path
        self.bucket_name = bucket_name
        self.prefix = prefix

    def _get_s3_url(self, context):
        if context.has_asset_key:
            idf = context.get_asset_identifier()
        else:
            idf = context.get_identifier()
        return f"s3://{self.bucket_name}/{self.prefix}{'/'.join(idf)}.parquet"

    def _get_local_url(self, context):
        if context.has_asset_key:
            idf = context.get_asset_identifier()
        else:
            idf = context.get_identifier()
        return f"{self.base_path}/{self.prefix}/{'/'.join(idf)}.csv"

    def handle_output(self, context: OutputContext, df: pd.DataFrame):
        if df is None:
            return

        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"Expected asset to return a df; got {df!r}")
        df.to_csv(self._get_local_url(context))

    def load_input(self, context: InputContext) -> pd.DataFrame:
        return pd.read_csv(self._get_local_url(context))


@io_manager(required_resource_keys={"base_path"})
def local_simple_pandas_io_manager(init_context):
    yield SimplePandasIOManager(base_path=init_context.resources.base_path)


def s3_simple_pandas_io_manager(init_context):
    yield SimplePandasIOManager(base_path="s3://" + init_context.resources.s3_bucket)
