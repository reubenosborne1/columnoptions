import pandas as pd
import datetime
import numpy as np


class FunctionsCollection:
    """
    A class holding generic aggregation functions.
    """

    @staticmethod
    def id(col: pd.Series) -> pd.Series:
        return col

    # parsign functions -- all must start with pd.series.
    @staticmethod
    def to_date_str(
        data: pd.Series, input_format: str, output_format: str
    ) -> pd.Series:
        def convert(x):
            try:
                date = datetime.datetime.strptime(x, input_format)
                return datetime.date.strftime(date, output_format)
            except ValueError:
                return "nan"

        return data.apply(convert)

    # non parsing functions
    @staticmethod
    def copy_col(series: pd.Series) -> pd.Series:
        return series

    @staticmethod
    def get_age_float(date_: pd.Series, dob: pd.Series) -> pd.Series:
        ds = date_.__sub__(dob)
        return ds.apply(lambda x: float(x.days / 365.25))

    @staticmethod
    def get_age_delta(date_: pd.Series, dob: pd.Series) -> pd.Series:
        return date_.__sub__(dob)

        # 3.  group by an index_series,

        # 3.  creates a new columns for each treatment specifying the value of a patient col at that treatmentr

    # replace_val_with_keep_key_val_in_each_group
    @staticmethod
    def next_lot_date(
        lots: pd.Series, date: pd.Series, patient_id: pd.Series
    ) -> pd.Series:

        df = pd.concat([lots, date, patient_id], axis=1)
        df.columns = ["lot", "date", "pid"]
        groups = df.groupby(patient_id)

        for group_at_idx in groups:
            df_at_idx = group_at_idx[1]
            sorted = df_at_idx.sort_values(by="lot")

            shifted_dates = list(sorted["date"])[1:] + [pd.NaT]
            df.loc[sorted.index, "new_date"] = shifted_dates

        res_series = df["new_date"].apply(lambda x: pd.to_datetime(x))
        return res_series

    @staticmethod
    def col_for_lot(
        keys: pd.Series, vals: pd.Series, idx: pd.Series, keep_key: int
    ) -> pd.Series:

        df = pd.concat([keys, vals], axis=1)
        df.columns = ["key", "val"]
        res_series = pd.Series([np.nan for x in idx], index=idx.index)
        groups = df.groupby(idx)

        for group_at_idx in groups:
            df_at_idx = group_at_idx[1]

            for (_, row) in df_at_idx.iterrows():
                if row["key"] == keep_key:
                    res_series[df_at_idx.index] = row["val"]

        return res_series
