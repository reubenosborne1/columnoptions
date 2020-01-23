import pandas as pd
import sys
import datetime


class TypeInfo:
    MAX_VALS = {"float": float("inf"), "date": pd.Timestamp.max, "int": sys.maxsize}

    MIN_VALS = {"float": -float("inf"), "date": pd.Timestamp.min, "int": -sys.maxsize}

    INTERVAL_DTYPES = {
        "int": "interval[int64]",
        "float": "interval[float64]",
        "date": "interval[datetime64[ns]]",
    }

    DTYPE_MAP = {
        "int": int,
        "str": str,
        "bool": bool,
        "float": float,
        "delta": lambda x: datetime.timedelta(x),
        "date": lambda x: pd.to_datetime(x),
        "cat": lambda x: x,
    }

    DTYPE_MAP_REV = {
        type(1): "int",
        type("str"): "str",
        type(True): "bool",
        type(0.1): "float",
        type(pd.to_datetime("01/11/2018")): "date",
    }
