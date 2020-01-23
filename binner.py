import logging
import datetime
import pandas as pd
import dateutil.parser
import numpy as np
from marshmallow_dataclass import dataclass, Dict, List, Any
from marshmallow import post_dump
from dataclasses import field
from type_info import TypeInfo
from column import Column
import functools

from distutils.util import strtobool
import sys

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
# logger.setLevel(logging.INFO)
DTYPE_MAP = TypeInfo.DTYPE_MAP


class Binner:
    BINNED_SUFFIX = "_binned"
    OTHER_BIN_NAME = "other"
    NULL_BIN_NAME = "NA"

    def condition_index(self, obj: str, dtype: str, series: pd.Series):

        _FUNC = {
            "[": "__ge__",
            "(": "__gt__",
            "]": "__le__",
            ")": "__lt__",
            "==": "__eq__",
        }

        obj = obj.strip()
        INTERVAL_SEP = ".."
        LIST_SEP = ","

        # case [3..76]
        if INTERVAL_SEP in obj:

            left, right = obj[1:-1].split(INTERVAL_SEP)
            left_val = DTYPE_MAP[dtype](left) if left else TypeInfo.MIN_VALS[dtype]
            right_val = DTYPE_MAP[dtype](right) if right else TypeInfo.MAX_VALS[dtype]

            left_brace, right_brace = obj[0], obj[-1]
            left_func = getattr(series, _FUNC[left_brace])
            right_func = getattr(series, _FUNC[right_brace])

            index = left_func(left_val) & right_func(right_val)

            return index

        # case [3,7]
        else:

            indexes = [series == DTYPE_MAP[dtype](x) for x in obj[1:-1].split(LIST_SEP)]
            index = functools.reduce(lambda a, b: a | b, indexes)
            return index

    # col.name, col.
    def get_bin_indices(self, series: pd.Series, dtype, bins) -> pd.Series:

        bin_index = {}
        for bin, range_config in bins.items():
            bin_index[bin] = self.condition_index(range_config, dtype, series)

        values_index = functools.reduce(lambda a, b: a | b, bin_index.values())
        null_index = series.isnull()
        other_index = series.index & ~(null_index | values_index)

        return bin_index, null_index, other_index, values_index

    def get_bin_index_explicit(
        self, series: pd.Series, dtype, bins, bin_options
    ) -> Dict:
        bin_index, null_index, other_index, values_index = self.get_bin_indices(
            series, dtype, bins
        )

        if bin_options["other"]:
            bin_index[self.OTHER_BIN_NAME] = other_index

        if bin_options["null"]:
            bin_index[self.NULL_BIN_NAME] = null_index

        return bin_index

    def get_bin_index(self, series: pd.Series, col: Column) -> Dict:
        return self.get_bin_index_explicit(
            pd.Series, col.dtype, col.bins, col.bin_options
        )

    def apply_bins(self, series: pd.Series, dtype, bins, bin_options) -> pd.Series:

        # get indices
        bin_index = self.get_bin_index_explicit(series, dtype, bins, bin_options)
        binned_series = pd.Series(index=series.index)
        # create the thingy
        for key, idx in bin_index.items():
            binned_series.loc[idx] = key

        return binned_series

    # so stuff to do ...
    def unpack_bins(self, series: pd.Series, remove_nan=True) -> Dict:

        if remove_nan:
            bins = [x for x in series.unique() if not pd.isnull(x)]

        else:
            bins = series.unique()

        bin_indexes = {}
        for bin in bins:
            bin_indexes[bin] = series[series == bin].index

        return bin_indexes
