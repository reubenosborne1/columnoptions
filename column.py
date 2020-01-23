import logging
import datetime
import pandas as pd
import dateutil.parser
import numpy as np
from marshmallow_dataclass import dataclass, Dict, List, Any
from marshmallow import post_dump
from dataclasses import field
from type_info import TypeInfo
import functools
from distutils.util import strtobool


import sys

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
# logger.setLevel(logging.INFO)
DTYPE_MAP = TypeInfo.DTYPE_MAP


@dataclass
class Column:
    name: str
    dtype: str = None
    kind: str = None
    reducer: str = None
    as_selection: bool = False
    parse_funcs: List[str] = field(default_factory=list)
    parse_kwargs: List[Dict[str, str]] = field(default_factory=dict)
    friendly_name: str = None
    bins: Dict[str, str] = field(default_factory=dict)
    bin_include: Dict[str, bool] = field(default_factory=dict)
    bin_name: str = None
    filters: Dict[str, Any] = field(default_factory=dict)
    create_func: str = None
    create_args: List[str] = field(default_factory=list)
    create_kwargs: Dict[str, Any] = field(default_factory=dict)

    SKIP_VALUES = [None, "", [], {}]

    def __post_init__(self):
        self.idx = None
        self.parsed = False
        self.binned = False
        self.renamed = False
        self.filtered = False
        self.dtype_normalized = False
        self.created = False

    @post_dump
    def remove_empty(self, data, **kwargs):
        """Removes empty values from the dumped dict"""
        return {
            key: value for key, value in data.items() if value not in self.SKIP_VALUES
        }

    def set_dtype(self, dtype: str):
        """Declares a new data type for a column. Resets dtype_normalized to False"""
        if not DTYPE_MAP.__contains__(dtype):
            raise TypeError(
                f"Data type `{dtype}` is not known! Options are: {list(DTYPE_MAP)}"
            )
        self.dtype_normalized = False
        self.dtype = dtype

    def set_friendly_name(self, new_friendly_name: str):
        self.friendly_name = new_friendly_name
