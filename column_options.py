import logging
import datetime
from col_creation_library import FunctionsCollection
import dateutil.parser

import numpy as np
from marshmallow_dataclass import dataclass, Dict, List, Any
from marshmallow import post_dump
from dataclasses import field
from type_info import TypeInfo
import functools
from distutils.util import strtobool

# import sys
from binner import Binner
import pandas as pd


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
# logger.setLevel(logging.INFO)
DTYPE_MAP = TypeInfo.DTYPE_MAP


# age at applied to itself


@dataclass
class Column:
    name: str
    dtype: str = None
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


@dataclass
class ColumnStack:
    nameSpace: Dict[str, str] = field(default_factory=dict)
    columnOptions: List[Column] = field(default_factory=list)

    def __post_init__(self):
        """
        Post-init makes sure the `self.columnOptions` only contains Column objects
        """

        self.nameSpaced: bool = False
        for n, col in enumerate(self.columnOptions):
            if not isinstance(col, Column):
                self.columnOptions[n] = Column(col)

    def get_full_list(self) -> list:
        """Returns a list of column names in the Stack"""
        return [x.name for x in self.columnOptions]

    def get_unparsed(self) -> List:
        condition = lambda x: all([x.parse_funcs, not x.parsed])
        return [x for x in self.columnOptions if condition(x)]

    def get_aggr_objs(self) -> list:
        """Returns a list of columns that need aggregating"""
        condition = lambda x: all([x.create_func, not x.created])
        return [x for x in self.columnOptions if condition(x)]

    def get_non_aggr_objs(self) -> list:
        """Returns a list of columns that need aggregating"""
        condition = lambda x: all([x.create_func])
        return [x for x in self.columnOptions if not condition(x)]

    def get_unbinned_objs(self) -> list:
        condition = lambda x: all([x.bins, not x.binned])
        return [x for x in self.columnOptions if condition(x)]

    def add_column(self, name: str) -> Column:
        """Adds a new column to the Stack"""
        self.columnOptions.append(Column(name))
        logger.info(f"  -> New column added to Stack: `{name}`")
        return self[name]

    def get_columns(self, col_names):
        return [x for x in self.columnOptions if x.name in col_names]

    def append_column(self, col: Column) -> Column:
        """Adds a new column to the Stack"""
        self.columnOptions.append(col)
        logger.info(f"  -> New column added to Stack: `{col.name}`")

    @property
    def as_dict(self) -> dict:
        return {x.name: x for x in self.columnOptions}

    def __getitem__(self, args):
        if isinstance(args, slice):
            return self.columnOptions[args]
        elif isinstance(args, int):
            return self.columnOptions[args]
        elif isinstance(args, str):
            return self.as_dict[args]
        else:
            raise NotImplementedError(f"{args} is not implemented!")


@dataclass
class StackHandler:
    input_file: str

    stack: ColumnStack = None

    # BINNED_SUFFIX = '_binned'
    # OTHER_BIN_NAME = 'other'
    # NULL_BIN_NAME = 'NA'

    def __post_init__(self):
        self.namespaced = False
        """Reads a dataframe and initializes a stack with the column names"""
        self.df = pd.read_csv(self.input_file, sep=",", header=0)

        logger.info(f"  -> Ingested CSV file, columns: {list(self.df.columns)}")
        self.stack = ColumnStack(list(self.df.columns))

    def apply_namespace(self):
        if not self.stack.nameSpaced:
            for new_col, old_col in self.stack.nameSpace.items():
                self.df[new_col] = self.df[old_col]
            self.stack.nameSpaced = True

    def dump(self, **kwargs) -> dict:
        """Returns a dictionary with the stack configuration"""
        return self.stack.Schema().dump(self.stack, **kwargs)

    def load(self, data: dict, **kwargs):
        """Ingests a stack configuration and runs all instructions in the config"""
        self.stack = self.stack.Schema().load(data, **kwargs)
        self.render()

    def render(self):
        """Runs the chain of functions for the executing configuration instructions"""

        self.apply_namespace()
        self.apply_parse()
        self.apply_dtypes(on_created_cols=False)
        self.create_cols()
        self.apply_dtypes(on_created_cols=True)
        self.apply_filters()
        self.apply_bins()

    def append_column(self, data: dict, **kwargs):
        schema = self.dump()
        # merge dictionaries
        schema["columnOptions"] = schema["columnOptions"] + data["columnOptions"]
        self.stack = self.stack.Schema().load(schema, **kwargs)
        self.render()

    def apply_parse(self):
        for col in self.stack.get_unparsed():
            self.df[col.name] = self.df[col.name].apply(DTYPE_MAP["str"])
            for parse_func, parse_kwargs in zip(col.parse_funcs, col.parse_kwargs):
                logger.info(f"  -> Parsing data for: `{col.name}` with `{parse_func}`")
                agg_func = getattr(FunctionsCollection, parse_func)
                self.df[col.name] = agg_func(self.df[col.name], **parse_kwargs)
                col.parsed = True

    def apply_dtypes(self, on_created_cols=False):
        """
        Applies the data type as specified in the config, uses DTYPE_MAP for apply functions.
        """
        if on_created_cols:
            cols = [
                col
                for col in self.stack.get_aggr_objs()
                if col.dtype and not col.dtype_normalized
            ]
        else:
            cols = [
                col
                for col in self.stack.get_non_aggr_objs()
                if col.dtype and not col.dtype_normalized
            ]

        for col in cols:
            logger.info(f"  -> Converting data type for: `{col.name}` as `{col.dtype}`")
            col.dtype_normalized = True
            self.df[col.name] = self.df[col.name].apply(DTYPE_MAP[col.dtype])

    def create_cols(self):
        """
        Creates any new columns not present in the dataframe.
        Column key `create_func` is mandatory to specify which function is used to create the column
        `create_args` and `create_kwargs` are used to pass args and kwargs to the function.
        """
        for col in self.stack.get_aggr_objs():
            logger.info(f"  -> Creating new column: `{col.name}` as `{col.dtype}`")
            agg_func = getattr(FunctionsCollection, col.create_func)
            agg_args = [self.df[x] for x in col.create_args]
            self.df[col.name] = agg_func(*agg_args, **col.create_kwargs)
            col.dtype_normalized = True
            col.created = True

    # applies and also returns the result? ?
    def apply_bins(self):
        """
            Runs binning, and creates new columns with the binned results.
            """
        # bin_index = []
        for col in self.stack.get_unbinned_objs():
            self.df[col.name] = Binner().apply_bins(
                series=self.df[col.name],
                bin_options=col.bin_include,
                dtype=col.dtype,
                bins=col.bins,
            )
            col.binned = True

    def apply_filters(self):
        """
        Applies filters on the dataframe, as specified per column.
        Filters dictionary may have more that one filter specified.
        Python native keys may be used for applied filtering:
        __lt__ = Lower than
        __gt__ = Greater than
        __eq__ = Equals
        __ne__ = Not equals
        The filter value is a normalized dtype as specified by the column config.
        """

        def _apply_filter(value):
            _func = getattr(self.df[col.name], func_name)
            logger.info(f"  ---> Filter `{_func.__name__}`: `{value}`")
            _value = DTYPE_MAP[col.dtype](value)
            self.df = self.df[_func(_value)]

        for col in self.stack.columnOptions:
            if col.filters and not col.filtered:
                logger.info(f"  -> Filters applied on column: `{col.name}`")
                for func_name, value in col.filters.items():
                    _func = getattr(self.df[col.name], func_name)
                    logger.info(f"  ---> Filter `{_func.__name__}`: `{value}`")
                    _value = TypeInfo.DTYPE_MAP[col.dtype](value)
                    self.df = self.df[_func(_value)]
                col.filtered = True
