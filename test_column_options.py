import pytest
import os
import pandas as pd
import column_options
​
​
payload = {
    "columnOptions": [
        {
            "name": "pat_id",
            "dtype": "str",
        },
        {
            "name": "country",
            "dtype": "cat",
            # "filters": {"__eq__": "gb"}
        },
        {
            "name": "location_code",
            "dtype": "int"
        },
        {
            "name": "dob",
            "dtype": "date",
        },
        {
            "name": "lot1",
            "dtype": "date",
        },
        {
            "name": "aspirin",
            "dtype": "bool",
        },
        {
            "name": "age_at_lot1",
            "dtype": "float",
            "bins": {
                "<20": "..20",
                "20s": "20..30",
                "30s": "30..40",
                "40s": "40..50",
                "50+": "50..",
            },

        },
    ]
}
​
​
@pytest.fixture
def sh():
    BASE_DIR = os.path.join(os.path.dirname(__file__))
    INPUT_DIR = os.path.join(BASE_DIR, "inputs")
    INPUT_FILE = os.path.join(INPUT_DIR, "test-col-ops.csv")
    return column_options.StackHandler(INPUT_FILE)
​
​
def test_ingest_csv(sh):
    assert type(sh.df) == pd.DataFrame
    assert list(sh.df.columns) == sh.stack.get_full_list()
​
​
def test_load_config(sh):
    sh.load(payload)
    # Checks a new column was created
    assert "age_at_lot1" in sh.stack.get_full_list()
    assert "age_at_lot1" in sh.df.columns
    # Checks the new column was binned
    assert "age_at_lot1_binned" in sh.stack.get_full_list()
    assert "age_at_lot1_binned" in sh.df.columns
    assert "20s" in list(sh.df["age_at_lot1_binned"])
​
​
def test_apply_filters(sh):
    sh.load(payload)
    assert "fr" in list(sh.df["country"])
    sh.stack["country"].filters = {"__eq__": "gb"}
    sh.render()
    assert "fr" not in list(sh.df["country"])
​
​
def test_ammend_dump(sh):
    sh.load(payload)
    assert sh.df["location_code"][0] == 83476
    sh.stack["location_code"].set_dtype("str")
    sh.render()
    assert sh.df["location_code"][0] == "83476"
    d = sh.dump()
    assert type(d) == dict
    assert d["columnOptions"][2]["dtype"] == "str"
​
​
def test_getitem(sh):
    sh.load(payload)
    assert sh.stack[0] == sh.stack["pat_id"]
    assert len(sh.stack[:2]) == 2
​
​
def test_set_friendly(sh):
    sh.load(payload)
<<<<<<< HEAD
    c = sh.stack["country"]
    c.set_friendly_name("Country of Birth")
    assert c.friendly_name == "Country of Birth"
=======
    c = sh.stack['country']
    c.set_friendly_name('Country of Birth')
    assert c.friendly_name == 'Country of Birth'
>>>>>>> 0212106936aa14d2f248c58f13b2b13fdeec35ba
