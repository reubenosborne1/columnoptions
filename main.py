import pandas as pd 
import column_options
import os
import json

# load config file
with open('config.json', "r") as config_file:
	payload = json.load(config_file)

print(payload)

# set directories and load input
BASE_DIR = os.path.join(os.path.dirname(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "inputs")
INPUT_FILE = os.path.join(INPUT_DIR, "ADT.csv")

#load input and config with column options
sh =column_options.StackHandler(INPUT_FILE)
sh.load(payload)
print(sh.df)

#write to csv

OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "output.csv")
data = sh.df
data.to_csv(OUTPUT_FILE)



