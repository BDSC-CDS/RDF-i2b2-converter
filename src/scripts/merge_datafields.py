import os
import sys
import pandas as pd
import pdb
import json

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

METADATA_LOC = myPath+"/../../files/output_tables/METADATA.csv"
DATA_LOC = myPath+"/../../files/output_tables/OBSERVATION_FACT.csv"

with open('migration_logs.json') as json_file:
    migrations = json.load(json_file)

# TODO also keep log of T vs N.... since some elements written in the ontology as strings are in fact floats

def transfer_values():
    df = pd.read_csv(DATA_LOC)
    to_move = df.loc[df["C_BASECODE"].isin(migrations.keys())]

    dests = to_move.apply(lambda row: df.loc[df["C_BASECODE"].isin(migrations[row["C_BASECODE"]]) & df["INSTANCE_NUM"]==row["INSTANCE_NUM"] & df["PATIENT_NUM"]==row["PATIENT_NUM"]])
    pdb.set_trace()



