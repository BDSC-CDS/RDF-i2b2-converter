import os
import sys
import pandas as pd
import pdb
import json

COLUMNS_TO_REPLACE = ["VALTYPE_CD", "TVAL_CHAR", "NVAL_NUM", "VALUEFLAG_CD", "QUANTITY_NUM", "UNITS_CD"]

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

METADATA_LOC = myPath+"/../../files/output_tables/METADATA.csv"
DATA_LOC = myPath+"/../../files/output_tables/OBSERVATION_FACT.csv"

with open('migration_logs.json') as json_file:
    migrations = json.load(json_file)

def transfer_obs_numerical_values():
    df = pd.read_csv(DATA_LOC)
    for el, destinations in migrations.items():
        # Get the migration code we are treating
        rows = df.loc[df["MODIFIER_CD"]==el]
        # Not interested in migrating to a row that should be migrated in any case, so take the diff
        co_df = df.loc[df.index.difference(rows.index)]
        
        # Find the destination rows
        dests = rows.apply(lambda row: co_df.loc[
            (
                (co_df["MODIFIER_CD"].isin(destinations))
                |((co_df["CONCEPT_CD"].isin(destinations))
                    &(co_df["MODIFIER_CD"=="@"]))
                
            )
            & (
                co_df["INSTANCE_NUM"]==row["INSTANCE_NUM"]
            )
            ], axis=1)
        
        # df.apply returns an indexed Series so the index can still be used for replacement 
        for idx in dests.index:
            dests[idx].update(df.iloc[idx][COLUMNS_TO_REPLACE])
        # Merge the updated lines into the original dataframe... oof
        df.update(pd.concat(dests.tolist()))
    df.to_csv(DATA_LOC, index=False)


    
