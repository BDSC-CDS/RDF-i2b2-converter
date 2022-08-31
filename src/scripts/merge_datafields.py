import os
import sys
import pandas as pd
import pdb
import json

COLUMNS_TMP = ["CONCEPT_CD", "MODIFIER_CD"]

def transfer_obs_numerical_values(output_tables_loc):

    globals()["OUTPUT_TABLES_LOCATION"] = output_tables_loc
    globals()["OBS_TABLE"] = output_tables_loc + "OBSERVATION_FACT.csv"
    def transfer(row):
        vv = row["INSTANCE_NUM"]
        if vv%50 ==0:
            print("transfering patient", vv)
        # Get the group of interest for this particular observation
        curgrp = gps.get_group(tuple(row.loc[["PATIENT_NUM", "CONCEPT_CD", "INSTANCE_NUM"]]))
        dests = get_dests(row, curgrp)
        # If destinations already exist in the table, inject the row information in them. Else, mutate the row basecode itself.
        filling = get_shot(dests, row)
        return filling

    def get_dests(row, curgrp):
        dest = curgrp.loc[curgrp["MODIFIER_CD"].isin(destinations)
            |((curgrp["CONCEPT_CD"].isin(destinations))
                &(curgrp["MODIFIER_CD"]=="@"))
            ]
        return dest
    
    def get_shot(dests, row):
        dests_idx = dests.index
        if dests_idx.size >1:
            raise Exception("Cannot relocate", row["C_FULLNAME"], "to more than one location")
        elif dests_idx.size==0:
            tmp = pd.Series({"CONCEPT_CD":row["CONCEPT_CD"], "MODIFIER_CD":destinations[0], "INDICES_TO_RM":None})
        else:
            tmp = pd.concat([dests[COLUMNS_TMP].squeeze(), pd.Series({"INDICES_TO_RM":dests_idx.values[0]})])
        #tmp.rename(row.name)
        return tmp

    try:
        with open(OUTPUT_TABLES_LOCATION+'migrations_logs.json') as json_file:
            migrations = json.load(json_file)
    except:
        print("No migration logs found, skipping")
        return
    df = pd.read_csv(OBS_TABLE)
    df.columns = map(str.upper, df.columns)
    final_tab=[]
    gps=df.groupby(["PATIENT_NUM", "CONCEPT_CD", "INSTANCE_NUM"])
    for el, destinations in migrations.items():
        # Get the migration code we are treating
        tbmigrated_rows = df.loc[df["MODIFIER_CD"]==el]
        print("migrating value fields for", el, ",", tbmigrated_rows.shape, "lines affected")
        res =tbmigrated_rows.apply(lambda row: transfer(row), axis=1)
        # Inject the corrected subrow where it belongs in the matrix 
        # Merge the updated lines into the original dataframe... oof
        df=df.drop(res["INDICES_TO_RM"].dropna())
        df.update(res[COLUMNS_TMP]) 
    df.to_csv(OBS_TABLE, index=False)