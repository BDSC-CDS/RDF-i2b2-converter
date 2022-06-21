import os
import sys
import pandas as pd
import pdb
import json

COLUMNS_TO_REPLACE = ["VALTYPE_CD", "TVAL_CHAR", "NVAL_NUM", "VALUEFLAG_CD", "QUANTITY_NUM", "UNITS_CD"]

myPath = os.path.dirname(os.path.abspath(__file__))+"/"
sys.path.insert(0, myPath)

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
        filling = get_shot(curgrp, dests, row)
        return filling

    def get_dests(row, curgrp):
        dest = curgrp.loc[curgrp["MODIFIER_CD"].isin(destinations)
            |((curgrp["CONCEPT_CD"].isin(destinations))
                &(curgrp["MODIFIER_CD"]=="@"))
            ]
        if dest.empty:
            print("Nothing found for ", el)
        return dest.index
    
    def get_shot(grp, dest_idx, row):
        tmp = pd.DataFrame(columns=COLUMNS_TO_REPLACE)
        for idx in dest_idx:
            # Those indices we are looping over refer to the "tbmigrated_rows" dataslice
            tmp = pd.concat([tmp, row.loc[COLUMNS_TO_REPLACE].rename(idx).to_frame().T], axis=0)
        return tmp


    with open(myPath+'migrations_logs.json') as json_file:
        migrations = json.load(json_file)
    df = pd.read_csv(OBS_TABLE)
    df.columns = map(str.upper, df.columns)
    final_tab=[]
    gps=df.groupby(["PATIENT_NUM", "CONCEPT_CD", "INSTANCE_NUM"])
    for el, destinations in migrations.items():
        # Get the migration code we are treating
        tbmigrated_rows = df.loc[df["MODIFIER_CD"]==el]
        print("migrating value fields for", el, ",", tbmigrated_rows.shape, "lines affected")
        res =tbmigrated_rows.apply(lambda row: transfer(row), axis=1)
        coll = pd.concat(res.values) if not res.empty else res
        # Inject the corrected subrow where it belongs in the matrix 
        # Merge the updated lines into the original dataframe... oof
        df = df.loc[df.index.difference(tbmigrated_rows.index)]
        df.update(coll) 
    df.to_csv(OBS_TABLE, index=False)


    
