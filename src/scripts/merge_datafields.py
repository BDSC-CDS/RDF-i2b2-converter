import os
import sys
import pandas as pd
import pdb
import json

COLUMNS_TO_REPLACE = ["VALTYPE_CD", "TVAL_CHAR", "NVAL_NUM", "VALUEFLAG_CD", "QUANTITY_NUM", "UNITS_CD"]

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)
OBS_TABLE = myPath+"/../../files/output_tables/OBSERVATION_FACT.csv"

def transfer_obs_numerical_values():
    with open(myPath+'/../../files/migrations_logs.json') as json_file:
        migrations = json.load(json_file)
    df = pd.read_csv(OBS_TABLE)
    df.columns = map(str.upper, df.columns)
    final_tab=[]
    for el, destinations in migrations.items():
        # Get the migration code we are treating
        tbmigrated_rows = df.loc[df["MODIFIER_CD"]==el]
        print("migrating value fields for", el, ",", tbmigrated_rows.shape, "lines affected")
        # Not interested in migrating to a row that should be migrated in any case, so take the diff
        co_df = df.loc[df.index.difference(tbmigrated_rows.index)]
        
        # Find the destination rows
        dests = tbmigrated_rows.apply(lambda row: co_df.loc[
            (
                (co_df["MODIFIER_CD"].isin(destinations))
                |((co_df["CONCEPT_CD"].isin(destinations))
                    &(co_df["MODIFIER_CD"]=="@"))
                
            )
            & (
                co_df["INSTANCE_NUM"]==row["INSTANCE_NUM"]
            )
            ].index, axis=1)
        if dests.empty:
            print("Nothing found for ", el)
            continue
        # df.apply returns an indexed Series so the index can still be used for replacement 
        tmp = pd.DataFrame(columns=co_df.columns)
        for idx in dests.index:
            # get the index to migrate TO
            endidx = dests[idx]
            if len(endidx)>1:
                raise Exception("Cannot relocate a numerical value to more than one destination")
            # Those indices we are looping over refer to the "tbmigrated_rows" dataslice
            tmp = pd.concat([tmp, tbmigrated_rows.loc[idx][COLUMNS_TO_REPLACE].rename(endidx[0]).to_frame().T,], axis=0)
            
        # Merge the updated lines into the original dataframe... oof
        co_df.update(tmp)
        final_tab.append(co_df.copy())
    final = pd.concat(final_tab, axis=0)
    final.to_csv(OBS_TABLE, index=False)


    
