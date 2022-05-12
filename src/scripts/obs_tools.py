from utils import *

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)
OBS_TABLE = myPath+"/../../files/output_tables/OBSERVATION_FACT.csv"

def fill_nulls():
    """
    Fill the mandatory keys in the observation table with default values.
    Typically,

    Replace the None values in "ENCOUNTER_NUM" by -1,
    Replace the None values in "START_DATE" by the default date specified in the config file.
    """
    df = pd.read_csv(OBS_TABLE)
    df = df.fillna({"ENCOUNTER_NUM":"-1", "START_DATE":DEFAULT_DATE, "PROVIDER_ID":"@"})
    df.fillna("").to_csv(OBS_TABLE, index=False)

def reindex():
    """
    Replace encounter numbers and patient numbers with integer ones.
    In a proper way, the reindexing should yield only distinct encounter numbers, including for originally-NaN values.
    For now, it keeps the NaNs in place. They are replaced by a -1 in the fill_nulls() function.
    """
    def new_id(row, col):
        tmp = lookup[lookup[col] == key].index.values[0]
        return tmp if tmp>0 else -1

    df = pd.read_csv(OBS_TABLE)
    encs = pd.Series(df["ENCOUNTER_NUM"].dropna().unique())
    pats = pd.Series(df["PATIENT_NUM"].dropna().unique())
    df = df.fillna({"ENCOUNTER_NUM":""})
    lookup = pd.DataFrame({"ENCOUNTER_NUM":encs, "PATIENT_NUM":pats})
    padding = pd.DataFrame([[""]*len(lookup.columns)], columns=lookup.columns)
    lookup = padding.append(lookup, ignore_index=True)

    # todo: swap using the lookup
    for col in ["ENCOUNTER_NUM", "PATIENT_NUM"]:
        print("Reindexing ", col)
        gpd = df.groupby(col)
        for key, subdf in gpd.groups.items():
            df.loc[subdf, col] = new_id(key, col)

    #df["ENCOUNTER_NUM"]=df.apply(lambda row: new_id(row, "ENCOUNTER_NUM"), axis=1)
    #print("Reindexing patient numbers")
    #df["PATIENT_NUM"]=df.apply(lambda row: new_id(row, "PATIENT_NUM"), axis=1)
    df["TEXT_SEARCH_INDEX"] = df.index.values+1
    df.to_csv(OBS_TABLE, index=False)
    return lookup

def check_basecodes():
    df = pd.read_csv(OBS_TABLE)
    conc = pd.Series(df["CONCEPT_CD"].unique())
    mod = pd.Series(df["MODIFIER_CD"].unique())
    mod = mod[~mod.isin(["@"])]
    pdb.set_trace()
    mod_dim = pd.read_csv(OUTPUT_TABLES+"MODIFIER_DIMENSION.csv")["MODIFIER_CD"]
    conc_dim = pd.read_csv(OUTPUT_TABLES+"CONCEPT_DIMENSION.csv")["CONCEPT_CD"]
    assert all(conc.isin(conc_dim)) and all (mod.isin(mod_dim))