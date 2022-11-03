from utils import *

OBS_TABLE = OUTPUT_TABLES_LOCATION +"OBSERVATION_FACT.csv"

def check_basecodes():
    df = pd.read_csv(OBS_TABLE)
    conc = pd.Series(df["CONCEPT_CD"].unique())
    mod = pd.Series(df["MODIFIER_CD"].unique())
    mod = mod[~mod.isin(["@"])]
    try:
        mod_dim = pd.read_csv(OUTPUT_TABLES_LOCATION+"MODIFIER_DIMENSION.csv")["MODIFIER_CD"]
        conc_dim = pd.read_csv(OUTPUT_TABLES_LOCATION+"CONCEPT_DIMENSION.csv")["CONCEPT_CD"]
    except:
        print("Conversion passed but consistency check were not performed due to absence of ontology tables (CONCEPT_DIMENSION and/or MODIFIER_DIMENSION in the folder", OUTPUT_TABLES_LOCATION)
        print("Put them there and call check_basecodes() in a new Python interface")
        raise Exception("Converted, but skipped consistency checked.")
    if not (all(conc.isin(conc_dim)) and all (mod.isin(mod_dim))):
        print("Some concepts or modifiers are not in the ontology. \nPlease take a look at the \"logs_missing_concepts\" and \"logs_missing_modifiers\" logfiles. \\\
            If unreadable, change the \"DEBUG\" variable in the config files to True, and run the \"make debug\" command. Also check the CONCEPT_DIMENSION and MODIFIER_DIMENSION \\\
            in ", OUTPUT_TABLES_LOCATION, 
                "are in debug mode as well (concept_cd and modifier_cd should not be hashes but human-readable.")
        missing_concepts = conc[~conc.isin(conc_dim)]
        missing_modifiers = mod[~mod.isin(mod_dim)]
        tail_modifiers = [k[-20:] for k in missing_modifiers]
        pd.set_option('display.max_colwidth', None)
        missing_concepts.to_csv(OUTPUT_TABLES_LOCATION+"logs_missing_concepts.csv")
        missing_modifiers.to_csv(OUTPUT_TABLES_LOCATION+"logs_missing_modifiers.csv")
    else:
        print("Success! All items are consistent with the ontology.")