import os
import sys
import pandas as pd
import pdb

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

METADATA_LOC = myPath+"/../../files/output_tables/METADATA.csv"


migrations = {
    "sphn:hasLabResultValue" : {
        "concept":"sphn:LabResult", 
        "destination": ["swissbioref:hasLabResultLabTestCode/*"], 
        "xmlvaluetype":"Float"
        },
    "sphn:hasDateTime":{
        "concept":"sphn:BirthDate", 
        "destination":["."], 
        "xmlvaluetype":"Integer"
        },
    "sphn:hasBodyWeightValue":{
        "concept":"sphn:BodyWeight", 
        "destination":["."], 
        "xmlvaluetype":"PosFloat"
        }
    }

def extract_parent_id(row):
    # tool to retrieve the short URI of the parent element in the i2b2 ontology file
    if pd.isnull(row["C_PATH"]) :
        return row[["M_APPLIED_PATH"]].str.extract(r'.*\\([^\\]+)\\')[0]["M_APPLIED_PATH"]
    else :
        return row[["C_PATH"]].str.extract(r'.*\\([^\\]+)\\')[0]["C_PATH"]

def resolve_rows(df, destdic):
    """
    The destination field is a list of "/" - separated paths pointing to destination elements. 
    The '*' character is a shortcut for "all children from this point".
    """
    destination = destdic["destination"]
    conc = destdic["concept"]
    conc_idx = df.loc[df["C_FULLNAME"].str.contains(conc)]
    res_idx = pd.Index([])
    for path in destination:
        if path == ".":
            idces = conc_idx
        elif "*" in path:
            npath= path[:path.find("/*")]
            idces = df.loc[df["C_FULLNAME"].str.contains(npath) & df["M_APPLIED_PATH"].str.contains(conc)]
        else:
            idces = df.loc[df["C_FULLNAME"] == ("\\"+path+"\\") & df["M_APPLIED_PATH"].str.contains(conc)]
        res_idx = res_idx.union(idces.index)

    return res_idx

df = pd.read_csv(METADATA_LOC)
# get the shortened URI that trails the full path
dfk=df.assign(key=df["C_FULLNAME"].str.extract(r'.*\\([^\\]+)\\'))

# get the positions of the lines to be deleted and digested into other lines
to_digest = dfk.loc[dfk["C_METADATAXML"].notnull() & dfk["key"].isin(migrations.keys())]

values = pd.DataFrame(columns=["C_METADATAXML"])

for _,row in to_digest.iterrows():
    destdic = migrations[row["key"]]
    # Check it's the good item related to the good parent
    if destdic["concept"] != extract_parent_id(row):
        pdb.set_trace()
        raise Exception("Concept does not match at ", destdic["concept"])

    # change type if necessary in the xml frame
    if "xmlvaluetype" in destdic.keys():
        xmls = row[["C_METADATAXML"]].str.replace("(?<=<DataType>).*?(?=<\/DataType>)", destdic["xmlvaluetype"], regex=True)
        xml = xmls["C_METADATAXML"]
    else:
        xml = row["C_METADATAXML"]

    # find out which rows should receive this xml 
    destination_indexes= resolve_rows(dfk[["C_FULLNAME", "C_PATH", "M_APPLIED_PATH"]], destdic)
    # For each found index, store it into the temporary table
    df.loc[destination_indexes, "C_METADATAXML"] = xml
df=df.drop(to_digest.index)
df.to_csv(METADATA_LOC)



