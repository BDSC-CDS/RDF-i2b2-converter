import pandas as pd
import json


def extract_parent_id(row):
    """
    Tool to retrieve the short URI of the parent element in the i2b2 ontology file
    """
    if pd.isnull(row["C_PATH"]):
        return row[["M_APPLIED_PATH"]].str.extract(r".*\\([^\\]+)\\")[0][
            "M_APPLIED_PATH"
        ]
    else:
        return row[["C_PATH"]].str.extract(r".*\\([^\\]+)\\")[0]["C_PATH"]


def resolve_rows(df, destdic):
    """
    The destination field is a list of paths pointing to destination elements.
    The '*' character is a shortcut for "all children from this point".
    """
    destination = destdic["destination"]
    conc = destdic["concept"]
    conc_row = df.loc[df["C_FULLNAME"].str.contains(conc)]
    if len(conc_row.index) > 1:
        raise Exception("Several matches for migration destination of destdic")
    res_idx = pd.Index([])
    if conc_row["C_TABLENAME"].values[0] != "CONCEPT_DIMENSION":
        return res_idx
    for path in destination:
        if path == ".":
            idces = conc_row
        elif "*" in path:
            npath = path[: path.find("/*")]
            idces = df.loc[
                df["C_FULLNAME"].str.contains(npath)
                & ~(df["C_FULLNAME"] == ("\\" + npath + "\\"))
                & df["M_APPLIED_PATH"].str.contains(conc)
            ]
        else:
            idces = df.loc[
                (df["C_FULLNAME"] == ("\\" + path + "\\"))
                & (df["M_APPLIED_PATH"].str.contains(conc))
            ]
        res_idx = res_idx.union(idces.index)

    return res_idx


def merge_metadatavaluefields(migrations_config, metadata_file, migrations_logfile):
    """
    Merge ontology items following the configuration file.
    """
    logs = {}
    df = pd.read_csv(metadata_file)
    # get the shortened URI that trails the full path
    dfk = df.assign(key=df["C_FULLNAME"].str.extract(r".*\\([^\\]+)\\"))

    # get the positions of the lines to be deleted and digested into other lines
    to_digest = dfk.loc[
        dfk["C_METADATAXML"].notnull() & dfk["key"].isin(migrations_config.keys())
    ]

    moved = pd.Index([])
    for ix, row in to_digest.iterrows():
        destdic = migrations_config[row["key"]]
        # Check it's the good item related to the good parent
        if destdic["concept"] != extract_parent_id(row):
            print("Concept does not match at ", destdic["concept"], "could not migrate")
            continue
        # find out which rows should receive this xml
        destination_indexes = resolve_rows(
            dfk[
                [
                    "C_FULLNAME",
                    "C_PATH",
                    "M_APPLIED_PATH",
                    "C_TABLENAME",
                    "C_VISUALATTRIBUTES",
                ]
            ],
            destdic,
        )
        if len(destination_indexes) == 0:
            continue

        # change type if necessary in the xml frame
        if "xmlvaluetype" in destdic.keys():
            xmls = row[["C_METADATAXML"]].str.replace(
                "(?<=<DataType>).*?(?=<\/DataType>)",
                destdic["xmlvaluetype"],
                regex=True,
            )
            xml = xmls["C_METADATAXML"]
        else:
            xml = row["C_METADATAXML"]

        logs.update(
            {row["C_BASECODE"]: df.loc[destination_indexes, "C_BASECODE"].tolist()}
        )
        # For each found index, store it into the temporary table
        df.loc[destination_indexes, "C_METADATAXML"] = xml
        new_va = (
            row["C_VISUALATTRIBUTES"]
            if "newvisualattributes" not in destdic.keys()
            else destdic["newvisualattributes"]
        )
        df.loc[destination_indexes, "C_VISUALATTRIBUTES"] = new_va
        moved = moved.union([ix])
    df = df.drop(moved)
    df.to_csv(metadata_file, index=False)
    with open(migrations_logfile, "w") as outfile:
        json.dump(logs, outfile)
