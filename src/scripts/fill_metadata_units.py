import os
import sys, pdb
import pandas as pd


def insert_units(metadata_file, units_file):
    df = pd.read_csv(metadata_file)
    DISCR_REGEX = "Integer|Float|PosFloat"
    UNIT_XML_TAG = "<NormalUnits>"

    try:
        lookup = pd.read_csv(units_file, sep=";")
    except:
        print("No unit lookup file found, skipping")
        return
    lookup = dict(zip(lookup["Code"], lookup["Unit"]))

    # Select only rows using metadataxml
    num_df = df.loc[
        df["C_METADATAXML"].fillna("").str.contains(DISCR_REGEX, regex=True)
    ]
    # Extract an ID key matching the keys of the lookup table, save them to the subDF we just defined in a new column
    num_df = num_df.assign(unitkey=num_df["C_FULLNAME"].str.extract(r".*\\([^\\]+)\\"))
    # Now also store the units (stored as values in the lookup table) associated to each key, in a new column
    num_df = num_df.assign(
        units=num_df.apply(
            lambda row: lookup[row.unitkey]
            if row.unitkey in lookup.keys()
            else "(unit not found)",
            axis=1,
        )
    )
    # Now merge the original XML content with the content of the newly created column
    metadataxml = num_df.apply(
        lambda row: row["C_METADATAXML"].replace(
            UNIT_XML_TAG, UNIT_XML_TAG + row.units
        ),
        axis=1,
    ).to_frame("C_METADATAXML")
    df.update(metadataxml)
    df.to_csv(metadata_file, index=False)
