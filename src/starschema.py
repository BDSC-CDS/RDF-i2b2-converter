import pandas as pd
from typing import List

"""
A set of functions creating the i2b2 CSV tables for the star schema and and ONT cell.
"""


def gen_concept_modifier_dim(
    output_tables_loc, metadata_file, columns, debug_status: bool
):
    """
    Build two tables: i2b2's CONCEPT_DIMENSION and MODIFIER_DIMENSION that stores the concepts and modifiers codes as
    well as the full paths used in the ontology.
    These informations are needed to join CRC tables to ontology tables.
    """
    df = pd.read_csv(metadata_file)
    concept_df = pd.DataFrame(columns=columns["CONCEPT_DIMENSION"])
    modifier_df = pd.DataFrame(columns=columns["MODIFIER_DIMENSION"])

    concept_df[["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR"]] = df.loc[
        df["C_TABLENAME"] == "CONCEPT_DIMENSION", ["C_FULLNAME", "C_BASECODE", "C_NAME"]
    ]
    modifier_df[["MODIFIER_PATH", "MODIFIER_CD", "NAME_CHAR"]] = df.loc[
        df["C_TABLENAME"] == "MODIFIER_DIMENSION",
        ["C_FULLNAME", "C_BASECODE", "C_NAME"],
    ]
    suffix = "_VERBOSE" if debug_status else ""
    concept_df.fillna("").to_csv(
        output_tables_loc + "CONCEPT_DIMENSION" + suffix + ".csv", index=False
    )
    modifier_df.fillna("").to_csv(
        output_tables_loc + "MODIFIER_DIMENSION" + suffix + ".csv", index=False
    )


def init_crc_table(table_path: str, columns: List[str]):
    """
    Create a CSV table with the specified columns.
    """
    patdf = pd.DataFrame(columns=columns)
    patdf.to_csv(table_path, index=False)


def gen_provider_dim(
    output_tables_loc: str, columns: List[str], providers_sparqlres: List
):
    """
    Create the PROVIDER_DIMENSION CSV table out of the provider resource discovery.
    """
    prov_df = pd.DataFrame(columns=columns)
    kdic = {"PROVIDER_ID": [], "PROVIDER_PATH": []}
    for el in providers_sparqlres:
        kdic["PROVIDER_ID"].append(el[1].toPython())
        kdic["PROVIDER_PATH"].append(el[0].toPython())
    pdf = pd.DataFrame.from_dict(kdic)
    prov_df = pd.concat([prov_df, pdf], axis=0)
    prov_df.to_csv(output_tables_loc + "PROVIDER_DIMENSION.csv", index=False)


def init_star_schema(
    output_tables_loc: str, tables: List[str], columns: dict, providers: List
):
    """
    Create the observation-based star schema tables.
    """
    for table in tables:
        init_crc_table(table_path=output_tables_loc + table, columns=columns[table])
    gen_provider_dim(
        output_tables_loc=output_tables_loc,
        columns=columns["PROVIDER_DIMENSION"],
        providers_sparqlres=providers,
    )


def gen_table_access(output_tables_loc, metadata_file, columns):
    """
    Create and fillup the TABLE_ACCESS CSV table.
    """
    df = pd.read_csv(metadata_file)
    table_access = pd.DataFrame(columns=columns)
    inter = table_access.columns.intersection(df.columns)
    table_access[inter] = df.loc[
        (df["C_HLEVEL"] == 0) & (df["C_FACTTABLECOLUMN"] == "CONCEPT_CD"), inter
    ]
    table_access["C_TABLE_CD"] = "sphn"
    table_access["C_TABLE_NAME"] = "sphn"
    table_access["C_DIMTABLENAME"] = "CONCEPT_DIMENSION"
    table_access["C_PROTECTED_ACCESS"] = "N"

    table_access.fillna("").to_csv(output_tables_loc + "TABLE_ACCESS.csv", index=False)
