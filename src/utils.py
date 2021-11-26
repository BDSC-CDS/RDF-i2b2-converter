import pandas as pd
import pdb
import json, os, datetime
from configs import *

""""
This file figures file and format utility functions.
It initializes global variables by reading the "ontology_config" file.
"""

def sanitize(db, col_name):
    for el in db:
        for col in col_name:
            el[col] = el[col].replace("'", "")
            el[col] = el[col].replace('"', "")
            el[col] = el[col].replace(" ", "_")
    return db


def format_date(rdfdate, generalize=USE_DUMMY_DATES):
    """
    Format an RDF xsd:date resource onto a "timestamp without time zone" string, readable in postgres.
    If the kyarg "generalize" is set, replace the date by Jan 1st
    """
    mydate = rdfdate.toPython()
    if generalize:
        mydate = datetime.date(mydate.year, 1, 1)
    elif type(mydate) != datetime.date:
        mydate = mydate.date()
    cpy = mydate.__str__()
    return cpy + " 00:00:00"


def add_spaces(oname):
    """
    Inserts spaces between words delimited by capital letters
    """
    fname = ""
    for i in range(len(oname) - 1):
        if (
            i > 0
            and oname[i].isupper()
            and oname[i - 1] != " "
            and (oname[i + 1].islower() or oname[i - 1].islower())
        ):
            fname = fname + " " + oname[i]
        else:
            fname = fname + oname[i]
    return fname + oname[-1]

def wipe_directory(dir_path, names=[]):
    """
    Delete files in the given directory.
    """
    if names == []:
        names = os.listdir(dir_path)
    for k in names:
        os.remove(dir_path+k)
        print("Removed file: ", dir_path+k)


def db_to_csv(db, filename, init=False, columns=[]):
    """
    Simple tool writing a list of dictionaries with matching keys to a csv database-ready file.
    Argument is only the target filename, will be written in the output_tables directory.
    """
    df = pd.DataFrame(db, columns=columns) if columns!=[] else pd.DataFrame(db)
    mode = "w" if init else "a"
    header = init
    df.to_csv(path_or_buf=filename, mode=mode, header=header, index=False)


def from_csv(filename):
    """
    Read the csv database and outputs a list of dicts.
    Argument is the full relative path.
    """
    db = pd.read_csv(filename)
    return db.to_dict("records")


def generate_xml(metadata_dict):
    """
    Generate the metadata_xml panel for a specific entry. Uses the XML_PATTERN and UNITS_DIC global variables.
    enum types are not used since we have explicit valuesets as modifier leaves.
    Default type is string but PosFloat, Integer, PosInteger, Float are accepted
    """
    keyz = metadata_dict.keys()
    shortname =metadata_dict["TestName"]if "TestName" in keyz else ""
    shortname =metadata_dict["TestName"]if "TestName" in keyz else ""
    simple_type=metadata_dict["DataType"] if "DataType" in keyz else ""
    valueset=metadata_dict["EnumValues"] if "EnumValues" in keyz else ""
    test_id=metadata_dict["TestID"] if "TestID" in keyz else ""
    units=metadata_dict["ValueMetadata"] if "Units" in keyz else ""
    if simple_type is None:
        return
    res = XML_PATTERN
    res = res.replace("<TestName></TestName>", "<TestName>" + shortname + "</TestName>")
    res = res.replace(
        "<DataType></DataType>", "<DataType>" + simple_type + "</DataType>"
    )
    res = res.replace("<TestID></TestID>", "<TestID>" + test_id + "</TestID>")
    if simple_type == "Enum" and len(valueset) > 0:
        enumstr = "".join(
            ['<Val description="">' + elem + "</Val>" for elem in valueset]
        )
        res = res.replace(
            "<EnumValues></EnumValues>", "<EnumValues>" + enumstr + "</EnumValues>"
        )
    res = res.replace(
        "</ValueMetadata>", units + "</ValueMetadata>"
    )
    return res

def remove_duplicates(dics):
    """
    Given an unordered list of dictionaries, return a shortened list without duplicates.
    d1 is a duplicate of d2 if d1 == d2.
    """
    return [dict(t) for t in {tuple(d.items()) for d in dics}]
