import pandas as pd
import pdb
import json, os, datetime

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


def db_to_csv(db, filename, mode="w"):
    """
    Simple tool writing a list of dictionaries with matching keys to a csv database-ready file.
    Argument is only the target filename, will be written in the output_tables directory.
    """
    df = pd.DataFrame(db)
    df.to_csv(path_or_buf=filename, mode=mode, header=(mode != "a"))


def from_csv(filename):
    """
    Read the csv database and outputs a list of dicts.
    Argument is the full relative path.
    """
    db = pd.read_csv(filename)
    return db.to_dict("records")


def reduce_term(verbose):
    """
    Extract the identifying code of the term if any.
    """
    suffix = verbose.split("\\")[-1] if "\\" in verbose else verbose
    nb_par = suffix.count("(")
    if nb_par == 1 or suffix[0] == "(":
        cutp = suffix.split("(")[1]
        return cutp[: cutp.find(")")]
    elif suffix[-1] == ")":
        cutp = suffix.split("(")[-1]
        cutp = cutp[:-1]
        return cutp
    else:
        return suffix


def remove_duplicates(dics):
    """
    Given an unordered list of dictionaries, return a shortened list without duplicates.
    d1 is a duplicate of d2 if d1 == d2.
    """
    return [dict(t) for t in {tuple(d.items()) for d in dics}]
