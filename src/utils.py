import pandas as pd
import pdb
import json, os, datetime

""""
This file figures file and format utility functions.
It initializes global variables by reading the "ontology_config" file.
"""

# This macro allows to write a modifier name as its default value as well
DIRECT_VALUE = ["SomaticVariantFound"]
LOGS_FOLDER = "files/logs/"
MAX_BASECODE_LEN = 49

with open("files/ontology_config") as ff:
    category = ff.readline()
    while category:
        if "ONTOLOGY_GRAPH_LOCATION" in category:
            ONTOLOGY_GRAPH_LOCATION = json.loads(ff.readline())
        elif "ONTOLOGY_NAME" in category:
            ONTOLOGY_NAME = json.loads(ff.readline())
        elif "ABSTRACT" in category:
            ABSTRACT_CLASSES = json.loads(ff.readline())
        elif "EXTERNAL_LOCATION" in category:
            EXTERNAL_LOCATION = json.loads(ff.readline())
        elif "BLACKLIST" in category:
            CONCEPT_BLACKLIST_TOSORT = json.loads(ff.readline())
        elif "NARROW_ONTOLOGY" in category:
            NARROW_ONTOLOGY = json.loads(ff.readline())
        elif "OBSERVATION_INFO" in category:
            OBSERVATION_INFO = json.loads(ff.readline())
        elif "OUTPUT_TABLES" in category:
            OUTPUT_TABLES = json.loads(ff.readline())
        elif "EXTERNAL_TERMS" in category:
            EXT_DICTS = json.loads(ff.readline())
        elif "UNIT_DESCRIPTOR" in category:
            UNITS_DESCR = json.loads(ff.readline())
        elif "LANGUAGE" in category:
            PREF_LANGUAGE = json.loads(ff.readline())
        elif "START_SEARCH_INDEX" in category:
            START_SEARCH_INDEX = json.loads(ff.readline())
        elif "DEACTIVATE_VALUESET " in category:
            DEACTIVATE_VALUESET_TOSORT = json.loads(ff.readline())
        elif "DEFAULT_DATE" in category:
            DEFAULT_DATE = json.loads(ff.readline())
        elif "DATE_DESCRIPTOR" in category:
            DATE_DESCR = json.loads(ff.readline())
        elif "USE_DUMMY_DATES" in category:
            USE_DUMMY_DATES = bool(json.loads(ff.readline()))
        elif "RDF_FORMAT" in category:
            RDF_FORMAT = json.loads(ff.readline())
        elif "DATA_GRAPHS_LOCATION" in category:
            DATA_GRAPHS_LOCATION = json.loads(ff.readline())
            if os.path.isdir(DATA_GRAPHS_LOCATION):
                DATA_GRAPHS = [
                    DATA_GRAPHS_LOCATION + el for el in os.listdir(DATA_GRAPHS_LOCATION)
                ]
            else:
                DATA_GRAPHS = [DATA_GRAPHS_LOCATION]

        category = ff.readline()


def saved_lookup(item, relativepath=EXTERNAL_LOCATION):
    """
    States if the file is in the EXTERNAL_LOCATION directory.
    Detects the file if the same words are present in the required item and in the file's name.
    """
    diritems = os.listdir(os.path.join(os.curdir, relativepath))
    clean = item.replace("-", " ")
    clean = clean.replace("_", " ")
    clean = clean.replace(".", " ")
    words = clean.split(" ")
    for elem in diritems:
        clem = elem.replace("-", " ")
        clem = clem.replace(".", " ")
        loc = clem.split(" ")
        if all([word in elem for word in words]) or all([lo in item for lo in loc]):
            return elem
    return False


def write_ontodic(dicname, data, mode="w"):
    with open(EXTERNAL_LOCATION + dicname, mode) as file:
        json.dump(data, file)
    return


def read_ontodic(dicname):
    with open(EXTERNAL_LOCATION + dicname) as file:
        try:
            data = json.load(file)
        except:
            tmp = file.readline()
            tmp.replace("}{", ",")
            data = json.loads(tmp)
        # Next block is a ad-hoc fix for ontologies that come as a simple list of values
        if not type(data) == dict and type(data[0]) != dict:
            tmp = data
            data = {}
            for el in tmp:
                if el is None:
                    continue
                data.update({"\\" + el: False})
    return data


def extend_valuepath(dicname):
    """
    Extends a path list with the elements of an ontology. Uses the bioontology API. dicname is a dictionary name, entrypoint is the root of the subtree to return.
    Looks if a local copy of the dictionary is available.
    """
    attempt = saved_lookup(dicname)
    if attempt is not False:
        return read_ontodic(attempt)
    else:
        return False


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


def db_to_csv(db, filename):
    """
    Simple tool writing a list of dictionaries with matching keys to a csv database-ready file.
    Argument is only the target filename, will be written in the output_tables directory.
    """
    df = pd.DataFrame(db)
    df.to_csv(path_or_buf=filename)


def from_csv(filename):
    """
    Read the csv database and outputs a list of dicts.
    Argument is the full relative path.
    """
    db = pd.read_csv(filename)
    return db.to_dict('records')


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
