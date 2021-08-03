from utils import *
from rdf_base import *

"""
Data loader file.
Mainly walks through the data RDF graph and the ontology graph in order to construct observation facts as i2b2 database lines.
"""
# Maintain a global list of object/class names which have a finite number of possible values, hence that define an extra path level instead of a value field.
# Walking through the instanciated data, one should check if the value should fit in the "value" fields of the table or if it is an element of a valueset.
HAVE_VALUESET = disc_valuesets()
PATIENT_URI = SPHN["L2-subject_pseudo_identifier"]
ENCOUNTER_URI = SPHN["L3-encounter"]
PROJECT_ID = "SPO"


def isfrom_valueset(c_res):
    # Check if the arg predicate references a valueset or if its values will be free text values

    # Edit: might be useless because the object is an instance of NamedIndividual if the concept has a valueset.
    return c_res.value(RDFS.subClassOf) == SPHN.Valueset


def navigate_graph(graphs=DATA_GRAPHS):
    """
    Returns a list of all the instances matching any of the ontology concepts returned by the setup() function.
    The data graphs used are all the turtle files found at the  directory address specified in the config file.
    """
    classes = setup()
    g = unify_graph(graphs)
    imported = {}
    instances = []
    for key in classes.keys():
        # migrate the ontology graph classes to this graph
        imported.update({key: g.resource(classes[key].identifier)})
        resp = g.query(
            """
            SELECT DISTINCT ?i where {
            ?i rdf:type ?c
        }
        """,
            initBindings={"?c": imported[key].identifier},
        )

        for el in resp:
            # Add the resources to the list of instances
            instances.append(g.resource(el[0]))

    return instances


def generate_dbblock(response, initial_instance_counts={}):
    """
    Iterates through the instances of each entry concept, triggers a modifier discovery and finally the db writing.
    Calls fixup functions needed for the database to be compatible with i2b2 and postgres constraints.
    """
    db = []
    oldc = ""
    instance_counts = initial_instance_counts.copy()
    lookup_table = trigger_bootstrap_counts(response)
    for observation in response:
        cname = observation.value(RDF.type).identifier.toPython()
        concept_line = {"name": cname}
        if cname not in instance_counts.keys():
            instance_counts.update({cname: 1})
        else:
            instance_counts[cname] = instance_counts[cname] + 1
        concept_line.update({"instance_num": instance_counts[cname]})
        if cname != oldc:
            print("next concept type:" + concept_line["name"])
        oldc = concept_line["name"]

        concept_line.update(
            {
                "code": reduce_basecode(concept_line["name"]),
                "start_date": "",
                "end_date": "",
            }
        )
        modifier_lines = dig_attributes(observation)
        # The first element of the modifier info list are datatype properties about the concept
        concept_line.update(modifier_lines[0])
        modifier_lines = modifier_lines[1:]
        properties = extract_metainfo(observation, lookup_table)

        db.extend(obsfact_dblines(concept_line, modifier_lines, properties))
    # Start date is a mandatory field for i2b2. Add a dummy entry for each line.
    add_startdate(db)
    # Encounter numbers are mandatory for i2b2. Add -1 when it is unrelevant (ex BirthDatetime)
    # and -2 when the encouter resource is simply missing
    add_missing_encounters(db)
    # Fill the star schema remaining tables : patient_dimension, provider_dimension, visit_dimension
    fill_otherdims(db)
    # Export the pairings created for the patient numbers and encounter numbers (homemade aliases)
    export_mappings(lookup_table)
    return db


def dig_attributes(rsc, code_hashprefix=""):
    """
    Each call to this function results in an additional db line.
    Recursively triggers the attributes extraction of an instance. Returns a list of dicts, each dict embedding the info of a modifier and the first element of the list being information about the concept itself.
    [
                        #dict of parameter information
                        {}

                        #dict of modifier 1
                        {"modifcode":XXX, "modifvalue":XXX, "valtype_cd":XXX, "modifcomment":XXX, "unit": XXX, "start date":XXX, "end date":XXX}

                        #dict of modifier 2
                        {"modifcode":XXX, "modifvalue":XXX, "valtype_cd":XXX, "modifcomment":XXX, "unit": XXX, "start date":XXX, "end date":XXX}

                        #dict of modifier 3, which is a submodifier of modifier 2.
                        {"modifcode":XXX, "modifvalue":XXX, "valtype_cd":XXX, "modifcomment":XXX, "unit": XXX, "start date":XXX, "end date":XXX}

                ]
    Modifiers and L1,L2,L3,date,unit attributes are treate differently because each modifier value requires an additional line in the observation fact table,
    while L1,L2,L3,date,unit attributes are additional information to add in these lines.

        -> Actually, unit and datetime should NOT be duplicated from the concept line since they are specific to a modifier.
        but L1,L2,L3 have no reason to be different in the modifier and concept lines
    """
    g = rsc.graph
    infos = []
    upper = {}
    zipped = [yy for yy in rsc.predicate_objects()]
    predicates = [u[0] for u in zipped]
    objects = [u[1] for u in zipped]
    digattrs = filter_obsfact(predicates, toignore=["L1", "L2", "L3"])
    ######################
    for pred, obj in zip(predicates, objects):
        if not pred in digattrs or any(
            [e in pred.identifier for e in CONCEPT_BLACKLIST]
        ):
            continue

        valueinf = {}
        nval = None
        tval = None
        name = reduce_basecode(code_hashprefix + pred.identifier.toPython())

        predid = rname(pred.identifier, g)
        # Workaround to detect if the predicate is a DatatypeProperty or an ObjectProperty
        if callable(obj.value):
            object_prop = True
            if len(list(obj.predicates())) == 0:
                with open(LOGS_FOLDER + "RDF_deadlink_log", "a") as ff:
                    json.dump(g.namespace_manager.normalizeUri(obj.identifier), ff)
                continue
        else:
            try:
                vtype = rname(obj.datatype.toPython(), g)
            # If xsd types are not specified in the data, we have to manually "typecheck"
            except:
                if type(obj.value) == str:
                    vtype = "string"
                elif int(obj.value) == obj.value:
                    vtype = "integer"
                else:
                    vtype = "double"
            object_prop = False
        if isfrom_valueset(pred):
            if not object_prop:
                # Has a valueset but is a DatatypeProperty
                tmp = {
                    "code": reduce_basecode(
                        code_hashprefix + pred.identifier.toPython(), value=obj.value
                    )
                }
                if any([el in predid for el in DIRECT_VALUE]):
                    tmp.update({"vtype": "T", "nval": "", "tval": obj.value})
                infos.append(tmp)
            else:
                # Has a valueset, is a Code object
                if obj.value(RDF.type) != g.resource(SPHN.Code):
                    raise Exception(
                        "Unpackable object with a valueset but is not a Code"
                    )
                tmp = {}
                """ Lookup into the Code object. If the coding system is known and expandable, it means the value should be appended
                to the path. Else, the value is free text and written in tval without path extension.
                """
                res = open_codeobj(obj)
                # If the coding system is available, add $codingsystem:$code to the path.
                # If not, only add the coding system, and the code will be written as text value tval
                # Return value of open_codeobj is (True, codingsystem,  code_id) except if codingsystem is empty.
                # In this case, the return value is (True, code_id) (2 elements)
                if res[0] is True:
                    # If there are only 2 elements in res, join inserts nothing (no coding system, no colon)
                    suffix = ":".join(res[1:])
                else:
                    # Coding system not found locally
                    suffix = res[1]
                    tmp.update({"vtype": "T", "nval": "", "tval": res[2]})
                tmp.update(
                    {
                        "code": reduce_basecode(
                            code_hashprefix + pred.identifier.toPython(), value=suffix
                        )
                    }
                )
                infos.append(tmp)
        # Next case : no valueset specified, is a DatatypeProperty (free text/date/number)
        elif not object_prop:
            # Deal with the datetime. Should have start_date, end_date fields if
            #   - "datetime" is present in the modifier name but NOT in the concept name. In this case start_date=end_date=datetime
            #   - if an attribute has end_date/start_date as predicates : extract their value and store them in the parent's dictionary
            #       (since these predicates should not result in an additional modifier line)

            # Deal with Birthdatetime ad-hoc because it's the only case where a date goes into a val field
            if "Datetime" in predid and "value" in predid:
                date = format_date(obj, generalize=True)
                year = obj.toPython().year
                vtype = "N"
                tval = "E"
                nval = year
                upper.update(
                    {
                        "start_date": date,
                        "end_date": date,
                        "vtype": "N",
                        "tval_char": "E",
                        "nval_num": nval,
                    }
                )
            elif "date" in vtype:
                date = format_date(obj)
                if "start_" in predid:
                    upper.update({"start_date": date})
                elif "end_" in predid:
                    upper.update({"end_date": date})
                else:
                    upper.update({"start_date": date, "end_date": date})

            elif "string" in vtype:
                nval = ""
                tval = obj.value
                vtype = "T"

            elif "double" in vtype or "integer" in vtype:
                nval = obj.value
                tval = "E"
                vtype = "N"

            if nval is not None:
                valueinf.update(
                    {"code": name, "vtype": vtype, "nval": nval, "tval": tval}
                )
                infos.append(valueinf)

        # No valueset, is an ObjectProperty
        else:
            vtype = rname(obj.value(RDF.type).identifier, g)
            if vtype in CONCEPT_BLACKLIST:
                continue

            if vtype in OBSERVATION_INFO:
                # Unit to unpack
                obs_values = [
                    e[1] for e in obj.predicate_objects() if e[0].identifier != RDF.type
                ]
                # Check if the object is indeed a wrapper for a single value
                if len(obs_values) == 1:
                    upper.update({vtype: obs_values[0]})
                else:
                    raise Exception(
                        'OBSERVATION_INFO global variable indicates a variable "'
                        + vtype
                        + '" that has several attributes. Cannot unpack'
                    )
            else:
                # If needed, recursive call and unpacking
                further = dig_attributes(obj, name)
                valueinf.update({"code": name})
                valueinf.update(further[0])
                if "Unit" in valueinf.keys():
                    for di in further[1:]:
                        di["Unit"] = valueinf["Unit"]
                infos.extend([valueinf] + further[1:])
    return [upper] + infos


def open_codeobj(obj):
    """
    Opens a sphn:Code instance and extracts it as an i2b2 reduced code.
    We distinguish three cases:
    First, there is no coding system.
    Second, the coding system is found in the terminology files.
    Last, the coding system is not found in the terminology files.
    """
    codingsys = obj.value(SPHN["A104-Code-coding_system_and_version"]).toPython()
    codid = obj.value(SPHN["A102-Code-identifier"]).toPython()
    if codid is None or codid == "":
        codid = obj.value(SPHN["A103-Code-code_name"]).toPython()

    exists = saved_lookup(codingsys)
    if exists:
        if codingsys is None or codingsys == "":
            return True, codid
        else:
            return True, exists, codid
    else:
        return False, codingsys, codid


def extract_metainfo(instance, lookup_table):
    """
    Extracts the information that modifers will inherit from the concept.
    The only exception can be datetime, if the modifier specifies its own (will overwrite).

    For encounter numbers and patient numbers, we use a lookup table that binds every instance to a number between 1 and the number of distinct instances.
    This because i2b2 does not support arbitrarily high (such as coded values) patient numbers and encounter numbers.
    """
    pd = [e for e in instance.predicates()]
    g = instance.graph
    res = {"L1": "", "L2": "", "L3": ""}
    dpi = g.resource(SPHN["L1-data_provider_institute"])
    spi = g.resource(SPHN["L2-subject_pseudo_identifier"])
    enc = g.resource(SPHN["L3-encounter"])
    if dpi in pd:
        dpi_v = instance.value(dpi.identifier).value(
            SPHN["A109-DataProviderInstitute-value"]
        )
        res.update({"L1": dpi_v})
    spi_instance = instance.value(spi.identifier)
    spi_v = str(lookup_table[spi.identifier][rname(spi_instance.identifier, g)])
    res.update({"L2": spi_v})
    if enc in pd:
        # enc_v_id = instance.value(SPHN["L3-encounter"]).value(SPHN["A115-Encounter-type"]).value(SPHN["A115-CareHandlingType-value"])
        enc_v_num = instance.value(SPHN["L3-encounter"])

        enc_instance = instance.value(enc.identifier)
        enc_v_num = str(lookup_table[enc.identifier][rname(enc_instance.identifier, g)])
        res.update({"L3": enc_v_num})
    # We now gather the datetime, except if the concept itself is a date (s.a .BirthDatetime)
    if not "datetime" in rname(instance.value(RDF.type).identifier, g):
        dtime = None
        for pred in pd:
            if "datetime" in rname(pred.identifier, g):
                dtime = instance.value(pred.identifier)
                res.update({"datetime": format_date(dtime)})
    return res


def add_missing_encounters(db):
    for row in db:
        if row["encounter_num"] == "":
            row["encounter_num"] = "-1"
        elif row["encounter_num"] is None:
            row["encounter_num"] = "-2"
        if row["provider_id"] == "":
            row["provider_id"] = "@"


def obsfact_dblines(conceptinfo, modifiersinfo, properties):
    """
    Craft observation_fact lines from a concept instance, the flattened list of its instanciated modifiers and the common info L1,L2,L3
    """
    dblines = []

    vtype = ""
    tval = ""
    nval_num = ""
    units = "@"
    if "vtype" in conceptinfo.keys():
        vtype = conceptinfo["vtype"]
        tval = conceptinfo["tval_char"]
        nval_num = conceptinfo["nval_num"]
    if "Unit" in conceptinfo.keys():
        units = conceptinfo["Unit"]
    source_sys = ""
    conceptline = {
        "encounter_num": properties["L3"],
        "patient_num": properties["L2"],
        "concept_cd": conceptinfo["code"],
        "provider_id": properties["L1"],
        "start_date": conceptinfo["start_date"],
        "modifier_cd": "@",
        "instance_num": conceptinfo["instance_num"],
        "valtype_cd": vtype,
        "tval_char": tval,
        "nval_num": nval_num,
        "valueflag_cd": "",
        "quantity_num": "",
        "units_cd": units,
        "end_date": conceptinfo["end_date"],
        "location_cd": "",
        "observation_blob": "",
        "confidence_num": "",
        "update_date": "",
        "download_date": "",
        "import_date": "",
        "sourcesystem_cd": source_sys,
        "upload_id": "",
    }
    dblines.append(conceptline)
    for mod in modifiersinfo:
        modline = conceptline.copy()
        if "vtype" in mod.keys():
            nval_num = mod["nval"]
            tval_char = mod["tval"]
            vtype = mod["vtype"]
        else:
            nval_num = ""
            tval_char = ""
            vtype = "@"
        if "Unit" in mod.keys():
            modline.update({"units_cd": mod["Unit"]})
        if "start_date" in mod.keys():
            modline.update({"start_date": mod["start_date"]})
        if "end_date" in mod.keys():
            modline.update({"end_date": mod["end_date"]})

        modline.update(
            {
                "modifier_cd": mod["code"],
                "valtype_cd": vtype,
                "tval_char": tval_char,
                "nval_num": nval_num,
            }
        )
        dblines.append(modline)
    return dblines


def add_startdate(db):
    # Add mandatory elements for non-null i2b2 cells. They will not be used in the current implementation.
    for idx, line in enumerate(db):
        if line["start_date"] == "":
            line["start_date"] = (
                str(datetime.date.fromordinal(DEFAULT_DATE)) + " 00:00:00"
            )
        line.update({"text_search_index": idx + START_SEARCH_INDEX})


def fill_otherdims(db):
    """
    Fill the patient dimension, visit dimension, provider dimension from the gathered observation fact.
    (concept and modifier dimension are filled separately since they do not depend on the instances)
    """
    visit_dimension = []
    patient_dimension = []
    provider_dimension = []
    tmpenc = []
    tmppat = []
    tmpprov = []

    for el in db:
        if el["modifier_cd"] != "@":
            # We skip modifiers lines as they do not bring new encounter, patient or provider IDs
            continue
        if not el["encounter_num"][0].isdigit():
            encnum = el["encounter_num"][1:]
        else:
            encnum = el["encounter_num"]
        if int(encnum) > 0 and el["encounter_num"] not in tmpenc:
            tmpenc.append(el["encounter_num"])
            visit_dimension.append(
                {
                    "encounter_num": el["encounter_num"],
                    "patient_num": el["patient_num"],
                    "active_status_cd": "",
                    "start_date": "",
                    "end_date": "",
                    "inout_cd": "",
                    "location_cd": "",
                    "location_path": "",
                    "length_of_stay": "",
                    "visit_blob": "",
                    "update_date": "",
                    "download_date": "",
                    "import_date": "",
                    "sourcesystem_cd": "",
                    "upload_id": "",
                }
            )

        if el["patient_num"] not in tmppat:
            tmppat.append(el["patient_num"])
            patient_dimension.append(
                {
                    "patient_num": el["patient_num"],
                    "vital_status_cd": "",
                    "birth_date": "",
                    "death_date": "",
                    "sex_cd": "",
                    "age_in_years_num": "",
                    "language_cd": "",
                    "race_cd": "",
                    "marital_status_cd": "",
                    "religion_cd": "",
                    "zip_cd": "",
                    "statecityzip_path": "",
                    "income_cd": "",
                    "patient_blob": "",
                    "update_date": "",
                    "download_date": "",
                    "import_date": "",
                    "sourcesystem_cd": "",
                    "upload_id": "",
                    "enc_dummy_flag_cd": "",
                }
            )
        if el["provider_id"] != "@" and el["provider_id"] not in tmpprov:
            tmpprov.append(el["provider_id"])
            try:
                provider_dimension.append(
                    {
                        "provider_id": el["provider_id"],
                        "provider_path": "/" + el["provider_id"],
                        "name_char": "",
                        "provider_blob": "",
                        "update_date": "",
                        "download_date": "",
                        "import_date": "",
                        "sourcesystem_cd": "",
                        "upload_id": "",
                    }
                )
            except:
                pdb.set_trace()
    db_csv(visit_dimension, "VISIT_DIMENSION")
    db_csv(patient_dimension, "PATIENT_DIMENSION")
    db_csv(provider_dimension, "PROVIDER_DIMENSION")


def export_mappings(lookup):
    default_patient = {
        "patient_ide": "",
        "patient_ide_source": "",
        "patient_num": "",
        "patient_ide_status": "",
        "project_id": PROJECT_ID,
        "upload_date": "",
        "update_date": "",
        "download_date": "",
        "import_date": "",
        "sourcesystem_cd": "",
        "upload_id": "",
    }
    default_encounter = {
        "encounter_ide": "",
        "encounter_ide_source": "",
        "project_id": PROJECT_ID,
        "encounter_num": "",
        "patient_ide": "-1",
        "patient_ide_source": "-1",
        "encounter_ide_status": "",
        "upload_date": "",
        "update_date": "",
        "download_date": "",
        "import_date": "",
        "sourcesystem_cd": "",
        "upload_id": "",
    }
    patient_mapping = []
    encounter_mapping = []
    for key in lookup[PATIENT_URI].keys():
        tmp = default_patient.copy()
        tmp.update(
            {
                "patient_ide": key,
                "patient_ide_source": key,
                "patient_num": lookup[PATIENT_URI][key],
            }
        )
        patient_mapping.append(tmp)

    for key in lookup[ENCOUNTER_URI]:
        tmp = default_encounter.copy()
        tmp.update(
            {
                "encounter_ide": key,
                "encounter_ide_source": key,
                "encounter_num": lookup[ENCOUNTER_URI][key],
            }
        )
        encounter_mapping.append(tmp)

    db_csv(patient_mapping, "PATIENT_MAPPING")
    db_csv(encounter_mapping, "ENCOUNTER_MAPPING")


def check_starschema(db):
    c_dicts = from_csv(OUTPUT_TABLES + "CONCEPT_DIMENSION.csv")
    m_dicts = from_csv(OUTPUT_TABLES + "MODIFIER_DIMENSION.csv")
    c_codes = [el["concept_cd"] for el in c_dicts]
    m_codes = [el["modifier_cd"] for el in m_dicts]
    print("Checking the consistency between data and modifier/concept dimension")
    notfound = []
    for dic in db:
        if dic["modifier_cd"] == "@":
            # this is a concept obs fact line
            if dic["concept_cd"] not in c_codes:
                notfound.append(dic["concept_cd"])
        else:
            if dic["modifier_cd"] not in m_codes:
                notfound.append(dic["modifier_cd"])
    notfound = list(set(notfound))
    if len(notfound) > 0:
        with open(LOGS_FOLDER + "i2b2_notfound_log", "w") as ff:
            json.dump(notfound, ff)
    try:
        with open(LOGS_FOLDER + "RDF_deadlink_log", "r") as ff:
            text = ff.read()
            splitt = text[1:-1].split('""')
            splitt = list(set(splitt))
        with open(LOGS_FOLDER + "RDF_deadlink_log", "w") as ff:
            json.dump(splitt, ff)
    except:
        pass

    return True


def trigger_bootstrap_counts(response):
    g = response[0].graph
    spi = PATIENT_URI
    enc = ENCOUNTER_URI
    return bootstrap_counts(g, spi, enc)
