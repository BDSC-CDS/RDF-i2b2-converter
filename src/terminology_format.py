from utils import *

"""
This file features ad-hoc tools to construct some common medical ontologies based on existing files.
Code written here is not called by the loader, only useful to the developer.

"""

SOURCE_DIR = "../misc/"


def suffix_to_prefix(filename):
    """
    Edits a terminology file so all the codes end up as prefix of their description. (Enhance readability)
    """

    with open(filename) as ff:
        elements = json.load(ff)
    res = {}
    for elem, val in elements.items():
        if "Codes obsol" in elem:
            continue
        swapped = False
        septed = elem.split("\\")
        for piece_idx in range(len(septed)):
            piece1 = septed[piece_idx]
            # If there is no code identifier in the term, check this is normal
            if len(piece1) > 0:
                # Take the first word. If it's only caps but not the only word, it's a hidden code.
                strips = piece1.split(" ")
                if len(strips) > 1 and "(" not in strips[0]:
                    if not any((c for c in strips[0] if c.islower())):
                        septed[piece_idx] = " ".join(
                            ["(" + strips[0] + ")"] + strips[1:]
                        )
                        swapped = True
            piece = septed[piece_idx]
            # If the term ends with its code between parenthesis, move it in front
            if len(piece) > 0 and piece[-1] == ")" and piece[0] != "(":
                try:
                    last_open = len(piece) - (piece[::-1].index("(") + 1)
                except:
                    pdb.set_trace()
                septed[piece_idx] = piece[last_open:].upper() + " " + piece[:last_open]
                swapped = True
        key = "\\".join(septed) if swapped else elem
        if "(" in elem and "(" not in key:
            pdb.set_trace()
        res.update({key: val})

    with open(filename, "w") as fp:
        json.dump(res, fp)


def read_term_i2b2DWH(csvfilename, cutp):
    """
    Retrieve the paths and leaf indicators from an i2b2 ontology db file.
    Argument @cutp is the name of the ontology as it should later appear in the final ontology.

    The output file will add its full name to each node in the path if the input ontology did not.

    Default behaviour is to cut all paths so the root of the ontology is this term/node.
    To deactivate this, change the internal function @cut().
    """

    def sort_levels(items):
        """
        Sort all the paths by depth level.
        """
        buckets = {}
        for elem in items:
            level = elem["path"].count("\\")
            if level in buckets.keys():
                buckets[level].append(elem)
            else:
                buckets[level] = [elem]
        return buckets

    def cut(fullname):
        resp = "\\" + fullname[fullname.index(cutp) :]
        if resp[-1] == "\\":
            resp = resp[:-1]
        return resp

    init = []
    csvfile = open(SOURCE_DIR + csvfilename, "r")
    reader = csv.DictReader(csvfile)
    for row in reader:
        if not cutp in row["C_FULLNAME"]:
            continue
        name = cut(row["C_FULLNAME"])
        haskids = "LA" not in row["C_VISUALATTRIBUTES"]
        init.append({"path": name, "haskids": haskids, "descr": row["C_NAME"]})
    csvfile.close()
    bucketted = sort_levels(init)
    res = complete_tree(bucketted)

    with open(EXTERNAL_LOCATION + cutp, "w") as ff:
        json.dump(res, ff)


def lowercase_choose(filename, prefix="Codes obsol"):
    with open(filename) as ff:
        terms = json.load(ff)
    ret = {}
    for elem in terms.keys():
        if prefix in elem:
            pdb.set_trace()
            tab = elem.split("\\")
            path = tab[:3]
            for lev in range(3, len(tab)):
                path.append(tab[lev].lower())
                path[-1] = path[-1][0].upper() + path[-1][1:]

        else:
            ret.update({elem: terms[elem]})
    return ret


def complete_tree(buckets):
    """
    Reconstructs a verbose tree from a sorted collection of coded paths and descriptors.

    buckets:
    {1: [
        {"path": "root\\", "descr":"CHOP", "haskids: True}
     ]
     2: [
        {"path": "root\\01\\", "descr":"firstlevelcategory", "haskids: True, "completed": "CHOP (root)\\firstlevelcategory (01)"}
     ]
    }
    """
    flat = {}
    for i in range(len(buckets.keys())):
        i = i + 1
        for item in buckets[i]:
            item.update({"completed": complete_term(item)})
            correct_indepth(buckets, item, i)
            flat.update({item["completed"]: item["haskids"]})
    return flat


def complete_term(entry):
    # Build the complete name of a leaf (merge its identifier and descriptor)
    resp = entry["path"]
    smallname = entry["descr"]
    splitd = resp.split("\\")
    nbr = splitd[-1]
    if nbr in smallname:
        try:
            int(nbr[1])
            if nbr[0] != "(":
                nbr = " (" + nbr + ")"
            compl = smallname[smallname.index(" ") + 1 :] + nbr
        except:
            compl = smallname
    else:
        compl = smallname
    return "\\".join(splitd[:-1]) + "\\" + compl


def correct_indepth(buckets, model, startidx):
    """
    Walk through the sorted children paths of a node and overwrites them to embed the full name of the said node.
    """
    old = model["path"]
    new = model["completed"]

    for j in buckets.keys():
        if j > startidx:
            for item in buckets[j]:
                rep = len(old)
                if item["path"][:rep] == old:
                    model["haskids"] = True
                    item["path"] = new + item["path"][rep:]


def read_xls_DWH(terminology):
    def build_index_path(level, code):

        pass

    csvfilename = [fnm for fnm in os.listdir(SOURCE_DIR) if terminology in fnm][0]
    entries = []
    csvfile = open(SOURCE_DIR + csvfilename, "r")
    reader = csv.DictReader(csvfile)
    buckets = {}
    # a bucket i is a list of elements of level i
    for row in reader:
        path = build_index_path()
        descr = row["LIB_COURT_" + terminology]
        haskids = row["NOEUD_FEUILLE"] == "N"
        code = row["CODE_" + terminology + "_SANSPOINT"]
        entries.append({"code": code, "haskids": haskids, "descr": descr})
    pass


##################
## ICD-O reconstruction functions
##################


def read_term_ICDO(pathtofiles):
    ont_Tname = "ICDO-3-T"
    ont_Mname = "ICDO-3-M"
    t_file = pathtofiles + "Topoenglish.csv"
    m_file = pathtofiles + "Morphenglish.csv"

    ontT = read_topo(t_file)
    ontM = read_morph(m_file)

    with open(EXTERNAL_LOCATION + ont_Tname, "w") as ff:
        json.dump(ontT, ff)
    with open(EXTERNAL_LOCATION + ont_Mname, "w") as fp:
        json.dump(ontM, fp)


def read_topo(topofile):
    db = from_csv(topofile)
    flat = {}
    classed = {"3": [], "4": [], "5": [], "6": []}
    for line in db:
        section = line["Kode"].split(".")
        level = line["Lvl"]
        if len(section) == 1:
            path = "\\" + section[0]
        else:
            if level == "incl":
                path = (
                    "\\"
                    + section[0]
                    + "\\"
                    + ".".join(section)
                    + "\\"
                    + line["Kode"]
                    + "-incl"
                )
                level = "5"
            elif level == "k":
                path = (
                    "\\"
                    + section[0]
                    + "\\"
                    + ".".join(section)
                    + "\\"
                    + line["Kode"]
                    + "-k"
                )
                level = "5"
            elif level == "b":
                path = (
                    "\\"
                    + section[0]
                    + "\\"
                    + ".".join(section)
                    + "\\"
                    + line["Kode"]
                    + "-k\\"
                    + line["Kode"]
                    + "-b"
                )
                level = "6"
            else:
                path = "\\" + section[0] + "\\" + ".".join(section)
                level = "4"
        haskids = int(level) < 5 or path[-1] == "k"
        tmpdic = {
            "path": "\\ICDO-3-T" + path,
            "descr": line["Kode"] + " " + line["Title"],
            "haskids": haskids,
        }
        if int(level) > 4:
            tmpdic["descr"] = line["Title"]
        classed[level].append(tmpdic)
    for i in range(3, 7):
        key = str(i)
        for model in classed[key]:
            model["completed"] = complete_term(model)
            correct_indepth(classed, model, key)
            flat.update({model["completed"]: model["haskids"]})
    return flat


def read_morph(morphfile):
    db = from_csv(morphfile)
    flat = {}
    buckets = {"1": [], "2": []}
    for line in db:
        if line["Struct"] == "sub":
            buckets["2"].append(
                {
                    "path": "\\ICDO-3-M\\" + line["Code"] + "\\",
                    "descr": line["Label"],
                    "haskids": False,
                }
            )
        else:
            buckets["1"].append(
                {
                    "path": "\\ICDO-3-M\\" + line["Code"],
                    "descr": line["Code"] + " " + line["Label"],
                    "haskids": False,
                }
            )
    for i in [1, 2]:
        key = str(i)
        for model in buckets[key]:
            model["completed"] = complete_term(model)
            correct_indepth(buckets, model, key)
            flat.update({model["completed"]: model["haskids"]})
    return flat


def included_id(locationid, candid):
    """
    Returns True if the locationid is included in the interval defined by the candid, else returns False.
    """
    if "-" in candid:
        # First, check the letter matches
        bounds = candid.split("-")
        letter = [bnd[0] for bnd in bounds]
        if locationid[0] < letter[0] or locationid[0] > letter[1]:
            return False
        # Then, check the numbers. The target should be numerically between the two bounds, except if
        # the upper bound is written as int (ex target 84.2 should be valid for interval 82-84 as the dot
        # represents a subelement)
        number = [bnd[1:] for bnd in bounds]
        if float(locationid[1:]) < float(number[0]) or float(locationid[1:]) > float(
            number[1]
        ):
            if not included_id(locationid, bounds[1]):
                return False
        return True
    else:
        # The only inclusion is of type 84.3 c 84
        return candid in locationid


def find_parent_recurs(locationid, pool):
    """
    Navigates through the stored paths to place the locationid. Recursively narrows the search pool once a match is found.
    """
    if not bool(pool):
        return False
    for candidate in pool.keys():
        cid = candidate.split("(")[-1].split(")")[0]
        if included_id(locationid, cid):
            # Once we found an entrypoint, we do the recursive call over the children of the entry point
            res = find_parent_recurs(
                locationid,
                {key: pool[key] for key in pool.keys() if candidate + "\\" in key},
            )
            return res if res else candidate
    return False


def correct_position(classlabel, locationid, pool):
    """
    Finds the correct position of the class (represented by its name and ID) in the pool i.e finds the closest parent having an adequate interval as ID.
    Returns the complete path of the class that is, the path of the closest parent (ID removed) plus the class name and ID.

    We assume that all lost children are leaves.
    """
    foundparent = find_parent_recurs(locationid, pool)
    # Find a parent, and look in its children if any would fit as a parent too, if so update

    if pool[foundparent] == False:
        pool[foundparent] = True
    # Remove the ID from the parent path
    path = foundparent
    path = path + "\\" + classlabel + "(" + locationid + ")"
    return {path: False}
