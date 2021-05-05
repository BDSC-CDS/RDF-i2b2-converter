from utils import *


def generate_tables(suffix=""):
    """
    Reads the SVIP ontology file and splits it into four tables, that both the ontology and the connector will use.
    """
    hgvs_c = []
    hgvs_p = []
    gene_name = []
    protein_variant = []
    data = read_ontodic("SVIP" + suffix)
    for el in data:
        hgvs_c.append(el["hgvs_c"])
        hgvs_p.append(el["hgvs_p"])
        gene_name.append(el["gene_name"])
        protein_variant.append(el["protein_variant"])
    write_ontodic("SVIP.hgvs_c", hgvs_c, "a")
    write_ontodic("SVIP.hgvs_p", hgvs_p, "a")
    write_ontodic("SVIP.protein_variant", protein_variant, "a")
    write_ontodic("SVIP.gene_name", gene_name, "a")
