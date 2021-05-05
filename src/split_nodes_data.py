from rdf_base import *
import random
from shutil import rmtree

"""
A tool script that will split existing RDF data into the 3 nodes for test deployments.
Folders node_0, node_1 ... node_i must exist in the same folder where the data is.
Data will be split in i patient groups that will populate the i nodes.

Each node folder can then be specified one by one as the data folder for a data loading procedure.
"""

totalg = unify_graph(DATA_GRAPHS)


g_nodes = [rdflib.Graph(), rdflib.Graph(), rdflib.Graph()]
nb_nodes = len(g_nodes)
directories = []
# Create target directories if they do not exist, empty them
for idx in range(nb_nodes):
    target_dir = DATA_GRAPHS_LOCATION + "node_" + str(idx) + "/"
    directories.append(target_dir)
    os.makedirs(os.path.dirname(target_dir), exist_ok=True)
    rmtree(target_dir)
    os.makedirs(os.path.dirname(target_dir), exist_ok=True)


list_patients = totalg.query(
    """
    SELECT DISTINCT ?p 
    WHERE
    {
        ?p rdf:type ?spi
    }
    """,
    initBindings={"spi": SPHN["SubjectPseudoIdentifier"]},
)
list_patients = [e[0] for e in list_patients]
numpat = len(list_patients)
bounds = [0]
context = []
prev_min = round(numpat * 0.1)
max_b = round(numpat * 0.8)
for i in range(nb_nodes):
    bounds.append(random.randint(prev_min, max_b))
    prev_min = bounds[-1]
    context.append("partial_" + str(i))
bounds[-1] = numpat
pdb.set_trace()

conj = rdflib.ConjunctiveGraph()
conj.addN([e + (RDF.maingraph,) for e in totalg])

for e in range(nb_nodes):

    patients = list_patients[bounds[e] : bounds[e + 1]]
    spi_graph = rdflib.Graph()
    for p in patients:
        # Add all links with this patient as subject to the main graph.
        conj.addN(
            [link + (RDF[context[e]],) for link in totalg.triples((p, None, None))]
        )

    response = conj.query(
        """
        SELECT ?s ?p ?o
        {   
            ?s ?p ?o .
            ?s ?spi ?q .
            GRAPH ?ctx {?q rdf:type ?n}
        }
    """,
        initBindings={
            "spi": SPHN["L2-subject_pseudo_identifier"],
            "ctx": RDF[context[e]],
            "code": SPHN.Code,
        },
    )

    conj.addN([tp + (RDF[context[e]],) for tp in response])
    g_nodes[e].addN(
        [
            x[:-1] + (g_nodes[e],)
            for x in conj.quads((None, None, None, RDF[context[e]]))
        ]
    )

    additional = conj.query(
        """
        SELECT ?s ?p ?o
        WHERE{
            ?s ?p ?o.
            {
            {
                ?s rdf:type ?code
            }
            UNION
            {
                ?s rdf:type ?unit
            }
            UNION
            {
                ?s rdf:type ?dpi
            }
            }
        }
    """,
        initBindings={
            "code": SPHN.Code,
            "unit": SPHN.Unit,
            "dpi": SPHN.DataProviderInstitute,
        },
    )

    g_nodes[e].addN([link + (g_nodes[e],) for link in additional])

onto_g = rdflib.Graph()
onto_g.parse(ONTOLOGY_GRAPH, format="turtle")
for idx in range(nb_nodes):
    split_graph(g_nodes[idx], onto_g, directories[idx])
pdb.set_trace()
