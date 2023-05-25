import networkx as nx
import uuid
from rdflib import URIRef, RDF, Literal

def underscore_uuid():
    return "_" + str(uuid.uuid1())

def createTopology_networkX(graph, cim, base, logger, connected_acline, connected_trafo, connected_breaker, cnodes):

    logger.info("Start adding topological islands using networkX")
    G_tisland = nx.Graph(name="Network topological islands")

    #list to store islands id
    islands_id = []

    #add all connectivity nodes to G_tisland
    for cn in cnodes:
        G_tisland.add_node(cn[0], cc=cn[1], ccname=cn[2], bv=cn[3])


    #add all branch types to G_tisland
    G_tisland.add_edges_from(connected_acline, type="Acline", color="r")
    G_tisland.add_edges_from(connected_trafo, type="Trafo", color="b")
    G_tisland.add_edges_from(connected_breaker, type="Breaker", color="g")

    #find connected components of graph
    S_island = [G_tisland.subgraph(c).copy() for c in nx.connected_components(G_tisland)]
    nIsland = 0 #iterator

    for tisland in S_island:
        nIsland = nIsland + 1
        island_name = "Island " + str(nIsland)
        island_id = underscore_uuid()
        cim.NewIsland = URIRef(base + str(island_id))
        islands_id.append(cim.NewIsland)

        # add new topological island to rdflib graph
        graph.add((cim.NewIsland, RDF.type, cim.TopologicalIsland))
        graph.add((cim.NewIsland, cim["IdentifiedObject.name"], Literal(island_name)))

        for cn in tisland:
            G_tisland.add_node(cn, island=cim.NewIsland)


    logger.info("End adding topological islands using networkX")

    logger.info("Start adding topological nodes using networkX")
    # graph for topological nodes search
    G_tnodes = nx.Graph(name="Network topological nodes")

    #G_tnodes has just breakers as branches
    G_tnodes.add_edges_from(connected_breaker, type="Breaker", color="g")
    #add all nodes to graph
    G_tnodes.add_nodes_from(G_tisland.nodes(data=True))

    #find graph connected components
    S_nodes = [G_tnodes.subgraph(c).copy() for c in nx.connected_components(G_tnodes)]
    nTN = 0 #nodes iterator
    for tnode in S_nodes:
        nTN = nTN + 1
        TN_name = "TNODE"+str(nTN)
        node_id = underscore_uuid()
        cim.NewNode = URIRef(base + str(node_id))

        # add new topological node to rdflib graph
        graph.add((cim.NewNode, RDF.type, cim.TopologicalNode))
        #graph.add((cim.NewNode, cim["IdentifiedObject.name"], Literal(TN_name)))
        graph.add((cim.NewNode, cim["IdentifiedObject.name"], list(tnode.nodes.data("ccname"))[0][1]))
        graph.add((cim.NewNode, cim["TopologicalNode.ConnectivityNodeContainer"], list(tnode.nodes.data("cc"))[0][1]))
        graph.add((cim.NewNode, cim["TopologicalNode.TopologicalIsland"], list(tnode.nodes.data("island"))[0][1]))
        graph.add((cim.NewNode, cim["TopologicalNode.BaseVoltage"], list(tnode.nodes.data("bv"))[0][1]))

        # create voltage state variables
        svvoltage_id = underscore_uuid()
        cim.NewSV = URIRef(base + str(svvoltage_id))

        # add state variables for voltages
        volt = graph.value((list(tnode.nodes.data("bv"))[0][1]), cim["BaseVoltage.nominalVoltage"])
        graph.add((cim.NewSV, RDF.type, cim.SvVoltage))
        graph.add((cim.NewSV, cim["SvVoltage.v"], Literal(volt)))
        graph.add((cim.NewSV, cim["SvVoltage.angle"], Literal(0)))
        graph.add((cim.NewSV, cim["SvVoltage.TopologicalNode"], cim.NewNode))

        #add topological nodes to terminals
        for cn in tnode:
            for term, pred, connN in graph.triples((None, cim['Terminal.ConnectivityNode'], cn)):
                if (term, cim['Terminal.TopologicalNode'], None) not in graph:
                    graph.add((term, cim['Terminal.TopologicalNode'], cim.NewNode))
            G_tisland.add_node(cn, tnode=cim.NewNode, tnodeName = TN_name)

    # remove topological nodes from terminals of close breakers
    term_switch = graph.query("""SELECT ?term WHERE {
        ?term cim:Terminal.ConductingEquipment ?CE .
        ?CE cim:Switch.normalOpen "false" .}""")

    for row in term_switch:
        graph.remove((row.term, cim['Terminal.TopologicalNode'], None))


    logger.info("End adding topological nodes")

    return islands_id




