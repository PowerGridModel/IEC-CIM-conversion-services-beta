from rdflib import RDF, Graph, Namespace
import json


def transformation_function(input_file, output_folder, logger, cim, input_parameters, corr_id):

    logger.info("Start IEC CIM to PGM transform")

    # create CIM file graph and import into g
    g = Graph()
    g.parse(input_file)
    cim = Namespace(cim)

    # register and bind namespace
    g.bind("cim", cim)


    #cim to PGM
    num_components = 0
    list_nodes, list_nodes_json, num_components = busbar_to_nodes(g, cim, logger, num_components, input_parameters)
    list_symloads, num_components = energyconsumer_to_symload(g, cim, logger, num_components, list_nodes, input_parameters)
    list_sources, num_components = energysource_to_source(g, cim, logger, num_components, list_nodes, input_parameters)
    list_aclines, num_components = aclinesegment_to_line(g, cim, logger, num_components, list_nodes, input_parameters)
    list_trafo, num_components = powertransformer_to_transformer(g, cim, logger, num_components, list_nodes, input_parameters)

    #create output json
    out_pgm = create_out_json(list_nodes_json, list_symloads, list_sources, list_aclines, list_trafo)
    output_file = output_folder + "/output_" + corr_id + ".json"

    with open(output_file, "w") as outfile:
        json.dump(out_pgm, outfile, indent=4)

    return out_pgm



def busbar_to_nodes(graph, cim, logger, num_components, input_parameters):
    list_nodes = []
    list_nodes_json = []
    logger.info("Start transforming BusbarSections to nodes")

    for n in graph.subjects(RDF.type, cim["TopologicalNode"]):
        num_components = int(num_components) + 1
        bv = graph.value(n, cim["TopologicalNode.BaseVoltage"])
        volt = graph.value(bv, cim["BaseVoltage.nominalVoltage"])
        name = graph.value(n, cim["IdentifiedObject.name"])
        list_nodes.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_uid_name"): n})
        list_nodes_json.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_base_voltage_name"): float(volt) * 1000, "name": str(name)})


    logger.info("End transforming BusbarSections to nodes: " + str(len(list_nodes)) + " nodes")

    return(list_nodes, list_nodes_json, num_components)


def energyconsumer_to_symload(graph, cim, logger, num_components, list_nodes, input_parameters):
    list_symload = []

    logger.info("Start transforming EnergyConsumers to symloads")

    for n in graph.subjects(RDF.type, cim["EnergyConsumer"]):
        num_components = num_components + 1

        term = graph.value(predicate=cim["Terminal.ConductingEquipment"], object=n)
        bus = graph.value(term, cim["Terminal.TopologicalNode"])
        p = graph.value(n, cim["EnergyConsumer.pfixed"])
        q = graph.value(n, cim["EnergyConsumer.qfixed"])

        node = [i for i in list_nodes if i[input_parameters.get("input_uid_name")]==bus][0][input_parameters.get("input_id_name")]

        list_symload.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_nodes_name"): node,
                             input_parameters.get("active_power_name"): float(p) * 1000000, input_parameters.get("reactive_power_name"): float(q) * 1000000,
                             input_parameters.get("input_status_name"): 1, input_parameters.get("input_type_nodes_name"): 0})

    logger.info("End transforming EnergyConsumers to symloads: " + str(len(list_symload)) + " symloads")

    return list_symload, num_components

def energysource_to_source(graph, cim, logger, num_components, list_nodes, input_parameters):

    logger.info("Start transforming ExternalNetworkInjections to sources - CIM17")
    list_source = []
    for n in graph.subjects(RDF.type, cim["ExternalNetworkInjection"]):
        num_components = num_components + 1
        term = graph.value(predicate=cim["Terminal.ConductingEquipment"], object=n)
        bus = graph.value(term, cim["Terminal.TopologicalNode"])

        node = [i for i in list_nodes if i[input_parameters.get("input_uid_name")] == bus][0][input_parameters.get("input_id_name")]

        list_source.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_nodes_name"): node,
                             input_parameters.get("input_ref_voltage_name"): 1, input_parameters.get("input_status_name"): 1})

    logger.info("End transforming ExternalNetworkInjections to sources: " + str(len(list_source)) + " sources")

    logger.info("Start transforming EnergySources to sources - CIM15")
    list_source = []
    for n in graph.subjects(RDF.type, cim["EnergySource"]):
        num_components = num_components + 1
        term = graph.value(predicate=cim["Terminal.ConductingEquipment"], object=n)
        bus = graph.value(term, cim["Terminal.TopologicalNode"])

        node = [i for i in list_nodes if i[input_parameters.get("input_uid_name")] == bus][0][input_parameters.get("input_id_name")]

        list_source.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_nodes_name"): node,
                            input_parameters.get("input_ref_voltage_name"): 1, input_parameters.get("input_status_name"): 1})

    logger.info("End transforming EnergySources to sources: " + str(len(list_source)) + " sources")

    return list_source, num_components

def aclinesegment_to_line(graph, cim, logger, num_components, list_nodes, input_parameters):
    list_lines = []

    logger.info("Start transforming AclineSegments to lines")
    for n in graph.subjects(RDF.type, cim["ACLineSegment"]):
        num_components = num_components + 1

        terms = list(graph.subjects(predicate=cim["Terminal.ConductingEquipment"], object=n))
        tnode1 = graph.value(terms[0], cim["Terminal.TopologicalNode"])
        tnode2 = graph.value(terms[1], cim["Terminal.TopologicalNode"])
        name = graph.value(n, cim["IdentifiedObject.name"])
        r = float(graph.value(n, cim["ACLineSegment.r"]))
        x = float(graph.value(n, cim["ACLineSegment.x"]))
        bch = float(graph.value(n, cim["ACLineSegment.bch"])) / (2 * 3.141592654 * 50)
        #r0 = float(graph.value(n, cim["ACLineSegment.r0"]))
        #x0 = float(graph.value(n, cim["ACLineSegment.x0"]))
        #b0ch = float(graph.value(n, cim["ACLineSegment.b0ch"]))/ (2 * 3.141592654 * 50)

        tan1_default = 0
        in_default = 10000

        node1 = [i for i in list_nodes if i[input_parameters.get("input_uid_name")] == tnode1][0][input_parameters.get("input_id_name")]
        node2 = [i for i in list_nodes if i[input_parameters.get("input_uid_name")] == tnode2][0][input_parameters.get("input_id_name")]

        list_lines.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_left_nodes_name"): node1, input_parameters.get("input_right_nodes_name"): node2,
                            input_parameters.get("res_name"): r, input_parameters.get("react_name"): x, input_parameters.get("bch_name"): bch, "tan1": tan1_default,
                           input_parameters.get("res_name_0"): 0, input_parameters.get("react_name_0"): 0, input_parameters.get("bch_name_0"): 0, "tan0": tan1_default,
                           "i_n": in_default,
                           input_parameters.get("input_left_status_name"): 1, input_parameters.get("input_status_na_name") : 1, input_parameters.get("input_nodes_name"): str(name)})

    logger.info("Set default values for lines: tan1=0, tan0=0, i_n=10000, r0=0, x0=0, c0=0")
    logger.info("End transforming AclineSegments to lines: " + str(len(list_lines)) + " lines")

    return list_lines, num_components


def powertransformer_to_transformer(graph, cim, logger, num_components, list_nodes, input_parameters):
    list_trafo = []

    logger.info("Start transforming PowerTransformers to transformers")
    for n in graph.subjects(RDF.type, cim["PowerTransformer"]):
        num_components = num_components + 1
        name = graph.value(n, cim["IdentifiedObject.name"])
        tend = list(graph.subjects(predicate=cim["PowerTransformerEnd.PowerTransformer"], object=n))
        term1 = graph.value(tend[0], cim["TransformerEnd.Terminal"])
        term2 = graph.value(tend[1], cim["TransformerEnd.Terminal"])
        tnode1 = graph.value(term1, cim["Terminal.TopologicalNode"])
        tnode2 = graph.value(term2, cim["Terminal.TopologicalNode"])
        v1 = float(graph.value(tend[0], cim["PowerTransformerEnd.ratedU"])) * 1000
        v2 = float(graph.value(tend[1], cim["PowerTransformerEnd.ratedU"])) * 1000


        node1 = [i for i in list_nodes if i[input_parameters.get("input_uid_name")] == tnode1][0][input_parameters.get("input_id_name")]
        node2 = [i for i in list_nodes if i[input_parameters.get("input_uid_name")] == tnode2][0][input_parameters.get("input_id_name")]

        if v1>v2:
            S = float(graph.value(tend[0], cim["PowerTransformerEnd.ratedS"]))
            list_trafo.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_left_nodes_name"): node1, input_parameters.get("input_right_nodes_name"): node2,
                           "u1": v1, "u2": v2, input_parameters.get("base_mva_name"): S, "uk": 0.04, "pk": 1000,
                           "i0": 0.001, "p0": 100, "winding_from": 2, "winding_to": 1, "clock": 5,
                           "tap_side": 0, "tap_pos": 0, "tap_min": -5, "tap_max": 5, "tap_nom": 0,  "tap_size": 2300.0,
                           "from_status": 1, "to_status" : 1, "name": str(name)})
        else:
            S = float(graph.value(tend[1], cim["PowerTransformerEnd.ratedS"]))
            list_trafo.append({input_parameters.get("input_id_name"): num_components, input_parameters.get("input_left_nodes_name"): node2, input_parameters.get("input_right_nodes_name"): node1,
                           "u1": v2, "u2": v1, input_parameters.get("base_mva_name"): S, "uk": 0.04, "pk": 1000,
                           "i0": 0.001, "p0": 100, "winding_from": 2, "winding_to": 1, "clock": 5,
                           "tap_side": 0, "tap_pos": 0, "tap_min": -5, "tap_max": 5, "tap_nom": 0,  "tap_size": 2300.0,
                           input_parameters.get("input_left_status_name"): 1, input_parameters.get("input_right_status_name") : 1, "name": str(name)})

    logger.info("Set default values for transformers: uk=0.04, pk=1000, i0=0.001, p0=100, tap changer values ")
    logger.info("End transforming PowerTransformers to trasformers: " + str(len(list_trafo)) + " transformers")

    return list_trafo, num_components

def create_out_json(list_nodes, list_symloads, list_sources, list_aclines, list_trafo):
    out_pgm = dict()
    if len(list_nodes) > 0:
        out_pgm["node"] = list_nodes
    if len(list_symloads) > 0:
        out_pgm["sym_load"] = list_symloads
    if len(list_sources) > 0:
        out_pgm["source"] = list_sources
    if len(list_aclines) > 0:
        out_pgm["line"] = list_aclines
    if len(list_trafo)>0:
        out_pgm["transformer"] = list_trafo

    return out_pgm
