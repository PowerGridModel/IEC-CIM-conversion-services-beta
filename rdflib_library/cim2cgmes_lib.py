from rdflib import Graph, RDF

def cim2cgmes(input_graph, cim, logger):
    # create CIM output graph
    out_graph = Graph()
    out_graph.bind("cim", cim)
    # ---------------------END: importing input files and creating graphs ------------------------------#

    # ---------------------BEGIN: selecting CIM entities ------------------------------#
    logger.info("Start selecting IEC CIM entities for bus-branch model")
    logger.info("Selecting regions and subregions")
    for s, p, o in input_graph.triples((None, RDF.type, cim["SubGeographicalRegion"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))
    for s, p, o in input_graph.triples((None, RDF.type, cim["GeographicalRegion"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting topological nodes")
    for s, p, o in input_graph.triples((None, RDF.type, cim["TopologicalNode"])):
        for s1, p1, o1 in input_graph.triples((s, p, None)):
            out_graph.add((s1, p1, o1))
        for s1, p1, o1 in input_graph.triples((s, cim["IdentifiedObject.name"], None)):
            out_graph.add((s1, p1, o1))
        for s1, p1, o1 in input_graph.triples((s, cim["TopologicalNode.BaseVoltage"], None)):
            out_graph.add((s1, p1, o1))
        for s1, p1, o1 in input_graph.triples((s, cim["TopologicalNode.ConnectivityNodeContainer"], None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting base voltages")
    for s, p, o in input_graph.triples((None, RDF.type, cim["BaseVoltage"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting substations and voltage levels")
    for s, p, o in input_graph.triples((None, RDF.type, cim["Substation"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))
    for s, p, o in input_graph.triples((None, RDF.type, cim["VoltageLevel"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting energy consumers and related terminals")
    for s, p, o in input_graph.triples((None, RDF.type, cim["EnergyConsumer"])):
        for s1, p1, o1 in input_graph.triples((s, p, None)):
            out_graph.add((s1, p1, o1))
    for s, p, o in input_graph.triples((None, RDF.type, cim["EnergyConsumer"])):
        for s1, p1, o1 in input_graph.triples((s, cim["IdentifiedObject.name"], None)):
            out_graph.add((s1, p1, o1))
        for s1, p1, o1 in input_graph.triples((s, cim["Equipment.EquipmentContainer"], None)):
            out_graph.add((s1, p1, o1))
        #for cim17
        for s1, p1, o1 in input_graph.triples((s, cim["Equipment.aggregate"], None)):
            out_graph.add((s1, p1, o1))

        for s1, p1, o1 in input_graph.triples((None, cim["Terminal.ConductingEquipment"], s)):
            out_graph.add((s1, RDF.type, cim["Terminal"]))
            out_graph.add((s1, p1, o1))

        #for cim17
        for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.sequenceNumber"], None)):
            out_graph.add((s2, p2, o2))
        for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.connected"], None)):
            out_graph.add((s2, p2, o2))

        #for cim15
        for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.sequenceNumber"], None)):
            out_graph.add((s2, p2, o2))
        for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.connected"], None)):
            out_graph.add((s2, p2, o2))

            for s2, p2, o2 in input_graph.triples((s1, cim["IdentifiedObject.name"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.TopologicalNode"], None)):
                out_graph.add((s2, p2, o2))

    logger.info("Selecting external network injections and related terminals (cim17)")
    for s, p, o in input_graph.triples((None, RDF.type, cim["ExternalNetworkInjection"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

        for s1, p1, o1 in input_graph.triples((None, cim["Terminal.ConductingEquipment"], s)):
            out_graph.add((s1, RDF.type, cim["Terminal"]))
            out_graph.add((s1, p1, o1))
            for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.sequenceNumber"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["IdentifiedObject.name"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.connected"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.TopologicalNode"], None)):
                out_graph.add((s2, p2, o2))

    logger.info("Selecting energy sources and related terminals (cim15)")
    for s, p, o in input_graph.triples((None, RDF.type, cim["EnergySource"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

        for s1, p1, o1 in input_graph.triples((None, cim["Terminal.ConductingEquipment"], s)):
            out_graph.add((s1, RDF.type, cim["Terminal"]))
            out_graph.add((s1, p1, o1))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.sequenceNumber"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["IdentifiedObject.name"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.connected"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.TopologicalNode"], None)):
                out_graph.add((s2, p2, o2))

    logger.info("Selecting power transformer ends")
    for s, p, o in input_graph.triples((None, RDF.type, cim["PowerTransformerEnd"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting power transformers and related terminals")
    for s, p, o in input_graph.triples((None, RDF.type, cim["PowerTransformer"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

        for s1, p1, o1 in input_graph.triples((None, cim["Terminal.ConductingEquipment"], s)):
            out_graph.add((s1, RDF.type, cim["Terminal"]))
            out_graph.add((s1, p1, o1))

            #cim17
            for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.sequenceNumber"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.connected"], None)):
                out_graph.add((s2, p2, o2))

            #cim15
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.sequenceNumber"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.connected"], None)):
                out_graph.add((s2, p2, o2))

            for s2, p2, o2 in input_graph.triples((s1, cim["IdentifiedObject.name"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.TopologicalNode"], None)):
                out_graph.add((s2, p2, o2))

    logger.info("Selecting acline segments and related terminals")
    for s, p, o in input_graph.triples((None, RDF.type, cim["ACLineSegment"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

        for s1, p1, o1 in input_graph.triples((None, cim["Terminal.ConductingEquipment"], s)):
            out_graph.add((s1, RDF.type, cim["Terminal"]))
            out_graph.add((s1, p1, o1))
            for s2, p2, o2 in input_graph.triples((s1, cim["IdentifiedObject.name"], None)):
                out_graph.add((s2, p2, o2))

            #cim17
            for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.sequenceNumber"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["ACDCTerminal.connected"], None)):
                out_graph.add((s2, p2, o2))
            #cim15
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.sequenceNumber"], None)):
                out_graph.add((s2, p2, o2))
            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.connected"], None)):
                out_graph.add((s2, p2, o2))

            for s2, p2, o2 in input_graph.triples((s1, cim["Terminal.TopologicalNode"], None)):
                out_graph.add((s2, p2, o2))

    logger.info("Selecting ratio tap changer")
    for s, p, o in input_graph.triples((None, RDF.type, cim["RatioTapChanger"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting lines")
    for s, p, o in input_graph.triples((None, RDF.type, cim["Line"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting svvoltages")
    for s, p, o in input_graph.triples((None, RDF.type, cim["SvVoltage"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("Selecting svpowerflows")
    for s, p, o in input_graph.triples((None, RDF.type, cim["SvPowerFlow"])):
        for s1, p1, o1 in input_graph.triples((s, None, None)):
            out_graph.add((s1, p1, o1))

    logger.info("End selecting IEC CIM entities for bus-branch model")
    # ---------------------END: selecting CIM entities ------------------------------#


    return out_graph

