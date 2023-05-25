from power_grid_model.utils import import_json_data
import pandas as pd
import uuid
from rdflib import URIRef, RDF, Literal, Graph, Namespace
import os
from rdflib_library import cim2cgmes_lib
from rdflib_library import xslt_lib as xsltLibrary
from rdflib_library import topologicalProcessor as topLibrary


# ---------------------Generic functions ------------------------------#
def underscore_uuid():
    return "_" + str(uuid.uuid1())


def Z_from_pu(z, baseV, baseMVA):
    conversion_factor = baseV * baseV / baseMVA
    return z * conversion_factor


def Y_from_pu(y, baseV, baseMVA):
    conversion_factor = baseV * baseV / baseMVA
    return y / conversion_factor


def create_table_bus(input_db, vett_nodo, vett_nodo_na):
    nodi = input_db[vett_nodo]
    nodi_na = (input_db[vett_nodo_na]
    .rename(
        columns={vett_nodo_na[i]: vett_nodo[i] for i in range(len(vett_nodo))}))

    # append
    nodi_all = pd.concat([nodi, nodi_na]).drop_duplicates().reset_index(drop=True)

    return nodi_all


def define_rdf_xml_graph(cim_namespace, output_cim):
    # define graph and register namespaces
    graph = Graph()
    cim = Namespace(cim_namespace)
    outfile_namespace = "file://" + os.path.abspath(output_cim) + "#"
    base = Namespace(outfile_namespace)
    graph.bind("cim", cim)

    return graph, base, cim


# ---------------------RDFlib transformation functions ------------------------------#
def rdflib_transform(cim_version, input_nodes, input_ext_net, input_loads, input_aclines, input_trafos,
                     input_cong, operator_name, graph, cim, base, logger, input_parameters, input_switch_name):
    ###region and subregion
    names_reg_subreg = create_region_subregion(operator_name, graph, cim, base)
    logger.info("Created region: " + names_reg_subreg[0])
    logger.info("Created subregion: " + names_reg_subreg[1])
    sub_reg_id = names_reg_subreg[2]

    ### base voltages
    n_base_volt, dict_volt = create_base_voltage(input_nodes, graph, cim, base, input_parameters)
    logger.info("Created base voltages: " + n_base_volt + " elements")

    ### busbars and voltage levels
    n_busbars, dict_bus, dict_vlevel, dict_cn, cc_list_busbar = create_busbar_voltagelevel(cim_version, input_nodes, graph, cim, base,
                                                                           dict_volt, input_parameters)
    logger.info("Created busbars and voltage levels: " + n_busbars + " elements")

    ### external network injection
    n_ext_net = create_ext_net(cim_version, input_ext_net, graph, cim, base,
                             dict_volt, dict_vlevel, dict_cn, input_parameters)
    logger.info("Created external network injection: " + str(n_ext_net) + " elements")

    ### energy consumers
    n_loads = create_loads(cim_version, input_loads, graph, cim, base,
                           dict_vlevel, dict_cn, input_parameters)
    logger.info("Created loads and related terminals: " + n_loads + " elements")

    ### aclines
    n_aclines = create_aclines(cim_version, input_aclines, graph, cim, base, sub_reg_id, dict_vlevel, dict_cn,
                               dict_volt, input_parameters, input_switch_name)
    logger.info("Created aclines, related terminals, connectivity nodes and breakers: " + n_aclines[0] + " elements, " +
                n_aclines[1] + " close breakers, " + n_aclines[2] + " open breakers")

    connected_aclines = n_aclines[3]
    connected_breakers_acline = n_aclines[4]
    cc_list_aclines = n_aclines[5]

    ### trafos
    n_trafos = create_trafo(cim_version, input_trafos, graph, cim, base, sub_reg_id, dict_vlevel, dict_cn,
                            dict_volt, input_parameters, input_switch_name)

    logger.info(
        "Created transformers, transformers end, related terminals, connectivity nodes and breakers: " + n_trafos[
            0] + " trafos, " + n_trafos[1] + ' close breakers, ' + n_trafos[2] + ' open breakers')
    logger.info("Created substations with transformers: " + n_trafos[3])

    connected_trafo = n_trafos[4]
    connected_breakers_trafo = n_trafos[5]
    cc_list_trafo = n_trafos[6]

    ### disconnectors
    n_disc = create_disconnector(cim_version, input_cong, graph, cim, base, sub_reg_id, dict_vlevel, dict_cn, dict_volt, input_parameters,
                                 input_switch_name)

    logger.info("Created disconnectors: " + n_disc[1] + ' close disconnectors, ' + n_trafos[2] + ' open disconnectors')
    logger.info("Created substations with disconnectors: " + n_disc[3])

    connected_breakers_disc = n_disc[4]
    connected_breakers = connected_breakers_trafo + connected_breakers_disc + connected_breakers_acline
    cc_list = cc_list_aclines + cc_list_trafo + cc_list_busbar

    # substations without trafos or disconnectors
    n_sub = create_substations(input_nodes, graph, cim, base, sub_reg_id, dict_vlevel, input_parameters)
    logger.info("Created substations without trafos or disconnectors: " + str(n_sub))

    return (connected_aclines, connected_trafo, connected_breakers, cc_list)


def create_region_subregion(region_name, graph, cim, base):
    subregion_id = underscore_uuid()
    region_id = underscore_uuid()

    subregion_name = "subregion-" + region_name
    region_name = "region-" + region_name

    cim.NewSubReg = URIRef(base + str(subregion_id))
    cim.NewReg = URIRef(base + str(region_id))

    graph.add((cim.NewSubReg, RDF.type, cim.SubGeographicalRegion))
    graph.add((cim.NewSubReg, cim["IdentifiedObject.name"], Literal(subregion_name)))
    graph.add((cim.NewSubReg, cim["SubGeographicalRegion.Region"], cim.NewReg))

    graph.add((cim.NewReg, RDF.type, cim.GeographicalRegion))
    graph.add((cim.NewReg, cim["IdentifiedObject.name"], Literal(region_name)))

    return [region_name, subregion_name, cim.NewSubReg]


def create_base_voltage(input_bus, graph, cim, base, input_parameters):
    # find unique voltages
    voltages_list = input_bus[input_parameters.get("input_base_voltage_name")].unique()
    dict_volt = {}

    for num in voltages_list:
        volt_id = underscore_uuid()
        cim.NewVolt = URIRef(base + str(volt_id))

        graph.add((cim.NewVolt, RDF.type, cim.BaseVoltage))
        graph.add((cim.NewVolt, cim["BaseVoltage.nominalVoltage"], Literal(num)))
        graph.add((cim.NewVolt, cim["IdentifiedObject.name"], Literal(str(num))))

        dict_volt[num] = cim.NewVolt

    return str(len(voltages_list)), dict_volt


def create_busbar_voltagelevel(cim_version, input_bus, graph, cim, base, dict_volt, input_parameters):
    dict_bus = {}
    dict_cn = {}
    dict_vlevel = {}
    cc_list = [] #list of connectivity nodes, to be used in topological processor

    for num in range(len(input_bus)):
        bus_name = 'busbar-' + (str(input_bus[input_parameters.get("input_nodes_name")][num]))

        # find nominal voltage
        voltage = input_bus[input_parameters.get("input_base_voltage_name")][num]
        volt_id = dict_volt.get(voltage)

        bus_id = underscore_uuid()
        cim.NewBus = URIRef(base + str(bus_id))
        cn_id = underscore_uuid()
        cim.NewCN = URIRef(base + str(cn_id))
        vl_id = underscore_uuid()
        cim.NewVL = URIRef(base + str(vl_id))

        # add busbars
        graph.add((cim.NewBus, RDF.type, cim.BusbarSection))
        graph.add((cim.NewBus, cim["IdentifiedObject.name"], Literal(bus_name)))
        graph.add((cim.NewBus, cim["Equipment.EquipmentContainer"], cim.NewVL))
        graph.add((cim.NewBus, cim["ConductingEquipment.BaseVoltage"], volt_id))

        # add voltage levels
        graph.add((cim.NewVL, RDF.type, cim.VoltageLevel))
        graph.add((cim.NewVL, cim["VoltageLevel.BaseVoltage"], volt_id))
        graph.add((cim.NewVL, cim["IdentifiedObject.name"], Literal(bus_name + "-" + str(voltage) + "kV")))

        # create busbar terminal
        create_terminal(cim_version, "term-" + bus_name, cim.NewBus, cim.NewCN, 1, graph, cim, base)

        # add connectivity nodes
        graph.add((cim.NewCN, RDF.type, cim.ConnectivityNode))
        graph.add((cim.NewCN, cim["IdentifiedObject.name"], Literal("CN-" + bus_name)))
        graph.add((cim.NewCN, cim["ConnectivityNode.ConnectivityNodeContainer"], cim.NewVL))

        cc_list.append([cim.NewCN, cim.NewVL, Literal(bus_name + "-" + str(voltage) + "kV"), volt_id])

        dict_bus[bus_name] = cim.NewBus
        dict_vlevel[bus_name] = cim.NewVL
        dict_cn[bus_name] = cim.NewCN

    return str(len(input_bus)), dict_bus, dict_vlevel, dict_cn, cc_list


def create_loads(cim_version, input_load, graph, cim, base,
                 dict_vlevel, dict_cn, input_parameters):
    for num in range(len(input_load)):

        bus_name = 'busbar-' + str(input_load[input_parameters.get("input_nodes_name")][num])
        vlevel_id = dict_vlevel.get(bus_name)
        cn_id = dict_cn.get(bus_name)

        load_id = underscore_uuid()
        cim.NewLoad = URIRef(base + str(load_id))

        load_name = "load-" + str(input_load[input_parameters.get("input_nodes_name")][num])

        graph.add((cim.NewLoad, RDF.type, cim.EnergyConsumer))
        graph.add((cim.NewLoad, cim["IdentifiedObject.name"], Literal(load_name)))
        graph.add((cim.NewLoad, cim["Equipment.EquipmentContainer"], vlevel_id))

        # create powerflow state variables for loads
        svpower_id = underscore_uuid()
        cim.NewPF = URIRef(base + str(svpower_id))
        graph.add((cim.NewPF, RDF.type, cim.SvPowerFlow))

        if 'cim17' in cim_version:
            graph.add((cim.NewLoad, cim["Equipment.aggregate"], Literal("false")))

        if input_parameters.get("active_power_name") not in input_load.columns:
            graph.add((cim.NewLoad, cim["EnergyConsumer.pfixed"], Literal(0)))
            graph.add((cim.NewLoad, cim["EnergyConsumer.qfixed"], Literal(0)))
            graph.add((cim.NewPF, cim["SvPowerFlow.p"], Literal(0)))
            graph.add((cim.NewPF, cim["SvPowerFlow.q"], Literal(0)))
        else:
            graph.add((cim.NewLoad, cim["EnergyConsumer.pfixed"], Literal(input_load[input_parameters.get("active_power_name")][num])))
            graph.add((cim.NewLoad, cim["EnergyConsumer.qfixed"], Literal(input_load[input_parameters.get("reactive_power_name")][num])))
            graph.add((cim.NewPF, cim["SvPowerFlow.p"], Literal(input_load[input_parameters.get("active_power_name")][num])))
            graph.add((cim.NewPF, cim["SvPowerFlow.q"], Literal(input_load[input_parameters.get("reactive_power_name")][num])))

        term = create_terminal(cim_version, "term-" + load_name, cim.NewLoad, cn_id, 1, graph, cim, base)
        graph.add((cim.NewPF, cim["SvPowerFlow.Terminal"], term))

    return str(len(input_load))


def create_ext_net(cim_version, input_ext_net, graph, cim, base,
                   dict_volt, dict_vlevel, dict_cn, input_parameters):

    for num in range(len(input_ext_net)):

        bus_name = 'busbar-' + str(input_ext_net[input_parameters.get("input_nodes_name")][num])
        vlevel_id = dict_vlevel.get(bus_name)
        cn_id = dict_cn.get(bus_name)

        voltage_kV = input_ext_net[input_parameters.get("input_base_voltage_name")][num]
        base_voltage = dict_volt.get(voltage_kV)

        ext_net_id = underscore_uuid()
        cim.NewExtNet = URIRef(base + str(ext_net_id))
        ext_net_name = "external-network-" + str(input_ext_net[input_parameters.get("input_nodes_name")][num])

        if "cim17" in cim_version:
            graph.add((cim.NewExtNet, RDF.type, cim.ExternalNetworkInjection))
            graph.add((cim.NewExtNet, cim["Equipment.aggregate"], Literal("false")))

        elif "cim15" in cim_version:
            graph.add((cim.NewExtNet, RDF.type, cim.EnergySource))
            graph.add((cim.NewExtNet, cim["ConductingEquipment.BaseVoltage"], base_voltage))
            graph.add((cim.NewExtNet, cim["EnergySource.voltageMagnitude"], Literal(voltage_kV)))
            graph.add((cim.NewExtNet, cim["EnergySource.x"], Literal(0)))
            graph.add((cim.NewExtNet, cim["EnergySource.voltageAngle"], Literal(0)))

        graph.add((cim.NewExtNet, cim["IdentifiedObject.name"], Literal(ext_net_name)))
        graph.add((cim.NewExtNet, cim["Equipment.EquipmentContainer"], vlevel_id))

        create_terminal(cim_version, "term-" + ext_net_name, cim.NewExtNet, cn_id, 1, graph, cim, base)

    return len(input_ext_net)


def create_switch(cim_version, switch_type, switch_name, base_voltage, normal_open, cnode_start, cnode_end, container,
                  graph, cim, base):
    switch_id = underscore_uuid()
    cim.NewSwitch = URIRef(base + str(switch_id))

    if switch_type == 'breaker':
        graph.add((cim.NewSwitch, RDF.type, cim.Breaker))
        graph.add((cim.NewSwitch, cim["ProtectedSwitch.breakingCapacity"], Literal(-1)))
    elif switch_type == 'disconnector':
        graph.add((cim.NewSwitch, RDF.type, cim.Disconnector))

    graph.add((cim.NewSwitch, cim["IdentifiedObject.name"], Literal(switch_name)))
    graph.add((cim.NewSwitch, cim["ConductingEquipment.BaseVoltage"], base_voltage))
    graph.add((cim.NewSwitch, cim["Equipment.EquipmentContainer"], container))
    graph.add((cim.NewSwitch, cim["Switch.normalOpen"], Literal(normal_open)))

    create_terminal(cim_version, "term1-" + switch_name, cim.NewSwitch, cnode_start, 1, graph, cim, base)
    create_terminal(cim_version, "term2-" + switch_name, cim.NewSwitch, cnode_end, 2, graph, cim, base)


def create_lines(line_name, graph, cim, base, subregion):
    line_id = underscore_uuid()
    cim.NewLine = URIRef(base + str(line_id))
    graph.add((cim.NewLine, RDF.type, cim.Line))
    graph.add((cim.NewLine, cim["IdentifiedObject.name"], Literal(line_name)))
    graph.add((cim.NewLine, cim["Line.Region"], subregion))

    return cim.NewLine


def create_aclines(cim_version, input_branch, graph, cim, base, sub_reg_id, dict_vlevel, dict_cn, dict_volt, input_parameters, input_switch_name):
    connected_count = 0
    disconnected_count = 0
    connected_aclines = []
    connected_breaker = []
    cc_list = []

    for num in range(len(input_branch)):

        line_acline_name = str(input_branch[input_parameters.get("input_nodes_name")][num]) + "-" + str(
            input_branch[input_parameters.get("input_right_nodes_name")][num])

        line_name = "line-" + line_acline_name
        line_id = create_lines(line_name, graph, cim, base, sub_reg_id)

        start_busbar_name = 'busbar-' + str(input_branch[input_parameters.get("input_nodes_name")][num])
        end_busbar_name = 'busbar-' + str(input_branch[input_parameters.get("input_right_nodes_name")][num])

        start_vlevel_id = dict_vlevel.get(start_busbar_name)
        end_vlevel_id = dict_vlevel.get(end_busbar_name)

        start_cn_id = dict_cn.get(start_busbar_name)
        end_cn_id = dict_cn.get(end_busbar_name)

        voltage_kV = input_branch[input_parameters.get("input_base_voltage_name")][num]
        base_volt = dict_volt.get(voltage_kV)

        acline_id = underscore_uuid()
        cim.NewAcLine = URIRef(base + str(acline_id))
        graph.add((cim.NewAcLine, RDF.type, cim.ACLineSegment))
        graph.add((cim.NewAcLine, cim["IdentifiedObject.name"], Literal("acLineSegment-" + line_acline_name)))

        graph.add((cim.NewAcLine, cim["ACLineSegment.x"], Literal(input_branch[input_parameters.get("react_name")][num])))
        graph.add((cim.NewAcLine, cim["ACLineSegment.r"], Literal(input_branch[input_parameters.get("res_name")][num])))
        graph.add((cim.NewAcLine, cim["ACLineSegment.gch"], Literal(0)))
        graph.add((cim.NewAcLine, cim["ACLineSegment.bch"], Literal(input_branch[input_parameters.get("bch_name")][num])))
        graph.add((cim.NewAcLine, cim["ACLineSegment.x0"], Literal(input_branch[input_parameters.get("react_name_0")][num])))
        graph.add((cim.NewAcLine, cim["ACLineSegment.r0"], Literal(input_branch[input_parameters.get("res_name_0")][num])))
        graph.add((cim.NewAcLine, cim["ACLineSegment.b0ch"], Literal(input_branch[input_parameters.get("bch_name_0")][num])))
        graph.add((cim.NewAcLine, cim["Conductor.length"], Literal(input_branch[input_parameters.get("length_name")][num])))
        graph.add((cim.NewAcLine, cim["ConductingEquipment.BaseVoltage"], base_volt))

        if "cim17" in cim_version:
            graph.add((cim.NewAcLine, cim["Equipment.aggregate"], Literal("false")))

        graph.add((cim.NewAcLine, cim["Equipment.EquipmentContainer"], line_id))

        # create intermediate connectivity nodes
        cn_acline_1 = underscore_uuid()  # between acline and breaker
        cim.cnAcline1 = URIRef(base + str(cn_acline_1))
        cn_acline_2 = underscore_uuid()  # between acline and breaker
        cim.cnAcline2 = URIRef(base + str(cn_acline_2))

        # list of connected aclines
        connected_aclines.append([cim.cnAcline1, cim.cnAcline2])

        # add connectivity nodes
        graph.add((cim.cnAcline1, RDF.type, cim.ConnectivityNode))
        graph.add(
            (cim.cnAcline1, cim["IdentifiedObject.name"], Literal("CN-acLineSegment-" + line_acline_name + "-switch1")))
        graph.add((cim.cnAcline1, cim["ConnectivityNode.ConnectivityNodeContainer"], start_vlevel_id))
        graph.add((cim.cnAcline2, RDF.type, cim.ConnectivityNode))
        graph.add(
            (cim.cnAcline2, cim["IdentifiedObject.name"], Literal("CN-acLineSegment-" + line_acline_name + "-switch2")))
        graph.add((cim.cnAcline2, cim["ConnectivityNode.ConnectivityNodeContainer"], end_vlevel_id))

        cc_list.append([cim.cnAcline1, start_vlevel_id, Literal(start_busbar_name + "-" + str(voltage_kV) + "kV"), base_volt])
        cc_list.append([cim.cnAcline2, end_vlevel_id, Literal(end_busbar_name + "-" + str(voltage_kV) + "kV"), base_volt])


        term1 = create_terminal(cim_version, "term1-acLineSegment-" + line_acline_name, cim.NewAcLine, cim.cnAcline1, 1,
                                graph, cim, base)
        term2 = create_terminal(cim_version, "term2-acLineSegment-" + line_acline_name, cim.NewAcLine, cim.cnAcline2, 2,
                                graph, cim, base)

        if (input_branch[input_parameters.get("input_left_status_name")][num]) == input_switch_name[0]:
            connected_count = connected_count + 1
            normal_open = "false"
            connected_breaker.append([cim.cnAcline1, start_cn_id])
        elif input_branch[input_parameters.get("input_left_status_name")][num] == input_switch_name[1]:
            disconnected_count = disconnected_count + 1
            normal_open = "true"

        switch1 = create_switch(cim_version, input_parameters.get("IMS_cim_type"), "breaker1-acLineSegment-" + line_acline_name, base_volt,
                                normal_open,
                                cim.cnAcline1, start_cn_id, start_vlevel_id, graph, cim, base)

        if input_branch[input_parameters.get("input_status_na_name")][num] == input_switch_name[0]:
            connected_count = connected_count + 1
            normal_open = "false"
            connected_breaker.append([cim.cnAcline2, end_cn_id])
        elif input_branch[input_parameters.get("input_status_na_name")][num] == input_switch_name[1]:
            disconnected_count = disconnected_count + 1
            normal_open = "true"

        switch2 = create_switch(cim_version, input_parameters.get("IMS_cim_type"), "breaker2-acLineSegment-" + line_acline_name, base_volt,
                                normal_open,
                                cim.cnAcline2, end_cn_id, end_vlevel_id, graph, cim, base)

    return [str(len(input_branch)), str(connected_count), str(disconnected_count), connected_aclines, connected_breaker, cc_list]


def create_substations(input_bus, graph, cim, base, sub_reg_id, dict_vlevel, input_parameters):
    new_sub = []

    for num in range(len(input_bus)):
        busbar_name = 'busbar-' + str(input_bus[input_parameters.get("input_nodes_name")][num])
        vlevel_id = dict_vlevel.get(busbar_name)

        if (vlevel_id, cim["VoltageLevel.Substation"], None) in graph:
            pass
        else:
            # create substation
            sub_id = underscore_uuid()
            cim.NewSub = URIRef(base + str(sub_id))
            graph.add((cim.NewSub, RDF.type, cim.Substation))
            graph.add((cim.NewSub, cim["IdentifiedObject.name"],
                       Literal("Substation-" + str(input_bus[input_parameters.get("input_nodes_name")][num]))))
            graph.add((cim.NewSub, cim["Substation.Region"], sub_reg_id))
            graph.add((vlevel_id, cim["VoltageLevel.Substation"], cim.NewSub))

            new_sub.append("Substation-" + str(input_bus[input_parameters.get("input_nodes_name")][num]))

    return new_sub


def create_terminal(cim_version, terminal_name, cond_equipment, conn_node, seq_n, graph, cim, base, connected='true'):
    terminal_id = underscore_uuid()
    cim.NewTerminal = URIRef(base + str(terminal_id))
    graph.add((cim.NewTerminal, RDF.type, cim.Terminal))
    graph.add((cim.NewTerminal, cim["Terminal.ConductingEquipment"], cond_equipment))
    graph.add((cim.NewTerminal, cim["Terminal.ConnectivityNode"], conn_node))
    graph.add((cim.NewTerminal, cim["IdentifiedObject.name"], Literal(terminal_name)))

    if "cim17" in cim_version:
        graph.add((cim.NewTerminal, cim["ACDCTerminal.connected"], Literal(connected)))
        graph.add((cim.NewTerminal, cim["ACDCTerminal.sequenceNumber"], Literal(seq_n)))
        graph.add(
            (cim.NewTerminal, cim["Terminal.phases"], URIRef("http://iec.ch/TC57/2016/CIM-schema-cim17#PhaseCode.ABC")))

    elif "cim15" in cim_version:
        graph.add((cim.NewTerminal, cim["Terminal.connected"], Literal(connected)))
        graph.add((cim.NewTerminal, cim["Terminal.sequenceNumber"], Literal(seq_n)))

    return cim.NewTerminal


def create_trafo(cim_version, input_trafo, graph, cim, base, sub_reg_id, dict_vlevel, dict_cn, dict_volt, input_parameters, input_switch_name):
    connected_count = 0
    disconnected_count = 0
    substations = []
    connected_trafo = []
    connected_breaker = []
    cc_list = []

    for num in range(len(input_trafo)):
        trafo_name = 'trafo-' + str(input_trafo[input_parameters.get("input_nodes_name")][num]) + "-" + str(
            input_trafo[input_parameters.get("input_right_nodes_name")][num])
        substation_name = 'substation-' + str(input_trafo[input_parameters.get("input_nodes_name")][num]) + "-" + str(
            input_trafo[input_parameters.get("input_right_nodes_name")][num])
        substations.append(substation_name)

        start_busbar_name = 'busbar-' + str(input_trafo[input_parameters.get("input_nodes_name")][num])
        end_busbar_name = 'busbar-' + str(input_trafo[input_parameters.get("input_right_nodes_name")][num])

        start_vlevel_id = dict_vlevel.get(start_busbar_name)
        end_vlevel_id = dict_vlevel.get(end_busbar_name)

        start_cn_id = dict_cn.get(start_busbar_name)
        end_cn_id = dict_cn.get(end_busbar_name)

        ratedU1 = input_trafo[input_parameters.get("input_base_voltage_name")][num]
        base_volt1 = dict_volt.get(ratedU1)
        ratedU2 = ratedU1 / float(input_trafo[input_parameters.get("input_trans_ratio")][num])

        volt_2 = float(input_trafo[input_parameters.get("input_right_base_voltage_name")][num])
        step_voltage = (ratedU2 - volt_2) * 100 / volt_2
        base_volt2 = dict_volt.get(volt_2)

        x = input_trafo[input_parameters.get("react_name")][num]
        r = input_trafo[input_parameters.get("res_name")][num]

        base_mva = input_trafo[input_parameters.get("base_mva_name")][num]

        trafo_id = underscore_uuid()
        cim.NewTrafo = URIRef(base + str(trafo_id))
        trafo_end_id1 = underscore_uuid()
        cim.NewTrafoEnd1 = URIRef(base + str(trafo_end_id1))
        trafo_end_id2 = underscore_uuid()
        cim.NewTrafoEnd2 = URIRef(base + str(trafo_end_id2))
        tap_id = underscore_uuid()
        cim.NewTap = URIRef(base + str(tap_id))

        # create substation
        subregion = sub_reg_id
        sub_id = underscore_uuid()
        cim.NewSub = URIRef(base + str(sub_id))
        graph.add((cim.NewSub, RDF.type, cim.Substation))
        graph.add((cim.NewSub, cim["IdentifiedObject.name"], Literal(substation_name)))
        graph.add((cim.NewSub, cim["Substation.Region"], subregion))

        # add substation to voltage levels
        graph.add((start_vlevel_id, cim["VoltageLevel.Substation"], cim.NewSub))
        graph.add((end_vlevel_id, cim["VoltageLevel.Substation"], cim.NewSub))

        # create transformer
        graph.add((cim.NewTrafo, RDF.type, cim.PowerTransformer))
        graph.add((cim.NewTrafo, cim["IdentifiedObject.name"], Literal(trafo_name)))
        graph.add((cim.NewTrafo, cim["Equipment.EquipmentContainer"], cim.NewSub))
        if "cim17" in cim_version:
            graph.add((cim.NewTrafo, cim["Equipment.aggregate"], Literal("false")))

        # create transformer end 1
        graph.add((cim.NewTrafoEnd1, RDF.type, cim.PowerTransformerEnd))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.PowerTransformer"], cim.NewTrafo))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.g"], Literal(0)))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.b"], Literal(0)))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.r0"], Literal(0)))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.x0"], Literal(0)))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.g0"], Literal(0)))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.b0"], Literal(0)))

        if "cim17" in cim_version:
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.rground"], Literal(0)))
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.xground"], Literal(0)))
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.phaseAngleClock"], Literal(0)))
            graph.add((cim.NewTrafoEnd1, cim["TransformerEnd.grounded"], Literal("true")))
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.connectionKind"],
                       URIRef("http://iec.ch/TC57/2012/CIM-schema-cim16#WindingConnection.D")))

        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.ratedU"], Literal(ratedU1)))
        graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.ratedS"], Literal(base_mva)))
        graph.add((cim.NewTrafoEnd1, cim["TransformerEnd.BaseVoltage"], base_volt1))

        # create transformer end 2
        graph.add((cim.NewTrafoEnd2, RDF.type, cim.PowerTransformerEnd))

        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.PowerTransformer"], cim.NewTrafo))
        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.g"], Literal(0)))
        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.b"], Literal(0)))
        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.r0"], Literal(0)))
        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.x0"], Literal(0)))
        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.g0"], Literal(0)))
        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.b0"], Literal(0)))

        if "cim17" in cim_version:
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.rground"], Literal(0)))
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.xground"], Literal(0)))
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.phaseAngleClock"], Literal(1)))
            graph.add((cim.NewTrafoEnd2, cim["TransformerEnd.grounded"], Literal("true")))
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.connectionKind"],
                       URIRef("http://iec.ch/TC57/2012/CIM-schema-cim16#WindingConnection.Y")))

        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.ratedU"], Literal(ratedU2)))
        graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.ratedS"], Literal(base_mva)))
        graph.add((cim.NewTrafoEnd2, cim["TransformerEnd.BaseVoltage"], base_volt2))
        graph.add((cim.NewTrafoEnd2, cim["TransformerEnd.RatioTapChanger"], cim.NewTap))

        if ratedU1 > ratedU2:
            graph.add((cim.NewTrafoEnd1, cim["IdentifiedObject.name"], Literal(trafo_name + "-end1")))
            graph.add((cim.NewTrafoEnd1, cim["TransformerEnd.endNumber"], Literal(1)))
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.r"], Literal(r)))
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.x"], Literal(x)))
            graph.add((cim.NewTrafoEnd2, cim["IdentifiedObject.name"], Literal(trafo_name + "-end2")))
            graph.add((cim.NewTrafoEnd2, cim["TransformerEnd.endNumber"], Literal(2)))
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.r"], Literal(0)))
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.x"], Literal(0)))
        else:
            graph.add((cim.NewTrafoEnd2, cim["IdentifiedObject.name"], Literal(trafo_name + "-end1")))
            graph.add((cim.NewTrafoEnd2, cim["TransformerEnd.endNumber"], Literal(1)))
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.r"], Literal(r)))
            graph.add((cim.NewTrafoEnd2, cim["PowerTransformerEnd.x"], Literal(x)))
            graph.add((cim.NewTrafoEnd1, cim["IdentifiedObject.name"], Literal(trafo_name + "-end2")))
            graph.add((cim.NewTrafoEnd1, cim["TransformerEnd.endNumber"], Literal(2)))
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.r"], Literal(0)))
            graph.add((cim.NewTrafoEnd1, cim["PowerTransformerEnd.x"], Literal(0)))

        graph.add((cim.NewTap, RDF.type, cim["RatioTapChanger"]))
        graph.add((cim.NewTap, cim["IdentifiedObject.name"], Literal(trafo_name + "-ratio-tap-changer")))
        graph.add((cim.NewTap, cim["TapChanger.normalStep"], Literal(1)))
        graph.add((cim.NewTap, cim["TapChanger.neutralStep"], Literal(0)))
        graph.add((cim.NewTap, cim["TapChanger.lowStep"], Literal(0)))
        graph.add((cim.NewTap, cim["TapChanger.highStep"], Literal(1)))
        graph.add((cim.NewTap, cim["RatioTapChanger.stepVoltageIncrement"], Literal(step_voltage)))
        graph.add((cim.NewTap, cim["TapChanger.ltcFlag"], Literal("true")))
        graph.add((cim.NewTap, cim["TapChanger.neutralU"], Literal(volt_2)))
        graph.add((cim.NewTap, cim["RatioTapChanger.TransformerEnd"], cim.NewTrafoEnd2))

        # create intermediate connectivity nodes
        cn_trafo_1 = underscore_uuid()  # between trafo and breaker
        cim.cnTrafo1 = URIRef(base + str(cn_trafo_1))
        cn_trafo_2 = underscore_uuid()  # between trafo and breaker
        cim.cnTrafo2 = URIRef(base + str(cn_trafo_2))

        # add connectivity nodes
        graph.add((cim.cnTrafo1, RDF.type, cim.ConnectivityNode))
        graph.add((cim.cnTrafo1, cim["IdentifiedObject.name"], Literal("CN-" + trafo_name + "-switch1")))
        graph.add((cim.cnTrafo1, cim["ConnectivityNode.ConnectivityNodeContainer"], start_vlevel_id))
        graph.add((cim.cnTrafo2, RDF.type, cim.ConnectivityNode))
        graph.add((cim.cnTrafo2, cim["IdentifiedObject.name"], Literal("CN-" + trafo_name + "-switch2")))
        graph.add((cim.cnTrafo2, cim["ConnectivityNode.ConnectivityNodeContainer"], end_vlevel_id))

        connected_trafo.append([cim.cnTrafo1, cim.cnTrafo2])

        term1 = create_terminal(cim_version, "term1-" + trafo_name, cim.NewTrafo, cim.cnTrafo1, 1, graph, cim, base)
        term2 = create_terminal(cim_version, "term2-" + trafo_name, cim.NewTrafo, cim.cnTrafo2, 2, graph, cim, base)

        cc_list.append([cim.cnTrafo1, start_vlevel_id, Literal(start_busbar_name + "-" + str(ratedU1) + "kV"), base_volt1])
        cc_list.append([cim.cnTrafo2, end_vlevel_id, Literal(end_busbar_name + "-" + str(ratedU2) + "kV"), base_volt2])


        graph.add((cim.NewTrafoEnd1, cim["TransformerEnd.Terminal"], term1))
        graph.add((cim.NewTrafoEnd2, cim["TransformerEnd.Terminal"], term2))

        if input_trafo[input_parameters.get("input_left_status_name")][num] == input_switch_name[0]:
            connected_count = connected_count + 1
            normal_open = "false"
            connected_breaker.append([cim.cnTrafo1, start_cn_id])
        else:
            disconnected_count = disconnected_count + 1
            normal_open = "true"

        switch1 = create_switch(cim_version, input_parameters.get("IMS_cim_type"), "breaker-" + trafo_name, base_volt1, normal_open,
                                cim.cnTrafo1, start_cn_id, start_vlevel_id, graph, cim, base)

        if input_trafo[input_parameters.get("input_status_na_name")][num] == input_switch_name[0]:
            connected_count = connected_count + 1
            normal_open = "false"
            connected_breaker.append([cim.cnTrafo2, end_cn_id])
        else:
            disconnected_count = disconnected_count + 1
            normal_open = "true"

        switch2 = create_switch(cim_version, input_parameters.get("IMS_cim_type"), "breaker2-" + trafo_name, base_volt2, normal_open,
                                cim.cnTrafo2, end_cn_id, end_vlevel_id, graph, cim, base)

    return [str(len(input_trafo)), str(connected_count), str(disconnected_count), str(substations),connected_trafo, connected_breaker, cc_list]


def create_disconnector(cim_version, input_cong, graph, cim, base, sub_reg_id, dict_vlevel, dict_cn,
                        dict_volt, input_parameters, input_switch_name):
    connected_count = 0
    disconnected_count = 0
    substations = []
    connected_breaker = []

    for num in range(len(input_cong)):
        cong_name = "disconnector-" + str(input_cong[input_parameters.get("input_nodes_name")][num]) + "-" + str(
            input_cong[input_parameters.get("input_right_nodes_name")][num])
        substation_name = "substation-" + str(input_cong[input_parameters.get("input_nodes_name")][num]) + "-" + str(
            input_cong[input_parameters.get("input_right_nodes_name")][num])
        substations.append(substation_name)

        start_busbar_name = 'busbar-' + str(input_cong[input_parameters.get("input_nodes_name")][num])
        end_busbar_name = 'busbar-' + str(input_cong[input_parameters.get("input_right_nodes_name")][num])

        start_vlevel_id = dict_vlevel.get(start_busbar_name)
        end_vlevel_id = dict_vlevel.get(end_busbar_name)

        start_cn_id = dict_cn.get(start_busbar_name)
        end_cn_id = dict_cn.get(end_busbar_name)

        voltage_kV = input_cong[input_parameters.get("input_base_voltage_name")][num]
        base_volt = dict_volt.get(voltage_kV)

        # create substation
        sub_id = underscore_uuid()
        cim.NewSub = URIRef(base + str(sub_id))
        graph.add((cim.NewSub, RDF.type, cim.Substation))
        graph.add((cim.NewSub, cim["IdentifiedObject.name"], Literal(substation_name)))
        graph.add((cim.NewSub, cim["Substation.Region"], sub_reg_id))

        # add substation to voltage levels
        graph.add((start_vlevel_id, cim["VoltageLevel.Substation"], cim.NewSub))
        graph.add((end_vlevel_id, cim["VoltageLevel.Substation"], cim.NewSub))

        if input_cong[input_parameters.get("input_left_status_name")][num] == input_switch_name[0] and \
                input_cong[input_parameters.get("input_status_na_name")][num] == input_switch_name[0]:
            connected_count = connected_count + 1
            normal_open = "false"
            connected_breaker.append([start_cn_id, end_cn_id])
        else:
            disconnected_count = disconnected_count + 1
            normal_open = "true"

        disconnector = create_switch(cim_version, input_parameters.get("congiuntore_cim_type"), cong_name, base_volt, normal_open,
                                     start_cn_id, end_cn_id, end_vlevel_id, graph, cim, base)

    return [str(len(input_cong)), str(connected_count), str(disconnected_count), str(substations), connected_breaker]


# ---------------------Tranformation function ------------------------------#
def transformation_function(input_file, output_folder, out_type, input_type, corr_id, xslt_file,
                            cim_namespace, logger, input_parameters, input_nodes_names,
                            input_lines_name, input_switch_name):


    # importing input files
    logger.info("Input network described using PGM data model")
    logger.info("Start importing input files: " + input_file)
    input_nodes, input_loads, input_ext_net, input_aclines, input_cong, input_trafos = import_json_file_pgm(
        input_file, logger, input_parameters, input_nodes_names, input_lines_name)

    logger.info("End importing input files")


    # defining RDF XML graph
    logger.info("Start defining RDF XML graph")
    graph, base, cim = define_rdf_xml_graph(cim_namespace, output_folder)
    logger.info("End defining RDF XML graph")

    # rdflib transformation
    logger.info("Start rdf lib transformation")
    connected_aclines, connected_trafo, connected_breaker, cc_list = rdflib_transform(out_type, input_nodes, input_ext_net, input_loads, input_aclines, input_trafos,
                     input_cong, input_type, graph, cim, base, logger, input_parameters, input_switch_name)
    logger.info("End rdf lib transformation")

    # topological processor
    logger.info("Start topological processor")
    islands = topLibrary.createTopology_networkX(graph, cim,  base, logger, connected_aclines, connected_trafo, connected_breaker, cc_list)
    logger.info("End topological processor")

    # cgmes transformation
    if "cgmes" in out_type:
        logger.info("Start CIM 2 CGMES transformation")
        out_graph = cim2cgmes_lib.cim2cgmes(graph, cim, logger)
        logger.info("End CIM 2 CGMES transformation")

    else:
        out_graph = graph


    # xml serialization
    logger.info("Start xml serialization")
    xml_graph = out_graph.serialize(format='xml', encoding="utf-8")
    logger.info("End xml serialization")

    output_file = output_folder + "/output_" + corr_id + ".xml"

    # xslt transformation
    logger.info("Start xslt transformation")
    xsltLibrary.xslt_transform_string(xml_graph, xslt_file, output_file)
    logger.info("End xslt transformation")


# ---------------------Network definition functions ------------------------------#
def import_json_file_pgm(input_file, logger, input_parameters, input_nodes_names, input_lines_name):

    json_data = import_json_data(input_file, "input", ignore_extra=True)

    ### IMPORT LINES ###
    if input_lines_name[0] in json_data:
        logger.info("Found "+ str(len(json_data[input_lines_name[0]])) +  " lines in input json")
        lines = pd.DataFrame(json_data[input_lines_name[0]]) #create line table
        lines[input_parameters.get("length_name")] = 1 #set line length to default
        # lines C1 from F to S
        lines[input_parameters.get("bch_name")] = lines[input_parameters.get("bch_name")] * 2 * 3.141592654 * 50
        lines[input_parameters.get("bch_name_0")] = lines[input_parameters.get("bch_name_0")] * 2 * 3.141592654 * 50

    else: raise Exception("No line found in input json")


    ### IMPORT NODES ###
    if input_parameters.get("input_nodes_name") in json_data:
        logger.info("Found " + str(len(json_data[input_parameters.get("input_nodes_name")])) + " buses in input json")
        bus = pd.DataFrame(json_data[input_parameters.get("input_nodes_name")]) #create bus table
    else: raise Exception("No bus found in input json")

    ### IMPORT SOURCES ###
    if input_nodes_names[0] in json_data:
        logger.info("Found " + str(len(json_data[input_nodes_names[0]])) + " source in input json")
        source = pd.DataFrame(json_data[input_nodes_names[0]])[[input_parameters.get("input_nodes_name")]] #source table
        source[input_parameters.get("input_type_nodes_name")] = 'Ref' #add type ref
        # add nominal voltage to source and modify units
        source = source.merge(bus, left_on=input_parameters.get("input_nodes_name"),
                              right_on=input_parameters.get("input_id_name"), how='left').drop(columns=input_parameters.get("input_id_name"))
        source[input_parameters.get("input_base_voltage_name")] = source[input_parameters.get("input_base_voltage_name")] / 1000  # from V to kV
    else: raise Exception("No source found in input json")

    ### IMPORT SYMMETRICAL LOADS ###
    if input_nodes_names[1] in json_data:
        logger.info("Found " + str(len(json_data[input_nodes_names[1]])) + " symmetrical loads in input json")
        #load table, add type PQ
        load = pd.DataFrame(json_data[input_nodes_names[1]])[[input_parameters.get("input_nodes_name"),
                                                              input_parameters.get("active_power_name"),
                                                              input_parameters.get("reactive_power_name")]]
        load[input_parameters.get("input_type_nodes_name")] = 'PQ'
        # add nominal voltage to loads and modify units
        load = load.merge(bus, left_on=input_parameters.get("input_nodes_name"),
                          right_on=input_parameters.get("input_id_name"), how='left').drop(columns=input_parameters.get("input_id_name"))
        load[input_parameters.get("input_base_voltage_name")] = load[input_parameters.get("input_base_voltage_name")] / 1000  # from V to kV
        load[input_parameters.get("active_power_name")] = load[input_parameters.get("active_power_name")] / 1000000  # from W to MW
        load[input_parameters.get("reactive_power_name")] = load[input_parameters.get("reactive_power_name")] / 1000000  # from VAR to MVAR
    else: logger.info("No symmetrical load found in input json")

    ### IMPORT SYMMETRICAL GENERATORS ###
    if input_nodes_names[2] in json_data:
        logger.info("Found " + str(len(json_data[input_nodes_names[2]])) + " symmetrical generators in input json")
        #generator table, add type PQ and change P and Q to negative
        generators = pd.DataFrame(json_data[input_nodes_names[2]])[[input_parameters.get("input_nodes_name"),
                                                                    input_parameters.get("active_power_name"),
                                                                    input_parameters.get("reactive_power_name")]]
        generators[input_parameters.get("input_type_nodes_name")] = 'PQ'
        # add nominal voltage to loads and modify units
        generators = generators.merge(bus, left_on=input_parameters.get("input_nodes_name"),
                          right_on=input_parameters.get("input_id_name"), how='left').drop(columns=input_parameters.get("input_id_name"))
        generators[input_parameters.get("input_base_voltage_name")] = generators[input_parameters.get("input_base_voltage_name")] / 1000  # from V to kV
        generators[input_parameters.get("active_power_name")] = - generators[input_parameters.get("active_power_name")] / 1000000  # from W to MW and convert to negative
        generators[input_parameters.get("reactive_power_name")] = - generators[input_parameters.get("reactive_power_name")] / 1000000  # from VAR to MVAR and convert to negative

        #concatenate loads and generators
        load = pd.concat([load, generators], axis=0).reset_index()
    else: logger.info("No symmetrical generator found in input json")

    bus[input_parameters.get("input_nodes_name")] = bus[input_parameters.get("input_id_name")]
    
    ### ADD BASE VOLTAGES TO LINES ###
    bus[input_parameters.get("input_base_voltage_name")] = bus[input_parameters.get("input_base_voltage_name")] / 1000
    lines[input_parameters.get("input_nodes_name")] = lines[input_parameters.get("input_left_nodes_name")]
    lines = lines.merge(bus[[input_parameters.get("input_nodes_name"), input_parameters.get("input_base_voltage_name")]], left_on=input_parameters.get("input_left_nodes_name"),
                        right_on=input_parameters.get("input_nodes_name")).drop(columns=[input_parameters.get("input_nodes_name")+"_x", input_parameters.get("input_nodes_name")+"_y"])
    lines = lines.merge(bus[[input_parameters.get("input_nodes_name"), input_parameters.get("input_base_voltage_name")]], left_on=input_parameters.get("input_right_nodes_name"),
                        right_on=input_parameters.get("input_nodes_name")).drop(columns=[input_parameters.get("input_nodes_name")])
    lines = lines.rename(columns={input_parameters.get("input_base_voltage_name")+"_x":input_parameters.get("input_base_voltage_name"),
                                  input_parameters.get("input_base_voltage_name")+"_y":input_parameters.get("input_right_base_voltage_name"),
                                  input_parameters.get("input_left_nodes_name"):input_parameters.get("input_nodes_name")})


    ### IMPORT TRANSFORMERS ###
    if input_lines_name[2] in json_data:
        logger.info("Found " + str(len(json_data[input_lines_name[2]])) + " transformers in input json")
        #create empty datasets for trafos and conjunctors
        trafos = pd.DataFrame(json_data[input_lines_name[2]])

        trafos[input_parameters.get("bch_name")] = 0
        trafos[input_parameters.get("res_name")]=0
        trafos[input_parameters.get("react_name")]=0
        trafos = trafos.rename(columns={input_parameters.get("input_left_nodes_name"):input_parameters.get("input_nodes_name"),
                                        "u1":input_parameters.get("input_base_voltage_name")})
        trafos[input_parameters.get("input_base_voltage_name")] = trafos[input_parameters.get("input_base_voltage_name")] / 1000
        trafos[input_parameters.get("input_right_base_voltage_name")] = trafos["u2"] / 1000
        trafos[input_parameters.get("input_trans_ratio")] = trafos[input_parameters.get("input_base_voltage_name")]/trafos[input_parameters.get("input_right_base_voltage_name")]

    else:
        logger.info("No transformer found in input json")
        trafos = pd.DataFrame(columns=lines.columns)

    if input_lines_name[1] in json_data:
        logger.error("Found " + str(len(json_data[input_lines_name[1]])) + " links in input json; links non handled in the transformation!")

    else:
        logger.info("No link found in input json")
        cong = pd.DataFrame(columns=lines.columns)

    return bus, load, source, lines, cong, trafos
