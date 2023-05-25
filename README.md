<!--
SPDX-FileCopyrightText: 2022 Contributors to the Power Grid Model project <dynamic.grid.calculation@alliander.com>

SPDX-License-Identifier: MPL-2.0
-->

# IEC-CIM-conversion-services (Beta)
This repo contains the IECCimToPGM and PGMToIECCim conversion services that [RSE S.p.A.](https://www.rse-web.it/) developed. They developed these services to integrate Power Grid Model into their DSO platform. Since there is a broader interest in IECCimToPGM and PGMToIECCim conversion services they will open source these services and contribute them to the Power Grid Model project. In the future, these services may be integrated into [power-grid-model-io](https://github.com/PowerGridModel/power-grid-model-io). 

# Notable contributors
Our gratitude goes to the following contributors who have worked (and are still working) on these services before it became open source:
- Gabriele Paludetto (RSE)
- Francesca Soldan (RSE)
- Enea Bionda (RSE)


# Conversion services description

This service performs:
- PGM data model transformation to IEC CIM
- IEC CIM transformation to PGM data model.

For the transformation, rdflib python library is exploited.
Before launching the service, required libraries should be installed, using:
### `pip install -r requirements.txt` 


In the project directory, you can run:
### `python Main.py --input inputFilePath --in_type inputType --out_type outputType --corr_id correlationID`
where:
1) inputFilePath: relative path to the input file (e.g. InputTransform/test.json; InputTransform/test.xml)
2) inputType: cim15-301, cim17-301, cim15-cgmes, cim17-cgmes, pgm-json
2) outputType: cim15-301, cim17-301, cim15-cgmes, cim17-cgmes, pgm-json (please note: transformations from IEC CIM to another IEC CIM version are not supported)
3) correlationID: service correlation ID for unique identification (e.g. pgmTestNetwork)

As test command, you can run:
### `python Main.py --input InputTransform/test.json --in_type pgm-json --out_type cim17-301 --corr_id pgmTestNetwork`
### `python Main.py --input InputTransform/test.xml --in_type cim17-301 --out_type pgm-json --corr_id cimTestNetwork`

By default, the output file is saved in the folder OutputTransform. It is possible to specify the output folder in the command, using:
--output_folder outputFolderPath

## PGM to IEC CIM service
The service takes as input the pgm networks in json format. 
Field names are set in config.cfg, together with the acceptable categorical values contained in columns:
- breaker status (1,0)
- node type (source, sym_load, sym_gen)
- branch type (line, link, transformer)

The transformation requires the following steps:
1) Input network is transformed from/to IEC CIM using rdflib functions
2) Topology is calculated using networkx topological processor
3) If cgmes is selected as output type, classes filtering to conform CGMES is performed
4) xslt transformation and xml serialization are performed
5) Output file is saved in default output folder or in the specified one through command line parameter

## IEC CIM to PGM service
The service takes as input IEC CIM networks in RDF/XML format.
Different versions (CIM 15 and CIM 17) are supported.

The output in PGM data model querying the IEC CIM network graph.

## Known bugs/limitations
- For the transformation PGM to IEC CIM: not all PGM elements are supported, but only node, line, sym_load, sym_gen, source, transformer. You are welcome to contribute by supporting more elements, or by adding issues for discussing the addition of elements.
- For the transformation IEC CIM to PGM, not all IEC CIM elements are supported, only the most needed ones. Default values (not directly found in IEC CIM) are set for the transformation from lines and transformers. You are welcome to contribute by supporting more elements, or by adding issues for discussing the addition of elements.






# License
This project is licensed under the Mozilla Public License, version 2.0 - see [LICENSE](LICENSE) for details.

# Contributing
Please read [CODE_OF_CONDUCT](https://github.com/PowerGridModel/.github/blob/main/CODE_OF_CONDUCT.md) and [CONTRIBUTING](https://github.com/PowerGridModel/.github/blob/main/CONTRIBUTING.md) for details on the process 
for submitting pull requests to us.

# Contact
Please read [SUPPORT](https://github.com/PowerGridModel/.github/blob/main/SUPPORT.md) for how to connect and get into contact with the IEC-CIM-conversion-services-beta project.
