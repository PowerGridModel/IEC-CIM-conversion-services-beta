from rdflib_library import PGM_to_cim as cimLibrary
from rdflib_library import cim_to_PGM as pgmLibrary


class ExecutorService():
    def __init__(self, main, logger, in_file, out_folder, corr_id, in_type, out_type, input_parameters, input_nodes_names,
                 input_lines_names, input_switch_names):
        self.main = main
        self.logger = logger
        # -------------------BEGIN: Cfg ------------------------------#
        self.in_file = in_file
        self.in_type = in_type
        self.out_type = out_type
        self.out_folder = out_folder
        self.c_id = corr_id
        self.input_parameters = input_parameters
        self.input_nodes_names = input_nodes_names
        self.input_lines_names = input_lines_names
        self.input_switch_names = input_switch_names
        # ---------------------END: Cfg ------------------------------#


    def run(self):
        self.logger.info("PgmTransform: Service started!!")
        self.launch_service()

    def launch_service(self):

        # ---------------------BEGIN: setting CIM version ------------------------------#
        if self.in_type not in ["cim15-301", "cim17-301", "cim15-cgmes", "cim17-cgmes", "pgm-json"]:
            raise Exception(self.in_type + " is not an handled input type!")

        if self.out_type not in ["cim15-301", "cim17-301", "cim15-cgmes", "cim17-cgmes", "pgm-json"]:
            raise Exception(self.out_type + " is not an handled output type!")

        elif "cim17" in self.out_type:
            xslt_file = 'rdflib_library/cim17_adapter.xslt'
            cim_namespace = 'http://iec.ch/TC57/2016/CIM-schema-cim17#'

        elif "cim15" in self.out_type:
            xslt_file = 'rdflib_library/cim15_adapter.xslt'
            cim_namespace = 'http://iec.ch/TC57/2010/CIM-schema-cim15#'

        if ("cim" in self.in_type and "cim" in self.out_type) | ("json" in self.in_type and "json" in self.out_type):
            raise Exception(self.in_type + " transform to " + self.out_type + " is not handled!")

        # launch python script for rdflib model2modeltransformation
        try:
            self.logger.info("Output type " + self.out_type)

            if "json" in self.out_type:
                if "cim17" in self.in_type:
                    cim_namespace = 'http://iec.ch/TC57/2016/CIM-schema-cim17#'
                elif "cim15" in self.in_type:
                    cim_namespace = 'http://iec.ch/TC57/2010/CIM-schema-cim15#'

                output = pgmLibrary.transformation_function(self.in_file, self.out_folder, self.logger, cim_namespace, self.input_parameters, self.c_id)


            else:
                output = cimLibrary.transformation_function(self.in_file, self.out_folder, self.out_type, self.in_type, self.c_id,
                 xslt_file, cim_namespace, self.logger, self.input_parameters, self.input_nodes_names, self.input_lines_names, self.input_switch_names)


        except Exception as err:
            self.logger.error(err)
            self.logger.info("test PgmTransform")

