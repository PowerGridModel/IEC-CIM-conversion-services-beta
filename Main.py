import os
import logging
import logging.handlers
from ExecutorService import ExecutorService
import argparse
import configparser
import json

class Main:
    def __init__(self):
        # -------------------BEGIN: Cfg ------------------------------#
        # read configuration file
        cfg = configparser.ConfigParser()
        with open("config.cfg") as f:
            cfg.read_file(f)

        service_cfg = cfg["serviceParameters"]
        input_cfg = cfg["inputFileParameters"]

        #read input parameters from command line
        parser = argparse.ArgumentParser(description='Parse input')
        parser.add_argument('--input_path',
                            help="Input file path")
        parser.add_argument('--in_type',
                            help="cim15-301, cim15-cgmes, cim17-301, cim17-cgmes, pgm-json")
        parser.add_argument('--out_type',
                            help="cim15-301, cim15-cgmes, cim17-301, cim17-cgmes, pgm-json")
        parser.add_argument('--corr_id',
                            help="Correlation ID")
        parser.add_argument('--output_folder',
                            help="output folder path", default=service_cfg.get("default_output_folder"))

        args = parser.parse_args()
        self.in_type = args.in_type
        self.out_type = args.out_type
        self.c_id = args.corr_id
        self.in_file = args.input_path
        self.out_folder = args.output_folder
        self.log_file = service_cfg.get("default_log_file")
        self.input_parameters = input_cfg
        self.input_nodes_names = json.loads(cfg.get("inputFileParameters", "types_nodes"))
        self.input_lines_names = json.loads(cfg.get("inputFileParameters", "types_lines"))
        self.input_switch_names = json.loads(cfg.get("inputFileParameters", "types_switch"))

        # ---------------------END: Cfg ------------------------------#

        # ---------------------BEGIN: Logger ------------------------------#
        self.logger = logging.getLogger(__name__)
        # 10 MB log
        fh = logging.handlers.RotatingFileHandler(self.log_file, mode="w", maxBytes=10000000, backupCount=1)
        formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.setLevel("DEBUG")
        self.logger.addHandler(fh)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        # ---------------------END: Logger - -----------------------------


    def start(self):
        # ---------------------BEGIN: Start service ------------------#
        service_executor = ExecutorService(self, self.logger, self.in_file, self.out_folder, self.c_id,
                                           self.in_type, self.out_type, self.input_parameters,
                                           self.input_nodes_names, self.input_lines_names, self.input_switch_names)
        service_executor.run()
        # ---------------------END: Start service --------------------#


x = Main()

try:
    x.start()
except KeyboardInterrupt:
    x.logger.info(x.serviceName + " quitting")
    os._exit(1)


