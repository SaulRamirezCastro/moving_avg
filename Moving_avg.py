# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 3/2/2022

"""
import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class MovingAvg:

    def __init__(self):
        logger.info("Initialize Tbe Class Moving Avg")

    def _read_yaml_file(self) -> None:
        """Read the Yaml file configuration and set into the class variable
        _yaml_config
        Returns:
            None
        """
        logger.info('Reading config yaml file ')
        path = os.path.dirname(os.path.abspath(__file__))
        yaml_file = f"{path}/config/config.yml"
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if data:
            self._yaml_config = data

    def _read_file(self):
        pass