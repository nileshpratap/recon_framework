from pathlib import Path
import boto3
import configparser


class ConfigUtils(object):

    # Method to create Config to access all properties from Config.properties file.
    def getConfig(self):
        config = configparser.ConfigParser()
        config.read("Config.properties")
        return config
