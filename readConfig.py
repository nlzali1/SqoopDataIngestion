import json


class ReadConfiguration:
    def read_json(self, config_json_file):
        configfile = open(config_json_file)
        conf = json.load(configfile)
        configfile.close()
        return conf
