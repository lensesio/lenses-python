import configparser
import json

class ReadConfigFile:

    def __init__(self, filename):
        """

        :param filename: the name of config file.
        In this link https://docs.python.org/3/library/configparser.html you can see more about the format of
        configuration file
        """
        self.filename = filename

    def GetJSONs(self):
        """
        The configuration file must have only one section with any name.
        The name of option must be specificated by the use of call where they use it. They are predefined in each class
        :return: a dictionarie with many keys
        """
        config = configparser.ConfigParser()
        try:
            config.read(self.filename)
        except:
            raise Exception("There file with name {}\n".format(self.filename))
        try:
            section = config.sections()[0]
        except:
            raise Exception("There isn't any section in file.\n")
        options = config.options(section)
        json_dict = {}
        for option in options:
            if option in ["config", "compatibility", "schema"]:
                json_dict[option] = json.loads(config.get(section, option))
            else:
                json_dict[option] = config.get(section, option)
        if len(json_dict) == 0:
            raise Exception("File {} doesn't have any data.\n".format(self.filename))
        return json_dict



