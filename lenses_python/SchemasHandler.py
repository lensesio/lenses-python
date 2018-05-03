from requests import *
from lenses_python.ReadConfigFile import ReadConfigFile

class SchemasHandler:

    def __init__(self, url, username, password, token):
        self.url = url
        self.username = username
        self.password = password
        self.token = token
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token}

    def ListAllSubjects(self):
        """
        List all available subjects
        GET /api/proxy-sr/subjects

        :return:
        """
        url = self.url+"/api/proxy-sr/subjects"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code,response.text))
        else:
            return response.json()

    def ListVersionsSubj(self, subject):
        """
        List all versions of a particular subject
        GET /api/proxy-sr/subjects/(string: subject)/versions
        :param subject:
        :return:
        """
        url = self.url+"/api/proxy-sr/subjects/"+subject+"/versions"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code,response.text))
        else:
            return response.json()

    def DeleteSubj(self, subject):
        """
        Delete a subject and associated compatibility level
        DELETE /api/proxy-sr/subjects/(string: subject)
        :param subject:
        :return:
        """
        url = self.url+"/api/proxy-sr/subjects/"+subject
        response = delete(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code,response.text))

    def GetSchemaById(self, subjid):
        """
        Get the schema for a particular subject id
        GET /api/proxy-sr/schemas/ids/{int: id}
        :param subjid:
        :return:
        """
        url = self.url+"/api/proxy-sr/schemas/ids/"+subjid
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code,response.text))
        return response.json()

    def GetSchemaByVer(self, subject, verid):
        """
        if verid = "latest" get the last version
        Get the schema at a particular version
        GET /api/proxy-sr/subjects/(string: subject)/versions/(versionId: version)
        :param subject:
        :param verid:
        :return:
        """
        url = self.url+"/api/proxy-sr/subjects/"+subject+"/versions/"+verid
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def RegisterNewSchema(self, subject, schema_json, filename):
        """
         Register a new schema under a particular subject
         POST api/proxy-sr/subjects/(string: subject)/versions
        :param subject:
        :param schema_json: Example of it {'schema': '{"type":"record","name":"reddit_post_key",
        "namespace":"com.landoop.social.reddit.post.key","fields":[{"name":"subreddit_id","type":"string"}]}'}
        :param filename:
        :return:{'id': <id>}
        """
        if subject == "" and schema_json == "" and filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("schema", None) is not None and temp_dict.get("subject", None) is not None:
                subject = temp_dict["subject"]
                schema_json = temp_dict["schema"]
            else:
                raise Exception("In file there aren't sections subject and schema\n")
        elif filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("schema", None) is not None:
                # The option name of option must be config and have json format
                schema_json = temp_dict["schema"]
            else:
                raise Exception("In file there isn't section schema\n")
        url = self.url+"/api/proxy-sr/subjects/"+subject+"/versions"
        response = post(url, headers=self.default_headers, json=schema_json)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def DeleteSchemaByVersion(self, subject, version):
        """
        Delete a particular version of a subject
        DELETE /api/proxy-sr/subjects/(string: subject)/versions/(versionId: version)
        :param subject:
        :param version:string of int
        :return: if succeed return 1
        """
        url = self.url+"/api/proxy-sr/subjects/"+subject+"/versions/"+version
        response = delete(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def UpdateGlobalCompatibility(self, compatibility, filename):
        """
        Update global compatibility level
        PUT /api/proxy-sr/config
        :param compatibility: For example {'compatibility': 'BACKWARD'}
        :param filename:
        :return:
        """
        if filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("compatibility", None) is not None:
                # The option name of option must be config and have json format
                compatibility = temp_dict["compatibility"]
            else:
                raise Exception("In file there isn't section compatibility\n")
        url = self.url+"/api/proxy-sr/config"
        response = put(url, headers=self.default_headers, json=compatibility)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def GetGlobalCompatibility(self):
        """
        Get global compatibility level
        GET /api/proxy-sr/config
        :return:
        """
        url = self.url+"/api/proxy-sr/config"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def ChangeCompatibility(self, subject, compatibility, filename):
        """
        Change compatibility level of a subject
        PUT /api/proxy-sr/config/(string: subject)
        :param subject:
        :param compatibility: For example {'compatibility': 'BACKWARD'}
        :param filename:
        :return:
        """
        if subject == "" and compatibility == "" and filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("subject", None) is not None and temp_dict.get("compatibility", None) is not None:
                subject = temp_dict["subject"]
                compatibility = temp_dict["compatibility"]
            else:
                raise Exception("In file there aren't sections subject and compatibility\n")
        if filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("compatibility", None) is not None:
                # The option name of option must be config and have json format
                compatibility = temp_dict["compatibility"]
            else:
                raise Exception("In file there isn't section compatibility\n")
        url = self.url+"/api/proxy-sr/config/"+subject
        response = put(url, headers=self.default_headers, json=compatibility)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        response.json()

    def GetCompatibility(self, subject):
        """
        Get compatibility level of a subject
        GET /api/proxy-sr/config/(string: subject)
        :param subject:
        :return:
        """
        url = self.url+"/api/proxy-sr/config/"+subject
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()





