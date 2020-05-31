from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class SchemaRegistry:

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "schemaEndpoints")

        self.verify_cert=verify_cert
        self.schemas_end_point = self.url + self.lensesSchemasEndpoint
        self.schemas_config_end_point = self.url + self.lensesSchemasConfigEndpoint
        self.schemas_ids_end_point = self.url + self.lensesSchemasIDsEndpoint
        self.registry_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-kafka-lenses-token': self.token}

    def GetAllSubjects(self):
        """
            Returns:
                List with all subjects
        """
        self.getAllSubjects = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.schemas_end_point,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.getAllSubjects

    def UpdateSchema(self, subject, schema_json):
        __RQE = self.url
        __RQE = __RQE + "/api/proxy-sr/subjects/"
        __RQE = __RQE + subject + "/versions/"
        self.registerNewSchema = exec_request(
            __METHOD="post",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __DATA=schema_json,
            __VERIFY=self.verify_cert
        )

        return self.registerNewSchema

    def ListVersionsSubj(self, subject):
        """
            Parameters:
                - subject name (String)
            Returns:
                List with the subject's versions

        """
        __RQE = self.schemas_end_point
        __RQE = __RQE + '/' + subject + "/versions"
        self.listVersionsSubj = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.listVersionsSubj

    def RegisterNewSchema(self, subject, schema_json):
        """
            Parameters:
                - subject name (String)
                - subject's schema (Json)
                    - Example:
                    {
                        'schema':
                            '{
                                "type":"record","name":"reddit_post_key",'
                                '"namespace":"com.lensesio.social.reddit.post.key",'
                                '"fields":
                                    [
                                        {
                                            "name":"subreddit_id","type":"string"
                                        }
                                    ]
                            }'
                    }
            Returns:
                Json: {"id":subjectID}
        """
        __RQE = self.schemas_end_point
        __RQE = __RQE + '/' + subject + '/versions'
        self.registerNewSchema = exec_request(
            __METHOD="post",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __DATA=schema_json,
            __VERIFY=self.verify_cert
        )

        return self.registerNewSchema

    def DeleteSubj(self, subject):
        """
            Parameters:
                - subject name (String)
            Returns:
                If subject exists - String: '[subjectVersion]'
                If subject does not exist - String:
                '{"error_code":40401,"message":"Subject not found."}'
        """
        __RQE = self.schemas_end_point
        __RQE = __RQE + '/' + subject
        self.deleteSubj = exec_request(
            __METHOD="delete",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteSubj

    def GetSchemaById(self, subjid):
        """
            Parameters:
                - subjectID (Integer)
            Returns:
                If subjectID exists - Dictionary describing the schema
                If subjectID does not exist - String:
                '{"error_code":404,"message":"HTTP 404 Not Found"}'
        """
        __RQE = self.schemas_ids_end_point
        __RQE = __RQE + '/' + str(subjid)
        self.getSchemaById = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.getSchemaById

    def GetSchemaByVer(self, subject, verid):
        """
            Parameters:
                - subject name (String)
                - versionID (Integer)
            Returns:
                If subject & versiodID exists -
                    Dictionary describing the schema with the specific version
                If subject & versiodID does not exist - String:
                    '{"error_code":40402,"message":"Version not found."}'
        """
        __RQE = self.schemas_end_point
        __RQE = __RQE + '/' + subject + '/versions/' + str(verid)
        self.getSchemaByVer = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.getSchemaByVer

    def DeleteSchemaByVersion(self, subject, version):
        """
            Parameters:
                - subject name (String)
                - versionID (Integer)
            Returns:
                If subject & versiodID exists -
                    String with the deleted version
                If subject exists and versiodID does not exist - String:
                    '{"error_code":40402,"message":"Version not found."}'
                If subject does not exist - String:
                    '{"error_code":40401,"message":"Subject not found."}'

        """
        __RQE = self.schemas_end_point
        __RQE = __RQE + '/' + subject + '/versions/' + str(version)
        self.deleteSchemaByVersion = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteSchemaByVersion

    def UpdateGlobalCompatibility(self, compatibility):
        """
            Parameters:
                - compatibility (String)
        """
        __RQE = self.schemas_config_end_point
        self.updateGlobalCompatibility = exec_request(
            __METHOD="put",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __DATA=compatibility,
            __VERIFY=self.verify_cert
        )

        return self.updateGlobalCompatibility

    def GetGlobalCompatibility(self):
        __RQE = self.schemas_config_end_point
        self.getGlobalCompatibility = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.getGlobalCompatibility

    def ChangeCompatibility(self, subject, compatibility):
        """
            Parameters:
                - subject name (String)
                - compatibility (String)
        """
        __RQE = self.schemas_config_end_point
        __RQE = __RQE + '/' + subject
        self.changeCompatibility = exec_request(
            __METHOD="put",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __DATA=compatibility,
            __VERIFY=self.verify_cert
        )

        return self.changeCompatibility

    def GetCompatibility(self, subject):
        """
            Parameters:
                - subject name (String)
        """
        __RQE = self.schemas_config_end_point
        __RQE = __RQE + '/' + subject
        self.getCompatibility = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.registry_headers,
            __VERIFY=self.verify_cert
        )

        return self.getCompatibility
