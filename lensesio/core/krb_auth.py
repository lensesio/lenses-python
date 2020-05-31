from requests import get

try:
    import kerberos
except ImportError:
    pass


class KerberosTicket:
    def __init__(self, service):
        __, krb_context = kerberos.authGSSClientInit(service)
        kerberos.authGSSClientStep(krb_context, "")
        self._krb_context = krb_context
        self.auth_header = (
            "Negotiate " + kerberos.authGSSClientResponse(krb_context)
        )

    def verify_response(self, auth_header):
        for field in auth_header.split(","):
            kind, __, details = field.strip().partition(" ")
            if kind.lower() == "negotiate":
                auth_details = details.strip()
                break
        else:
            raise ValueError("Negotiate not found in %s" % auth_header)
        krb_context = self._krb_context
        if krb_context is None:
            raise RuntimeError("Ticket already used for verification")
        self._krb_context = None
        kerberos.authGSSClientStep(krb_context, auth_details)
        kerberos.authGSSClientClean(krb_context)


class krb5():
    def __init__(self, url, service):
        self.auth_url = url + "/api/auth"
        self.token = None
        self.service = service

    def KrbAuth(self):
        self.krb_ticket = KerberosTicket(self.service)
        headers = {"Authorization": self.krb_ticket.auth_header}
        self.krb_request = get(self.auth_url, headers=headers)
        self.krb_request = self.krb_request.json()
        self.token = self.krb_request['token']
        self.active_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/plain',
            'X-Kafka-Lenses-Token': self.token
        }
