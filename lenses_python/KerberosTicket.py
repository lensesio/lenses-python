import kerberos

class KerberosTicket:
    def __init__(self, service):
        """
        Use to take the Kerberos ticket

        :param service:
        """
        __, krb_context = kerberos.authGSSClientInit(service)
        kerberos.authGSSClientStep(krb_context, "")
        self._krb_context = krb_context
        # Create the value of authentication header
        self.auth_header = ("Negotiate " +kerberos.authGSSClientResponse(krb_context))
    def verify_response(self, auth_header):
        """
        Handle comma-separated lists of authentication fields
        :param auth_header:
        :return:
        """
        for field in auth_header.split(","):
            kind, __, details = field.strip().partition(" ")
            if kind.lower() == "negotiate":
                auth_details = details.strip()
                break
        else:
            raise ValueError("Negotiate not found in %s" % auth_header)
        # Finish the Kerberos handshake
        krb_context = self._krb_context
        if krb_context is None:
            raise RuntimeError("Ticket already used for verification")
        self._krb_context = None
        kerberos.authGSSClientStep(krb_context, auth_details)
        kerberos.authGSSClientClean(krb_context)
