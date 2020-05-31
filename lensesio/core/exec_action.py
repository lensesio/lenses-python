import requests


def exec_request(
                __METHOD,
                __EXPECTED,
                __URL,
                __HEADERS=None,
                __DATA=None,
                __DT='json',
                __VERIFY=True):
    if not __URL:
        print("Please provide an endpoint.")

    if not __DATA:
        request = getattr(
            requests, __METHOD
        )(
            url=__URL,
            headers=__HEADERS,
            verify=__VERIFY,
        )

    elif __DATA and __DT == 'json':
        request = getattr(
            requests, __METHOD
        )(
            url=__URL,
            headers=__HEADERS,
            json=__DATA,
            verify=__VERIFY,
        )
    elif __DATA and __DT == 'data':
        request = getattr(
            requests, __METHOD
        )(
            url=__URL,
            headers=__HEADERS,
            data=__DATA,
            verify=__VERIFY,
        )
    else:
        return "Error handling the request. Please report this."

    if __EXPECTED == 'json':
        if request.status_code in [200, 201, 202, 203]:
            __DATA = request.json()
        else:
            __DATA = request.text
    elif __EXPECTED == 'text':
        __DATA = request.text
    else:
        __DATA = request

    return __DATA
