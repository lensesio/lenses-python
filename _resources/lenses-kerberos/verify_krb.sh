#!/usr/bin/env bash

if [[ "${1}" == "auth" ]]; then
	docker exec -it krb bash -c "kinit devops <<CMD
devops
CMD

klist"
exit 0
fi

klist -k -t -K local/krb5.keytab

export KRB5_CONFIG="$PWD/local/krb5.conf"

kinit devops <<CMD
devops
CMD

curl -v --compressed --negotiate -u : "http://${1}:3030/api/auth"

if [[ "${2}" == "browser" ]]; then
	google-chrome-stable -auth-server-whitelist="${1}" "http://${1}:3030"
fi

kdestroy
