#!/bin/bash

set -x

# Network variables
export HOST_FQDN=${HOST_FQDN:-$(hostname -f)}
export NAMESERVER_IP=${NAMESERVER_IP:-}
export SEARCH_DOMAINS=${SEARCH_DOMAINS:-}
export HOST_IP=${HOST_IP:-}

# Kerberos Domain and realm variables
export DOMAIN=${DOMAIN:-"localhost.localdomain"}
export REALM=${REALM:-"${DOMAIN^^}"}

# Kerberos masterkey
export KERB_MASTER_KEY=${KERB_MASTER_KEY:-"masterkey"}

# Kerberos admin
export KERB_ADMIN_USER=${KERB_ADMIN_USER:-"admin"}
export KERB_ADMIN_PASS=${KERB_ADMIN_PASS:-"admin"}
# Kerberos user
export KRB_USER=${KRB_USER:-"devops"}
export KRB_USER_PASSWORD=${KRB_USER_PASSWORD:-"devops"}

function update_resolver {
  cat>/etc/resolv.conf<<CMD
nameserver ${NAMESERVER_IP}
search ${SEARCH_DOMAINS}
CMD
}

function update_hosts {
  while [[ "$#" -ge "1" ]]; do
    echo "${1} ${2}" >> /etc/hosts
    shift
  done
}

function update_nsswitch {
  sed -i "/^hosts:/ s/ *files dns/ dns files/" /etc/nsswitch.conf
}

function create_config {
  cat>/etc/krb5.conf<<EOF
[logging]
  default = FILE:/var/log/kerberos/krb5libs.log
  kdc = FILE:/var/log/kerberos/krb5kdc.log
  admin_server = FILE:/var/log/kerberos/kadmind.log

[libdefaults]
  default_realm = ${REALM}
  default_tkt_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac des-cbc-crc des-cbc-md5
  default_tgs_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac des-cbc-crc des-cbc-md5
  permitted_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac des-cbc-crc des-cbc-md5
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  forwardable = true

[realms]
 ${REALM} = {
   kdc = ${HOST_FQDN}
   admin_server = ${HOST_FQDN}
   default_principal_flags = +preauth
 }

[domain_realm]
  .${DOMAIN} = ${REALM}
  ${DOMAIN} = ${REALM}
EOF
}

function create_database {
  /usr/sbin/kdb5_util -P "${KERB_MASTER_KEY}" -r "${REALM}" create -s
}

function create_admin_user {
  kadmin.local -q "addprinc -pw ${KERB_ADMIN_PASS} ${KERB_ADMIN_USER}/admin"
  echo "*/admin@${REALM} *" > /var/kerberos/kadm5.acl
}

function create_user {
  kadmin.local -q "addprinc -pw ${KRB_USER_PASSWORD} ${KRB_USER}@${REALM}"
}

function add_principal {
  kadmin.local -q "addprinc -randkey HTTP/localhost.localdomain"
}

function create_prc_keytab {
  rm -f "/local/krb5.keytab" "/local/krb5.conf"
  kadmin.local -q "ktadd -k /local/krb5.keytab HTTP/localhost.localdomain"
  chmod 0766 /local/krb5.keytab
  cp /etc/krb5.conf /local/
}

function manage_krb {
  /usr/bin/dumb-init -- /usr/sbin/krb5kdc
}

function main {
  if [[ -n ${NAMESERVER_IP} && "${SEARCH_DOMAINS}" ]]; then
    update_resolver
  fi

  update_nsswitch

  if [[ ! -f /opt/krb_init ]]; then
    mkdir -vp "/var/kerberos" "/var/log/kerberos"

    create_config
    create_database
    create_admin_user
    create_user
    add_principal
    create_prc_keytab

    touch /opt/krb_init
  fi

    manage_krb "start"
    tail -F /var/log/kerberos/krb5kdc.log
}

if [[ "$0" == "$BASH_SOURCE" ]]; then
  main "$@"
fi
