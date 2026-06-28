#!/bin/sh -e

# The apache/nifi image only supports HTTPS (single-user secured) out of the
# box. Reconfigure the web server for plain HTTP (unsecured) access, then run
# NiFi. This runs against the live conf (which may live in a named volume), so
# the settings are applied on every start regardless of volume state.

NIFI_HOME="${NIFI_HOME:-/opt/nifi/nifi-current}"
props="${NIFI_HOME}/conf/nifi.properties"

set_prop() { sed -i "s|^${1}=.*|${1}=${2}|" "$props"; }

set_prop nifi.web.http.host                          0.0.0.0
set_prop nifi.web.http.port                          8080
set_prop nifi.web.https.host                         ""
set_prop nifi.web.https.port                         ""
set_prop nifi.remote.input.secure                    false
set_prop nifi.web.proxy.host                         ""
# Login identity provider requires HTTPS; clear it for unsecured HTTP.
set_prop nifi.security.user.login.identity.provider  ""
# Clear the keystore/truststore so NiFi does not build an SSL context (the
# image points these at conf/keystore.p12, which does not exist unsecured).
set_prop nifi.security.keystore                      ""
set_prop nifi.security.keystoreType                  ""
set_prop nifi.security.truststore                    ""
set_prop nifi.security.truststoreType                ""

"${NIFI_HOME}/bin/nifi.sh" run &
nifi_pid="$!"
tail -F --pid="${nifi_pid}" "${NIFI_HOME}/logs/nifi-app.log" 2>/dev/null &
trap 'echo "Received signal, shutting down NiFi..."; "${NIFI_HOME}/bin/nifi.sh" stop; exit 0;' TERM HUP INT
wait "${nifi_pid}"
