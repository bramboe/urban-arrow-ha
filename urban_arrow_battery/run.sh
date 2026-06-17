#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
set -e

BIKE_ADDRESS="$(bashio::config 'bike_address')"
export BIKE_ADDRESS
export COOLDOWN="$(bashio::config 'cooldown')"

# MQTT: use the configured override, else the Home Assistant MQTT service.
if bashio::config.has_value 'mqtt_host'; then
    export MQTT_HOST="$(bashio::config 'mqtt_host')"
    export MQTT_PORT="$(bashio::config 'mqtt_port')"
    export MQTT_USER="$(bashio::config 'mqtt_user')"
    export MQTT_PASS="$(bashio::config 'mqtt_pass')"
elif bashio::services.available 'mqtt'; then
    export MQTT_HOST="$(bashio::services 'mqtt' 'host')"
    export MQTT_PORT="$(bashio::services 'mqtt' 'port')"
    export MQTT_USER="$(bashio::services 'mqtt' 'username')"
    export MQTT_PASS="$(bashio::services 'mqtt' 'password')"
else
    bashio::exit.nok "No MQTT broker found. Install the Mosquitto add-on, or set mqtt_host/user/pass."
fi
bashio::log.info "MQTT broker: ${MQTT_HOST}:${MQTT_PORT}"

# Determine which address to bond. If 'bike_address' is empty, auto-detect the
# Bosch hub by its advertised name 'smart system eBike'.
PAIR_ADDR="${BIKE_ADDRESS}"
if [ -z "${PAIR_ADDR}" ]; then
    bashio::log.info "Auto-detecting the bike (scanning for 'smart system eBike')..."
    PAIR_ADDR="$(bluetoothctl --timeout 15 scan on 2>/dev/null \
        | grep -i 'smart system' \
        | grep -oiE '([0-9A-F]{2}:){5}[0-9A-F]{2}' | head -n1 || true)"
    if [ -n "${PAIR_ADDR}" ]; then
        bashio::log.info "Detected bike at ${PAIR_ADDR}"
    else
        bashio::log.warning "No bike detected yet (awake? in pairing mode?). The reader will keep scanning."
    fi
fi

# Best-effort one-time bonding (this bike uses code-free 'Just Works' pairing).
if [ -n "${PAIR_ADDR}" ]; then
    if ! bluetoothctl info "${PAIR_ADDR}" 2>/dev/null | grep -q "Bonded: yes"; then
        bashio::log.warning "Not bonded — pairing ${PAIR_ADDR}. Put the bike in PAIRING MODE now."
        bluetoothctl --timeout 20 scan on >/dev/null 2>&1 || true
        bluetoothctl pair "${PAIR_ADDR}" || true
    fi
    bluetoothctl trust "${PAIR_ADDR}" >/dev/null 2>&1 || true
fi

bashio::log.info "Starting Urban Arrow battery reader (${BIKE_ADDRESS:-auto-detect})"
exec python3 /bosch_mqtt_reader.py
