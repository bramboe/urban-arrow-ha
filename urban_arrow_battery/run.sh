#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
set -e

BIKE_ADDRESS="$(bashio::config 'bike_address')"
export BIKE_ADDRESS
export COOLDOWN="$(bashio::config 'cooldown')"

if [ -z "${BIKE_ADDRESS}" ]; then
    bashio::exit.nok "Set 'bike_address' in the add-on configuration (e.g. A4:0D:BC:8A:41:D7)."
fi

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

# Best-effort one-time bonding. This bike uses Just Works pairing (no passkey),
# so if it is in pairing mode and not yet bonded, we can pair + trust it here.
if ! bluetoothctl info "${BIKE_ADDRESS}" 2>/dev/null | grep -q "Bonded: yes"; then
    bashio::log.warning "Bike not bonded yet — attempting to pair."
    bashio::log.warning "Put the bike in PAIRING MODE (display > connect new device) now."
    bluetoothctl --timeout 20 scan on >/dev/null 2>&1 || true
    bluetoothctl pair "${BIKE_ADDRESS}" || true
    bluetoothctl trust "${BIKE_ADDRESS}" || true
fi
bluetoothctl trust "${BIKE_ADDRESS}" >/dev/null 2>&1 || true

bashio::log.info "Starting Urban Arrow battery reader for ${BIKE_ADDRESS}"
exec python3 /bosch_mqtt_reader.py
