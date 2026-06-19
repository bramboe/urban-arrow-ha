#!/usr/bin/with-contenv bashio
# shellcheck shell=bash

export BIKE_ADDRESS="$(bashio::config 'bike_address')"
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

# The reader auto-detects the bike, bonds it on the fly (Just Works pairing,
# done the moment the bike is seen in pairing mode — no restart needed), reads
# the battery and publishes to MQTT.
# COMODULE (URBANARROW) motion tracker — empty address = auto-detect by name.
export COMODULE_ADDRESS="$(bashio::config 'comodule_address')"
export MOTION_OFF_DELAY="$(bashio::config 'motion_off_delay')"

bashio::log.info "Starting Urban Arrow battery reader (${BIKE_ADDRESS:-auto-detect})"
exec python3 /bosch_mqtt_reader.py
