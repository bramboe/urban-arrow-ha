#!/usr/bin/with-contenv bashio
# shellcheck shell=bash

export BIKE_ADDRESS="$(bashio::config 'bike_address')"
if bashio::config.has_value 'bike_model'; then
    export BIKE_MODEL="$(bashio::config 'bike_model')"
else
    export BIKE_MODEL=""
fi
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
# Battery-friendly: keep the tracker connected only while armed (default).
if bashio::config.true 'tracker_always_on'; then export TRACKER_ALWAYS=1; else export TRACKER_ALWAYS=0; fi
# Passive presence anti-theft: trip the alarm when the tracker leaves BLE range.
if bashio::config.false 'presence_alarm'; then export PRESENCE_ALARM=0; else export PRESENCE_ALARM=1; fi
# TEMPORARY: log distinct COMODULE 155e status frames (find the main-battery flag).
if bashio::config.true 'probe_frames'; then export PROBE_FRAMES=1; else export PROBE_FRAMES=0; fi
if bashio::config.true 'adv_probe'; then export ADV_PROBE=1; else export ADV_PROBE=0; fi

bashio::log.info "Starting Urban Arrow battery reader (${BIKE_ADDRESS:-auto-detect})"
exec python3 /bosch_mqtt_reader.py
