ARG BUILD_FROM
FROM $BUILD_FROM

# Python + BlueZ (bluetoothctl for one-time bonding) + BLE/MQTT libs
RUN apk add --no-cache python3 py3-pip bluez \
    && pip3 install --no-cache-dir --break-system-packages bleak paho-mqtt aiohttp

COPY run.sh /run.sh
COPY bosch_mqtt_reader.py /bosch_mqtt_reader.py
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
