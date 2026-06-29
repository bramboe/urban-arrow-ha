ARG BUILD_FROM
FROM $BUILD_FROM

# Python + BlueZ (bluetoothctl for one-time bonding) + BLE/MQTT libs
RUN apk add --no-cache python3 py3-pip bluez ca-certificates \
    && pip3 install --no-cache-dir --break-system-packages \
       bleak paho-mqtt aiohttp aioesphomeapi zeroconf certifi

COPY run.sh /run.sh
COPY bosch_mqtt_reader.py /bosch_mqtt_reader.py
COPY skus.json /skus.json
COPY comp_init.txt /comp_init.txt
COPY bike.png /bike.png
COPY leaflet.js /leaflet.js
COPY leaflet.css /leaflet.css
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
