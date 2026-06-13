# Urban Arrow for Home Assistant

Reads the battery, odometer and last-update time from an Urban Arrow e-bike
(Bosch Smart System) over Bluetooth Low Energy — through any ESPHome Bluetooth
proxy in range.

The bike is only reachable while the display is on, so the sensors become
**unavailable** when the bike is asleep and update again on the next ride.
