### Docker/Podman setup

```
podman run --rm -it \
  -p 21224:21224 \
  -v /dev/ttyUSB0:/dev/ttyUSB0 \
  -e LOGLEVEL=debug \
  -e MODBUS_URI=rtu:/dev/ttyUSB0:baudRate=9600 \
  sdellysse/modbus-http
```
