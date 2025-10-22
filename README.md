# Parquet tool for NanoMQ

## Requires
* nanosdk
* openssl
* zstd

```
# Install openssl and zstd
apt install openssl-dev zstd

# Install nanosdk
git clone https://github.com/emqx/NanoSDK ; cd NanoSDK
git submodule update --init --recursive 
mkdir build && cd build
cmake -G Ninja ..
ninja install
```

## Start

```
Usage: ./parquet-tool <CMD>
Parquet tool version 0.0.9

Available commands: ls, sort, cat, search, fuzz, replay, schema, version

Examples:
./parquet-tool ls -r 0,1000 -d /tmp
./parquet-tool sort -k ts -d /tmp
./parquet-tool cat -c key -f /tmp/foo.parquet
./parquet-tool search -s canspi -r 0,1000 -d /tmp
./parquet-tool fuzz -s canspi -r 0,1000 -d /tmp
./parquet-tool replay -i 10 -u mqtt-tcp://127.1:1883 -t topic -f /tmp/foo.parquet
./parquet-tool schema -f /tmp/foo-schema.parquet
```
