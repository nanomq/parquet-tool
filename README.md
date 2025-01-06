# Parquet tool for NanoMQ

## Requires
* boost
* parquet
* nanosdk

## Start

```
Usage: ./parquet-tool <CMD>
Parquet tool version 0.0.2

Available commands: ls, sort, cat, search, fuzz, replay, version

Examples:
./parquet-tool ls -r 0,1000 -d /tmp
./parquet-tool sort -k ts -d /tmp
./parquet-tool cat -c key -f /tmp/foo.parquet
./parquet-tool search -s canspi -r 0,1000 -d /tmp
./parquet-tool fuzz -s canspi -r 0,1000 -d /tmp
./parquet-tool replay -i 10 -u mqtt-tcp://127.1:1883 -t topic -f /tmp/foo.parquet
```
