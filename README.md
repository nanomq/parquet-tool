# Parquet tool for NanoMQ

## Requires
* boost
* parquet
* nanosdk

## Start

```
Usage: ./parquet-tool <OPTION>
Parquet tool version 0.0.1

Available options: sort, search, binary, decrypt, replay, version
Examples:
./parquet-tool sort ts /tmp
./parquet-tool search 0 1000 /tmp
./parquet-tool binary data /tmp/foo.parquet /tmp/bar.parquet
./parquet-tool decrypt /tmp/foo.parquet
./parquet-tool replay 10 mqtt-tcp://127.0.0.1:1883 topic /tmp/foo.parquet

:parquet-tool sort ts|signal <DIR>
:sort parquet files in <DIR> with ts or signal
:--------------------------------------------------
:parquet-tool search <START-KEY> <END-KEY> <DIR>
:search parquet files in <DIR> in range of <START-KEY> to <END-KEY>
:--------------------------------------------------
:parquet-tool binary key|data|both <FILE...>
:print keys or data or both of them in <FILE...> in binary
:--------------------------------------------------
:parquet-tool decrypt key|data|both <FOOT-KEY> <COL1-KEY> <COL2-KEY> <FILE...>
:decrypt key or data or both in<FILE...> with <FOOT-KEY> <COL1-KEY> <COL2-KEY>
:--------------------------------------------------
:parquet-tool replay <INTERVAL> <MQTT-URL> <TOPIC> <FILE...>
:replay datas in <FILE...> to <MQTT-URL> mqtt broker in <INTERVAL>ms
:--------------------------------------------------
:parquet-tool decreplay <FOOT-KEY> <COL1-KEY> <COL2-KEY> <INTERVAL> <MQTT-URL> <TOPIC> <FILE...>
:Combine decrypt and replay
```

## QUIET Mode

Quiet mode is only valid when cmd
is `parquet-tool binary data ...` or `parquet-tool decrypt data ...`.
In this mode. Neither split `\n` nor other logs will be printed.
It will be useful when you try to write datas in parquet to a file.

```
QUIET=1 ./parquet-tool binary data /tmp/foo.parquet
```
