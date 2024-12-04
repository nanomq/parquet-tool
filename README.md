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
./parquet-tool replay 10 mqtt-tcp://127.0.0.1:1883 /tmp/foo.parquet

:parquet-tool sort ts|signal <DIR>
:sort parquet files in <DIR> with ts or signal
:--------------------------------------------------
:parquet-tool search <START-KEY> <END-KEY> <DIR>
:search parquet files in <DIR> in range of <START-KEY> to <END-KEY>
:--------------------------------------------------
:parquet-tool binary key|data|both <FILE...>
:print keys or data or both of them in <FILE...> in binary
:--------------------------------------------------
:parquet-tool decrypt <FOOT-KEY> <COL1-KEY> <COL2-KEY> <FILE...>
:decrypt <FILE...> with <FOOT-KEY> <COL1-KEY> <COL2-KEY>.
:filename of decrypted files will be like dec.foo.parquet
:--------------------------------------------------
:parquet-tool replay <INTERVAL> <MQTT-URL> <FILE...>
:replay datas in <FILE...> to <MQTT-URL> mqtt broker in <INTERVAL>ms
```
