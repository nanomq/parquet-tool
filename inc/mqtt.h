#ifndef PARQUET_TOOL_MQTT_H
#define PARQUET_TOOL_MQTT_H

#include <nng/nng.h>

void mqtt_connect(nng_socket *sock, char *url);
void mqtt_publish(nng_socket sock, char *topic, const char *data, size_t len);

#endif
