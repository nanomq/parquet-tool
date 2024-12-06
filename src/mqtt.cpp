#include <log.h>
#include <nng/nng.h>
#include <nng/mqtt/mqtt_client.h>

static void
disconnect_cb(nng_pipe p, nng_pipe_ev ev, void *arg)
{
	int reason = 0;
	nng_pipe_get_int(p, NNG_OPT_MQTT_DISCONNECT_REASON, &reason);
	ptlog("disconnected!");
	(void) ev;
	(void) arg;
}

static void
connect_cb(nng_pipe p, nng_pipe_ev ev, void *arg)
{
	int reason;
	nng_pipe_get_int(p, NNG_OPT_MQTT_CONNECT_REASON, &reason);
	ptlog("connected!");
	nng_aio_finish((nng_aio *)arg, 0);
	(void) ev;
	(void) arg;
}

void
mqtt_connect(nng_socket *sock, char *url)
{
	int         rv;
	nng_dialer  dialer;
	nng_aio    *connected;

	if ((rv = nng_aio_alloc(&connected, NULL, NULL)) != 0) {
		ptlog("error %d", rv);
		return;
	}
	nng_aio_begin(connected);

	if ((rv = nng_mqtt_client_open(sock)) != 0) {
		ptlog("error %d", rv);
		return;
	}

	if ((rv = nng_dialer_create(&dialer, *sock, url)) != 0) {
		ptlog("error %d", rv);
		return;
	}

	// create a CONNECT message
	/* CONNECT */
	nng_msg *connmsg;
	nng_mqtt_msg_alloc(&connmsg, 0);
	nng_mqtt_msg_set_packet_type(connmsg, NNG_MQTT_CONNECT);
	nng_mqtt_msg_set_connect_proto_version(connmsg, 4);
	nng_mqtt_msg_set_connect_keep_alive(connmsg, 60);
	nng_mqtt_msg_set_connect_client_id(connmsg, "nanomq_parquet_tool");
	nng_mqtt_msg_set_connect_clean_session(connmsg, true);

	nng_mqtt_set_connect_cb(*sock, connect_cb, (void *)connected);
	nng_mqtt_set_disconnect_cb(*sock, disconnect_cb, NULL);

	uint8_t buff[1024] = { 0 };

	ptlog("Connecting to server ...");
	nng_dialer_set_ptr(dialer, NNG_OPT_MQTT_CONNMSG, connmsg);
	nng_dialer_start(dialer, NNG_FLAG_NONBLOCK);

	nng_aio_wait(connected);
}

void
mqtt_publish(nng_socket sock, char *topic, const char *data, size_t len)
{
	int rv;
	int qos = 0;

	// create a PUBLISH message
	nng_msg *pubmsg;
	nng_mqtt_msg_alloc(&pubmsg, 0);
	nng_mqtt_msg_set_packet_type(pubmsg, NNG_MQTT_PUBLISH);
	nng_mqtt_msg_set_publish_dup(pubmsg, 0);
	nng_mqtt_msg_set_publish_qos(pubmsg, qos);
	nng_mqtt_msg_set_publish_retain(pubmsg, 0);
	nng_mqtt_msg_set_publish_payload(
	    pubmsg, (uint8_t *) data, len);
	nng_mqtt_msg_set_publish_topic(pubmsg, topic);

	if ((rv = nng_sendmsg(sock, pubmsg, NNG_FLAG_ALLOC)) != 0) {
		ptlog("nng_sendmsg", rv);
	}
}
