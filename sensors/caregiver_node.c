#include "contiki.h"
#include "contiki-net.h"
#include "net/routing/routing.h"
#include "net/ipv6/uip.h"
#include "net/ipv6/uip-icmp6.h"
#include "net/ipv6/sicslowpan.h"
#include "mqtt.h"
#include "coap-engine.h"
#include "coap-blocking-api.h"
#include "sys/etimer.h"
#include "sys/ctimer.h"
#include "lib/sensors.h"
#include "dev/button-hal.h"
#include "dev/leds.h"
#include "os/sys/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define LOG_MODULE "caregiver"
#define LOG_LEVEL  LOG_LEVEL_INFO

//MQTT BROKER
#define MQTT_CLIENT_BROKER_IP_ADDR "fd00::1"
static const char *broker_ip = MQTT_CLIENT_BROKER_IP_ADDR;

#define DEFAULT_BROKER_PORT 1883
#define DEFAULT_PUBLISH_INTERVAL (30 * CLOCK_SECOND)

#define MAX_TCP_SEGMENT_SIZE 128
#define CONFIG_IP_ADDR_STR_LEN 64
#define BUFFER_SIZE 64
#define NODE_ID_BUFFER_SIZE 32
#define APP_BUFFER_SIZE 512
#define TOPIC_BUFFER_SIZE 96 

static char client_id[BUFFER_SIZE];
static char heartbeat_topic[TOPIC_BUFFER_SIZE];
static char app_buffer[APP_BUFFER_SIZE];
static char broker_address[CONFIG_IP_ADDR_STR_LEN];
static char ack_topic_buf[TOPIC_BUFFER_SIZE];

static struct mqtt_connection conn;
mqtt_status_t status;

static uint8_t state;
#define STATE_INIT 0
#define STATE_NET_OK 1
#define STATE_CONNECTING 2
#define STATE_CONNECTED 3
#define STATE_SUBSCRIBED 4
#define STATE_RECONNECTING_FAST 5
#define STATE_RECONNECTING_SLOW 6
#define STATE_MANUAL_RESTART_REQUIRED 7

#define STATE_MACHINE_PERIODIC (CLOCK_SECOND >> 1)
static struct etimer periodic_timer;

#define MAX_FAST_RECONNECT_ATTEMPTS 5
#define MAX_TOTAL_RECONNECT_ATTEMPTS 8

#define FAST_RECONNECT_INTERVAL (5 * CLOCK_SECOND) / STATE_MACHINE_PERIODIC
#define SLOW_RECONNECT_INTERVAL (60 * CLOCK_SECOND) / STATE_MACHINE_PERIODIC
#define CONNECTING_TIMEOUT_INTERVAL (20 * CLOCK_SECOND) / STATE_MACHINE_PERIODIC

static uint8_t reconnect_attempts = 0;
static unsigned long reconnect_ticks = 0;
static unsigned long connecting_ticks = 0;

//CoAP registration
#define REGISTRATION_SERVER_EP "coap://[fd00::1]:5683"
#define REGISTRATION_PATH "/registration"

//caregiver sends heartbits in a relaxed way (since he doesn't non monitor critical data)
#define CAREGIVER_DEFAULT_PUBLISH_INTERVAL (60 * CLOCK_SECOND)
static unsigned long publish_counter = 0;
static unsigned long publish_every_n_ticks = CAREGIVER_DEFAULT_PUBLISH_INTERVAL / STATE_MACHINE_PERIODIC;

extern coap_resource_t res_config;
 
//returns actual rate for heartbeats publication
int get_publish_period(void) {
  return (int)((publish_every_n_ticks * STATE_MACHINE_PERIODIC) / CLOCK_SECOND);
}
 
//set a new rate in seconds for the adaptive mechanism
void set_publish_period(int seconds) {
  if(seconds > 0) {
    publish_every_n_ticks = (unsigned long)seconds * CLOCK_SECOND / STATE_MACHINE_PERIODIC;
    LOG_INFO("Rate aggiornato dalla Cloud App: %d s\n", seconds);
  }
}

#define MAX_ACTIVE_ALARMS 4   //maximu number of alarms that the caregiver can handle at once: it's equal to the number of patients 

//list to handle the patients that generated the alarm
typedef struct {
  char node_id[NODE_ID_BUFFER_SIZE]; //id
} alarm_entry_t;

//alarms are published at the end of the queue and are handled from the first one to the last one
static alarm_entry_t alarm_queue[MAX_ACTIVE_ALARMS];
static uint8_t queue_count = 0;  //index of the first arrived alarm, the once that the button confirms

static uint8_t alarm_pending = 0;

//red LED blinks during the alarm
#define ALARM_BLINK_PERIOD (CLOCK_SECOND / 4)
static struct etimer blink_timer;

PROCESS(caregiver_process, "Caregiver node");
AUTOSTART_PROCESSES(&caregiver_process);

//check if the node has network connectivity (global IPv6 address + default route)
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL ||
     uip_ds6_defrt_choose() == NULL) {
    return false;
  }
  return true;
}

static void enter_reconnect_or_manual_restart(void) {
  reconnect_ticks = 0;
  connecting_ticks = 0;

  if(reconnect_attempts >= MAX_TOTAL_RECONNECT_ATTEMPTS) {
    state = STATE_MANUAL_RESTART_REQUIRED;
    LOG_ERR("MQTT reconnection failed after %u attempts. Manual power cycle required: switch caregiver node OFF and ON.\n", reconnect_attempts);
    return;
  }

  if(reconnect_attempts >= MAX_FAST_RECONNECT_ATTEMPTS) {
    state = STATE_RECONNECTING_SLOW;
    LOG_INFO("MQTT reconnect: switching to slow mode\n");
  } else {
    state = STATE_RECONNECTING_FAST;
    LOG_INFO("MQTT reconnect: using fast mode\n");
  }
}

//adds alarm at the end of the queue
static void alarm_queue_push(const char *node_id) {
  uint8_t i;

  //if that patient has already an alarm, it won't be reinserted 
  for(i = 0; i < queue_count; i++) {
    if(strcmp(alarm_queue[i].node_id, node_id) == 0) {
      return;
    }
  }

  if(queue_count >= MAX_ACTIVE_ALARMS) {
    LOG_ERR("Coda allarmi piena, impossibile aggiungere %s\n", node_id);
    return;
  }

  strncpy(alarm_queue[queue_count].node_id, node_id, NODE_ID_BUFFER_SIZE - 1);
  alarm_queue[queue_count].node_id[NODE_ID_BUFFER_SIZE - 1] = '\0';
  queue_count++;
}

//removes the first element from the queue
//all the other alarms will be advanced of one position 
static void alarm_queue_pop_front(void) {
  uint8_t i;
  if(queue_count == 0) return;

  for(i = 0; i < queue_count - 1; i++) {
    alarm_queue[i] = alarm_queue[i + 1];
  }
  queue_count--;
}

//removes a specific node from the queue
//used when the patient deletes the alarm by clickig the button for more than 3s
static void alarm_queue_remove(const char *node_id) {
  uint8_t i, j;
  for(i = 0; i < queue_count; i++) {
    if(strcmp(alarm_queue[i].node_id, node_id) == 0) {
      for(j = i; j < queue_count - 1; j++) {
        alarm_queue[j] = alarm_queue[j + 1];
      }
      queue_count--;
      return;
    }
  }
}

//extract the value "node_id" from a JSON payload {"node_id":"xxxx","event":"FALL"}
//the message is fixed and generated only by the patient_node so we just need manual parsing manuale 
static int extract_node_id(const uint8_t *payload, uint16_t payload_len, char *out, int out_size) {
  const char *key = "\"node_id\":\"";
  int key_len = strlen(key);
  int i;

  for(i = 0; i + key_len < payload_len; i++) {
    if(memcmp(payload + i, key, key_len) == 0) {
      int start = i + key_len;
      int j = 0;
      while(start + j < payload_len && payload[start + j] != '"' && j < out_size - 1) {
        out[j] = payload[start + j];
        j++;
      }
      out[j] = '\0';
      return 1;
    }
  }
  return 0;
}

//activate red LED blinking
static void start_alarm_blink(void) {
  alarm_pending = 1;
  printf("\xF0\x9F\x94\x94 ALARM\n"); //logs the alarm
}

//stop led blinking and switch off the led
static void stop_alarm_blink(void) {
  alarm_pending = 0;
  leds_single_off(LEDS_RED);
}


//cerca "needle" dentro "chunk" senza assumere che chunk sia null-terminated
static int chunk_contains(const uint8_t *chunk, uint16_t chunk_len, const char *needle) {
  int needle_len = strlen(needle);
  int i;

  if(needle_len > chunk_len) return 0;

  for(i = 0; i + needle_len <= chunk_len; i++) {
    if(memcmp(chunk + i, needle, needle_len) == 0) {
      return 1;
    }
  }
  return 0;
}


//handles MQTT messages: a FALL event activates caregiver notification
//a false alarm form the patient closes it 
static void pub_handler(const char *topic, uint16_t topic_len, const uint8_t *chunk, uint16_t chunk_len) {
  char node_id[NODE_ID_BUFFER_SIZE];

  //log x vedere se il payload è ok
  LOG_INFO("chunk_len=%u | ", chunk_len);
  for(int i = 0; i < chunk_len; i++) {
    printf("%02x ", chunk[i]);
  }
  printf("\n");

  if(strncmp(topic, "alarm/", 6) == 0) {
    LOG_INFO("entro nell'if");

    if(strstr(topic, "/ack") != NULL) {
      LOG_INFO("An ACK is arrived! Return\n");
      return;  //an ack is arrived
    }

    if(!extract_node_id(chunk, chunk_len, node_id, NODE_ID_BUFFER_SIZE)) {
      LOG_INFO("Malformed payload, ignore! Return\n");
      return;  //malformed paylod -> ignore
    }

    if(chunk_contains(chunk, chunk_len, "\"event\":\"FALL\"")) {
      LOG_INFO("Ho ricevuto un messaggio FALL");
      alarm_queue_push(node_id);
      LOG_INFO("Inserisco il messaggio FALL nella coda");
      start_alarm_blink();
      LOG_INFO("Allarme FALL ricevuto da %s (in coda: %d)\n", node_id, queue_count);

    } else if(chunk_contains(chunk, chunk_len, "\"event\":\"RESOLVED\"")) {
      alarm_queue_remove(node_id);
      LOG_INFO("Allarme di %s risolto (in coda: %d)\n", node_id, queue_count);

      if(queue_count == 0) {
        stop_alarm_blink();
      }
    }
  }
}

//callback function called when an MQTT event arrives
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
  case MQTT_EVENT_CONNECTED:
    LOG_INFO("MQTT connected\n");
    reconnect_attempts = 0;
    reconnect_ticks = 0;
    connecting_ticks = 0;
    state = STATE_CONNECTED;
    break;

  case MQTT_EVENT_DISCONNECTED:
    LOG_INFO("MQTT disconnected. Reason %u\n", *((mqtt_event_t *)data));
    state = STATE_DISCONNECTED;
    enter_reconnect_or_manual_restart();
    process_poll(&caregiver_process);
    break;

  case MQTT_EVENT_PUBLISH: {
    LOG_INFO("evento pubblicato\n");
    struct mqtt_message *msg = data;
    LOG_INFO("Ricevuto messaggio sul topic: %s\n", msg->topic);
    pub_handler(msg->topic, strlen(msg->topic), msg->payload_chunk, msg->payload_length);
    break;
  }

  case MQTT_EVENT_SUBACK: {
   #if MQTT_311
      mqtt_suback_event_t *suback_event = (mqtt_suback_event_t *)data;

      if(suback_event->success) {
        LOG_INFO("MQTT subscribed successfully\n");
        state = STATE_SUBSCRIBED;
      } else {
        LOG_ERR("MQTT subscribe FAILED (return code %x)\n", suback_event->return_code);
        // stato non aggiornato: restiamo in STATE_CONNECTED, da gestire eventualmente con retry
      }
    #else
      LOG_INFO("MQTT subscribed\n");
      state = STATE_SUBSCRIBED;
    #endif
      break;
  }

  default:
    LOG_INFO("Unhandled MQTT event: %i\n", event);
    break;
  }
}

//heartbeat is more relaxed than patient one: it is needed to report to the Cloud App that the node is still active
static void publish_heartbeat(void) {
  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"caregiver\"}", client_id);
  mqtt_publish(&conn, NULL, heartbeat_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
}

//used to confirm that the caregiver has seen the alarm
//called when the caregiver presses the button
//it stops linking on the patient node
static void publish_ack(const char *node_id) {
  snprintf(ack_topic_buf, TOPIC_BUFFER_SIZE, "alarm/%s/ack", node_id);
  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"ack_by\":\"caregiver\"}", node_id);
  mqtt_publish(&conn, NULL, ack_topic_buf, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_1, MQTT_RETAIN_OFF);
}

//callback to handle response to CoAP registration (debug/logging)
static void client_chunk_handler(coap_message_t *response) {
  const uint8_t *chunk;
  int len;

  if(response == NULL) {
    LOG_INFO("Registration: timeout\n");
    return;
  }

  len = coap_get_payload(response, &chunk);
  LOG_INFO("Registration response: %.*s\n", len, (char *)chunk);
}


PROCESS_THREAD(caregiver_process, ev, data)
{
  coap_endpoint_t server_ep;
  static coap_message_t request[1];

  PROCESS_BEGIN();

  coap_activate_resource(&res_config, "config"); 

  //client_id from MAC address - node identity
  snprintf(client_id, BUFFER_SIZE, "%02x%02x%02x%02x%02x%02x",
           linkaddr_node_addr.u8[0], linkaddr_node_addr.u8[1],
           linkaddr_node_addr.u8[2], linkaddr_node_addr.u8[5],
           linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);

  LOG_INFO("Caregiver node process started (%s)\n", client_id);

  snprintf(heartbeat_topic, TOPIC_BUFFER_SIZE, "health/%s/heartbeat", client_id);

  //wait for connectivity before attempting coap registration
  while(!have_connectivity()) {
    PROCESS_PAUSE();
  }

  //coap registration - blocking, done once at boot before starting MQTT
  coap_endpoint_parse(REGISTRATION_SERVER_EP, strlen(REGISTRATION_SERVER_EP), &server_ep);

  coap_init_message(request, COAP_TYPE_CON, COAP_POST, 0);
  coap_set_header_uri_path(request, REGISTRATION_PATH);

  snprintf(app_buffer, APP_BUFFER_SIZE,
           "{\"node_id\":\"%s\",\"type\":\"caregiver\",\"protocol\":\"mqtt\"}", client_id);
  coap_set_payload(request, (uint8_t *)app_buffer, strlen(app_buffer));

  COAP_BLOCKING_REQUEST(&server_ep, request, client_chunk_handler);

  //activate MQTT
  mqtt_register(&conn, &caregiver_process, client_id, mqtt_event, MAX_TCP_SEGMENT_SIZE);

  state = STATE_INIT;
  etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
  etimer_set(&blink_timer, ALARM_BLINK_PERIOD); 

  while(1) {
    PROCESS_YIELD();

    //bottone pressed: caregiver confirms the first alarm received 
    if(ev == button_hal_press_event) {
      LOG_INFO("PRESSED BUTTON TO CONFIRM ALARM\n");
        if(queue_count > 0) {
          LOG_INFO("queue_count = %d\n", queue_count);
          LOG_INFO("QUEUE_COUNT>=0\n");
          char confirmed_id[NODE_ID_BUFFER_SIZE];
          strncpy(confirmed_id, alarm_queue[0].node_id, NODE_ID_BUFFER_SIZE);

          LOG_INFO("Caregiver confirms alarm for %s (coda rimanente: %d)\n",
                  confirmed_id, queue_count - 1);

          if(state == STATE_SUBSCRIBED) {
          publish_ack(confirmed_id);
          }

          alarm_queue_pop_front();

          if(queue_count == 0) {
          stop_alarm_blink();
          }
          //otherwise the LED blinking continues because there is at least one alarm in the queue 
        }
    }

    //red LED blinks when the alarm is active
    if(ev == PROCESS_EVENT_TIMER && data == &blink_timer) {
      if(alarm_pending) {
        leds_single_toggle(LEDS_RED);
        printf("\xF0\x9F\x94\x94 ALARM\n");
      }
      etimer_reset(&blink_timer);
    }

    if((ev == PROCESS_EVENT_TIMER && data == &periodic_timer) ||
       ev == PROCESS_EVENT_POLL) {

      if(state == STATE_INIT) {
        if(have_connectivity()) {
          state = STATE_NET_OK;
        }
      }

      if(state == STATE_NET_OK) {
        LOG_INFO("Connecting to MQTT broker\n");
        memcpy(broker_address, broker_ip, strlen(broker_ip));

        mqtt_connect(&conn, broker_address, DEFAULT_BROKER_PORT,
                      (DEFAULT_PUBLISH_INTERVAL * 3) / CLOCK_SECOND,
                      MQTT_CLEAN_SESSION_ON);
        state = STATE_CONNECTING;
      }

      if(state == STATE_CONNECTING) {
        connecting_ticks++;

        if(connecting_ticks >= CONNECTING_TIMEOUT_INTERVAL) {
          LOG_ERR("MQTT connecting timeout\n");
          mqtt_disconnect(&conn);
          enter_reconnect_or_manual_restart();
        }
      }

      if(state == STATE_CONNECTED) {
        //subscribes to all patients' alarms using a wildcard
        status = mqtt_subscribe(&conn, NULL, "alarm/#", MQTT_QOS_LEVEL_1);
        if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
          LOG_ERR("Tried to subscribe but command queue was full!\n");
          PROCESS_EXIT();
        }
      }

      if(state == STATE_SUBSCRIBED) {
        publish_counter++;
        if(publish_counter >= publish_every_n_ticks) {
          publish_counter = 0;
          publish_heartbeat();
        }
      }
      
      if(state == STATE_RECONNECTING_FAST || state == STATE_RECONNECTING_SLOW) {
        unsigned long interval;

        interval = (state == STATE_RECONNECTING_FAST) ? FAST_RECONNECT_INTERVAL : SLOW_RECONNECT_INTERVAL;

        reconnect_ticks++;
        if(reconnect_ticks >= interval) {
          reconnect_ticks = 0;

          if(have_connectivity()) {
            reconnect_attempts++;

            LOG_INFO("MQTT reconnect attempt %u/%u\n",
                     reconnect_attempts, MAX_TOTAL_RECONNECT_ATTEMPTS);

            snprintf(broker_address, CONFIG_IP_ADDR_STR_LEN, "%s", broker_ip);

            connecting_ticks = 0;

            mqtt_connect(&conn, broker_address, DEFAULT_BROKER_PORT,
                         (DEFAULT_PUBLISH_INTERVAL * 3) / CLOCK_SECOND,
                         MQTT_CLEAN_SESSION_ON);

            state = STATE_CONNECTING;
          } else {
            LOG_INFO("No IPv6 route yet. Waiting before next reconnect attempt\n");
          }
        }
      }
      
      if(state == STATE_MANUAL_RESTART_REQUIRED) {
        LOG_ERR("Caregiver node offline. Manual restart required: switch node OFF and ON. MQTT retries stopped to save battery.\n");
      }

      etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
    }
  }

  PROCESS_END();
}