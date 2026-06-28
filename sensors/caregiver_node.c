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
#ifndef LEDS_YELLOW
#define LEDS_YELLOW LEDS_GREEN
#endif

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
static char battery_topic[TOPIC_BUFFER_SIZE];
static char registration_topic[TOPIC_BUFFER_SIZE];
static char app_buffer[APP_BUFFER_SIZE];
static char broker_address[CONFIG_IP_ADDR_STR_LEN];
static char ack_topic_buf[TOPIC_BUFFER_SIZE];
static char battery_buffer[APP_BUFFER_SIZE];

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
#define STATE_SUBSCRIBING 8

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

#define CAREGIVER_LOW_BATTERY_INTERVAL     180

#define BATTERY_SIMULATION_PERIOD          (30 * CLOCK_SECOND)
#define BATTERY_DRAIN_STEP_PERCENT         1

#define BATTERY_LOW_20                     20
#define BATTERY_LOW_10                     10
#define BATTERY_LOW_5                      5

#define YELLOW_BLINK_ON_TIME               (5 * CLOCK_SECOND)
#define YELLOW_BLINK_OFF_TIME              (3 * CLOCK_SECOND)

static struct etimer battery_timer;
static struct etimer yellow_blink_timer;

static uint8_t battery_level = 50;

static uint8_t battery_notified_20 = 0;
static uint8_t battery_notified_10 = 0;
static uint8_t battery_notified_5 = 0;

static uint8_t low_battery_mode = 0;
static uint8_t yellow_blink_active = 0;
static uint8_t yellow_led_on = 0;
static uint8_t battery_dead = 0;
static uint8_t pending_battery_level = 0;
static uint8_t pending_battery_notify = 0;

static uint8_t pending_mqtt_registration = 1;

#define MQTT_REGISTRATION_RETRY_INTERVAL (2 * CLOCK_SECOND / STATE_MACHINE_PERIODIC)

static unsigned long mqtt_registration_retry_counter = 0;

#define BATTERY_NOTIFY_RETRY_INTERVAL (10 * CLOCK_SECOND / STATE_MACHINE_PERIODIC)
static unsigned long battery_notify_retry_counter = 0;

#define MAX_COAP_REG_ATTEMPTS 3
#define COAP_REG_RETRY_INTERVAL (5 * CLOCK_SECOND)

static uint8_t coap_registered = 0;
static uint8_t coap_registration_attempts = 0;

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


#define MAX_PATIENTS 4

#define PATIENT_EVENT_NONE          0
#define PATIENT_EVENT_FALL          1
#define PATIENT_EVENT_NODE_CRITICAL 2

typedef struct {
  char node_id[NODE_ID_BUFFER_SIZE];
  uint8_t used;
  uint8_t has_fall;
  uint8_t is_critical;
  uint8_t fall_seen;
  uint8_t critical_seen;
  uint16_t fall_order;
  uint16_t critical_order;
} patient_status_t;

static patient_status_t patients[MAX_PATIENTS];
static uint16_t next_event_order = 1;

static uint8_t alarm_pending = 0;

static void start_alarm_blink(void) {
  if(!alarm_pending) {
    alarm_pending = 1;
    printf("\xF0\x9F\x94\x94 ALARM\n");
  }
}
static void stop_alarm_blink(void) {
  alarm_pending = 0;
  leds_off(LEDS_RED);
}

static void debug_patient_table(void) {
  uint8_t i;

  LOG_INFO("---- Patient table ----\n");

  for(i = 0; i < MAX_PATIENTS; i++) {
    if(patients[i].used) {
      LOG_INFO("[%u] node_id=%s has_fall=%u is_critical=%u fall_seen=%u critical_seen=%u fall_order=%u critical_order=%u\n", i, patients[i].node_id, patients[i].has_fall, patients[i].is_critical, patients[i].fall_seen, patients[i].critical_seen, patients[i].fall_order, patients[i].critical_order);
    }
  }

  LOG_INFO("-----------------------\n");
}

//red LED blinks during the alarm
#define ALARM_BLINK_PERIOD (CLOCK_SECOND / 4)
static struct etimer blink_timer;

PROCESS(caregiver_process, "Caregiver node");
AUTOSTART_PROCESSES(&caregiver_process);

static uint8_t publish_mqtt_registration(void) {
  mqtt_status_t publish_status;

  if(state != STATE_SUBSCRIBED) {
    LOG_INFO("Cannot publish MQTT registration: MQTT not subscribed\n");
    return 0;
  }

  set_publish_period(CAREGIVER_DEFAULT_PUBLISH_INTERVAL/ CLOCK_SECOND);
  publish_counter = publish_every_n_ticks;

  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"caregiver\",\"event\":\"ONLINE\"}", client_id);

  publish_status = mqtt_publish(&conn, NULL, registration_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);

  if(publish_status == MQTT_STATUS_OK) {
    LOG_INFO("MQTT registration queued successfully on %s\n", registration_topic);
    return 1;
  }

  LOG_WARN("MQTT registration could not be queued on %s: status=%d\n", registration_topic, publish_status);
  return 0;
}

//check if the node has network connectivity (global IPv6 address + default route)
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) {
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

static patient_status_t *find_patient(const char *node_id) {
  uint8_t i;

  for(i = 0; i < MAX_PATIENTS; i++) {
    if(patients[i].used &&
       strncmp(patients[i].node_id, node_id, NODE_ID_BUFFER_SIZE) == 0) {
      return &patients[i];
    }
  }

  return NULL;
}

static patient_status_t *get_or_create_patient(const char *node_id) {
  uint8_t i;
  patient_status_t *patient;

  patient = find_patient(node_id);
  if(patient != NULL) {
    return patient;
  }

  for(i = 0; i < MAX_PATIENTS; i++) {
    if(!patients[i].used) {
      memset(&patients[i], 0, sizeof(patient_status_t));
      strncpy(patients[i].node_id, node_id, NODE_ID_BUFFER_SIZE - 1);
      patients[i].node_id[NODE_ID_BUFFER_SIZE - 1] = '\0';
      patients[i].used = 1;
      return &patients[i];
    }
  }

  LOG_ERR("Patient table full, impossible to add %s\n", node_id);
  return NULL;
}

static void release_patient_if_idle(patient_status_t *patient) {
  if(patient != NULL && !patient->has_fall && !patient->is_critical) {
    memset(patient, 0, sizeof(patient_status_t));
  }
}

static uint8_t has_active_patient_events(void) {
  uint8_t i;

  for(i = 0; i < MAX_PATIENTS; i++) {
    if(patients[i].used && (patients[i].has_fall || patients[i].is_critical)) {
      return 1;
    }
  }

  return 0;
}

static uint8_t count_active_patient_events(void) {
  uint8_t i;
  uint8_t count = 0;

  for(i = 0; i < MAX_PATIENTS; i++) {
    if(patients[i].used) {
      if(patients[i].has_fall) {
        count++;
      }
      if(patients[i].is_critical) {
        count++;
      }
    }
  }

  return count;
}

static void clear_patient_table(void) {
  memset(patients, 0, sizeof(patients));
  next_event_order = 1;
}

static void set_patient_fall(const char *node_id) {
  patient_status_t *patient;

  patient = get_or_create_patient(node_id);
  if(patient == NULL) {
    return;
  }

  if(!patient->has_fall) {
    patient->has_fall = 1;
    patient->fall_seen = 0;
    patient->fall_order = next_event_order++;
  }
}

static void clear_patient_fall(const char *node_id) {
  patient_status_t *patient;

  patient = find_patient(node_id);
  if(patient == NULL) {
    return;
  }

  patient->has_fall = 0;
  patient->fall_seen = 0;
  patient->fall_order = 0;

  release_patient_if_idle(patient);
}

static void set_patient_critical(const char *node_id) {
  patient_status_t *patient;

  patient = get_or_create_patient(node_id);
  if(patient == NULL) {
    return;
  }

  if(!patient->is_critical) {
    patient->is_critical = 1;
    patient->critical_seen = 0;
    patient->critical_order = next_event_order++;
  }
}

static void clear_patient_critical(const char *node_id) {
  patient_status_t *patient;

  patient = find_patient(node_id);
  if(patient == NULL) {
    return;
  }

  patient->is_critical = 0;
  patient->critical_seen = 0;
  patient->critical_order = 0;

  release_patient_if_idle(patient);
}

static uint8_t get_next_pending_event(char *node_id_out, uint8_t *event_type_out) {
  uint8_t i;
  uint16_t best_order = 0;
  patient_status_t *best_patient = NULL;
  uint8_t best_event_type = PATIENT_EVENT_NONE;

  for(i = 0; i < MAX_PATIENTS; i++) {
    if(!patients[i].used) {
      continue;
    }

    if(patients[i].has_fall && !patients[i].fall_seen) {
      if(best_order == 0 || patients[i].fall_order < best_order) {
        best_order = patients[i].fall_order;
        best_patient = &patients[i];
        best_event_type = PATIENT_EVENT_FALL;
      }
    }

    if(patients[i].is_critical && !patients[i].critical_seen) {
      if(best_order == 0 || patients[i].critical_order < best_order) {
        best_order = patients[i].critical_order;
        best_patient = &patients[i];
        best_event_type = PATIENT_EVENT_NODE_CRITICAL;
      }
    }
  }

  if(best_patient == NULL) {
    return 0;
  }

  memcpy(node_id_out, best_patient->node_id, NODE_ID_BUFFER_SIZE - 1);
  node_id_out[NODE_ID_BUFFER_SIZE - 1] = '\0';
  *event_type_out = best_event_type;

  return 1;
}

static void mark_pending_event_seen(const char *node_id, uint8_t event_type) {
  patient_status_t *patient;

  patient = find_patient(node_id);
  if(patient == NULL) {
    return;
  }

  //LOcal ack -> remove FALL notification from caregiver mem. Patient will stop local alarm after MQTT ack
  if(event_type == PATIENT_EVENT_FALL) {
    patient->has_fall = 0;
    patient->fall_seen = 0;
    patient->fall_order = 0;

  } else if(event_type == PATIENT_EVENT_NODE_CRITICAL) {
    // A critical node cannot be cleared by the caregiver button. The button only marks the notification as seen; the state remains critical until NODE_RECOVERED is received.
    patient->critical_seen = 1;
    patient->critical_order = 0;
  }

  release_patient_if_idle(patient);
}

static void update_alarm_indicator(void) {
  debug_patient_table();

  if(has_active_patient_events()) {
    start_alarm_blink();
  } else {
    stop_alarm_blink();
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

static void enable_low_battery_mode(void) {
  if(!low_battery_mode) {
    low_battery_mode = 1;

    publish_every_n_ticks =
      ((unsigned long)CAREGIVER_LOW_BATTERY_INTERVAL * CLOCK_SECOND) /
      STATE_MACHINE_PERIODIC;

    publish_counter = 0;

    LOG_WARN("Caregiver low battery mode enabled: heartbeat interval changed to %us\n",
             CAREGIVER_LOW_BATTERY_INTERVAL);
  }
}
static void handle_battery_depleted(void) {
  uint8_t old_state;

  if(battery_dead) {
    return;
  }

  battery_dead = 1;
  old_state = state;

  LOG_ERR("Battery depleted. Caregiver node is offline. Manual restart required: switch caregiver node OFF and ON.\n");

  // Stop alarm handling 
  alarm_pending = 0;
  clear_patient_table();

  // Stop battery warning 
  low_battery_mode = 0;
  yellow_blink_active = 0;
  yellow_led_on = 0;

  // Switch off local indicators 
  leds_off(LEDS_RED);
  leds_single_off(LEDS_YELLOW);

  // Stop timers: the node is now logically OFF 
  etimer_stop(&periodic_timer);
  etimer_stop(&battery_timer);
  etimer_stop(&yellow_blink_timer);
  etimer_stop(&blink_timer);

  state = STATE_MANUAL_RESTART_REQUIRED;

  if(old_state == STATE_CONNECTED ||
     old_state == STATE_SUBSCRIBED ||
     old_state == STATE_SUBSCRIBING ||
     old_state == STATE_CONNECTING) {
    mqtt_disconnect(&conn);
  }
}


  
static void check_battery_thresholds(void) {
  if(battery_level <= BATTERY_LOW_5 && !battery_notified_5) {
    enable_low_battery_mode();

    battery_notified_5 = 1;
    battery_notified_10 = 1;
    battery_notified_20 = 1;
    pending_battery_level = battery_level;
    pending_battery_notify = 1;
    battery_notify_retry_counter = BATTERY_NOTIFY_RETRY_INTERVAL;
    LOG_WARN("Battery threshold reached: 5%%\n");
    
  } else if(battery_level <= BATTERY_LOW_10 && !battery_notified_10) {
    enable_low_battery_mode();

    battery_notified_10 = 1;
    battery_notified_20 = 1;
    pending_battery_level = battery_level;
    pending_battery_notify = 1;
    battery_notify_retry_counter = BATTERY_NOTIFY_RETRY_INTERVAL;
    LOG_WARN("Battery threshold reached: 10%%\n");

  } else if(battery_level <= BATTERY_LOW_20 && !battery_notified_20) {
    enable_low_battery_mode();

    battery_notified_20 = 1;
    pending_battery_level = battery_level;
    pending_battery_notify = 1;
    battery_notify_retry_counter = BATTERY_NOTIFY_RETRY_INTERVAL;

    yellow_blink_active = 1;
    yellow_led_on = 1;
    leds_single_on(LEDS_YELLOW);
    etimer_set(&yellow_blink_timer, YELLOW_BLINK_ON_TIME);

    LOG_WARN("Battery threshold reached: 20%%\n");
  }
}

//handles MQTT messages: a FALL event activates caregiver notification
//a false alarm form the patient closes it 
static void pub_handler(const char *topic, uint16_t topic_len, const uint8_t *chunk, uint16_t chunk_len) {
  char node_id[NODE_ID_BUFFER_SIZE];

  if(strncmp(topic, "alarm/", 6) != 0) {
    return;
  }

  if(!extract_node_id(chunk, chunk_len, node_id, NODE_ID_BUFFER_SIZE)) {
    LOG_INFO("Malformed payload, ignored\n");
    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"FALL\"")) {
    LOG_INFO("FALL alarm received from %s\n", node_id);

    set_patient_fall(node_id);
    update_alarm_indicator();

    LOG_INFO("FALL stored in patient table. Active events: %u\n", count_active_patient_events());
    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"RESOLVED\"")) {
    LOG_INFO("Alarm resolved by patient %s\n", node_id);

    clear_patient_fall(node_id);
    update_alarm_indicator();

    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"NODE_CRITICAL\"")) {
    LOG_ERR("Patient node %s is CRITICAL: heartbeat missing\n", node_id);

    set_patient_critical(node_id);
    update_alarm_indicator();

    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"NODE_RECOVERED\"")) {
    LOG_INFO("Patient node %s recovered: heartbeat restored\n", node_id);

    clear_patient_critical(node_id);
    update_alarm_indicator();

    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"BATTERY_LOW\"")) {
    LOG_WARN("Patient node %s has low battery. Notification only, no ACK required.\n", node_id);
    // IMPORTANT:
    // BATTERY_LOW is not pushed in the patient table. The caregiver does not need to press the button
    // No ACK is sent for this event
     
    return;
  }

  LOG_INFO("Unknown event received on alarm topic\n");
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

      if(battery_dead) {
        state = STATE_MANUAL_RESTART_REQUIRED;
        LOG_ERR("MQTT disconnected because caregiver battery is depleted. Manual restart required.\n");
        break;
      }

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
      LOG_INFO("MQTT subscribed\n");
      state = STATE_SUBSCRIBED;

      pending_mqtt_registration = 1;
      mqtt_registration_retry_counter = MQTT_REGISTRATION_RETRY_INTERVAL;

      break;
    }

  default:
    LOG_INFO("Unhandled MQTT event: %i\n", event);
    break;
  }
}

//heartbeat is more relaxed than patient one: it is needed to report to the Cloud App that the node is still active
static void publish_heartbeat(void) {
  if(low_battery_mode) {
    snprintf(app_buffer, APP_BUFFER_SIZE,"{\"node_id\":\"%s\",\"type\":\"caregiver\",\"battery\":%u}",client_id,battery_level);
  } else {
    snprintf(app_buffer, APP_BUFFER_SIZE,"{\"node_id\":\"%s\",\"type\":\"caregiver\"}",client_id);
  }

  mqtt_publish(&conn, NULL, heartbeat_topic,(uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
}

//used to confirm that the caregiver has seen the alarm
//called when the caregiver presses the button
//it stops linking on the patient node
static void publish_ack(const char *node_id) {
  snprintf(ack_topic_buf, TOPIC_BUFFER_SIZE, "alarm/%s/ack", node_id);

  snprintf(app_buffer, APP_BUFFER_SIZE,"{\"node_id\":\"%s\",\"event\":\"ACK\",\"ack_by\":\"caregiver\",\"caregiver_id\":\"%s\"}",node_id,client_id);

  mqtt_publish(&conn, NULL, ack_topic_buf,(uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
}

static uint8_t publish_battery_status(uint8_t level) {
  mqtt_status_t publish_status;

  if(state != STATE_SUBSCRIBED) {
    LOG_INFO("Cannot publish battery status: MQTT not subscribed\n");
    return 0;
  }

  snprintf(battery_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"caregiver\",\"event\":\"BATTERY_LOW\",\"battery\":%u,\"new_rate\":%u}", client_id, level, CAREGIVER_LOW_BATTERY_INTERVAL);
  publish_status = mqtt_publish(&conn, NULL, battery_topic, (uint8_t *)battery_buffer, strlen(battery_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);

  if(publish_status == MQTT_STATUS_OK) {
    LOG_WARN("Battery low notification sent: %u%%, new_rate=%us\n", level, CAREGIVER_LOW_BATTERY_INTERVAL);
    return 1;
  }

  LOG_WARN("Battery low notification could not be queued: status=%d\n", publish_status);
  return 0;
}

//callback to handle response to CoAP registration (debug/logging)
static void client_chunk_handler(coap_message_t *response) {
  const uint8_t *chunk;
  int len;

  if(response == NULL) {
    LOG_INFO("Registration: timeout\n");
    coap_registered = 0;
    return;
  }

  len = coap_get_payload(response, &chunk);
  LOG_INFO("Registration response: %.*s\n", len, (char *)chunk);

  coap_registered = 1;
}


PROCESS_THREAD(caregiver_process, ev, data)
{
  coap_endpoint_t server_ep;
  static coap_message_t request[1];

  PROCESS_BEGIN();

  coap_activate_resource(&res_config, "config"); 

  // Client_id from MAC address 
  snprintf(client_id, BUFFER_SIZE, "%02x%02x%02x%02x%02x%02x", linkaddr_node_addr.u8[0], linkaddr_node_addr.u8[1], linkaddr_node_addr.u8[2], linkaddr_node_addr.u8[5],linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);

  LOG_INFO("Caregiver node process started (%s)\n", client_id);

  snprintf(heartbeat_topic, TOPIC_BUFFER_SIZE, "health/%s/heartbeat", client_id);
  snprintf(battery_topic, TOPIC_BUFFER_SIZE, "battery/%s", client_id);
  snprintf(registration_topic, TOPIC_BUFFER_SIZE, "registration/%s", client_id);

  // Bootstrap 
  //The node waits for IPv6/RPL connectivity before attempting CoAP registration

  while(!have_connectivity()) {
    LOG_INFO("Waiting for IPv6/RPL connectivity before CoAP registration\n");
    PROCESS_PAUSE();
  }

  // CoAP registration 
  // The registration is blocking, but only during bootstrap
  // and the node will start MQTT only after successful CoAP registration
  
  while(!coap_registered && coap_registration_attempts < MAX_COAP_REG_ATTEMPTS) {

    if(!have_connectivity()) {
      LOG_INFO("IPv6/RPL connectivity lost before CoAP registration. Waiting...\n");

      etimer_set(&periodic_timer, COAP_REG_RETRY_INTERVAL);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

      continue;
    }

    coap_registration_attempts++;

    LOG_INFO("Trying CoAP registration, attempt %u/%u\n",coap_registration_attempts, MAX_COAP_REG_ATTEMPTS);

    // Build CoAP registration request   
    coap_endpoint_parse(REGISTRATION_SERVER_EP, strlen(REGISTRATION_SERVER_EP), &server_ep);

    coap_init_message(request, COAP_TYPE_CON, COAP_POST, 0);
    coap_set_header_uri_path(request, REGISTRATION_PATH);

    snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"caregiver\"}", client_id);

    coap_set_payload(request, (uint8_t *)app_buffer, strlen(app_buffer));

    COAP_BLOCKING_REQUEST(&server_ep, request, client_chunk_handler);

    if(!coap_registered) {
      LOG_INFO("CoAP registration failed. Retrying in %u seconds...\n", (unsigned int)(COAP_REG_RETRY_INTERVAL / CLOCK_SECOND));

      etimer_set(&periodic_timer, COAP_REG_RETRY_INTERVAL);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));
    }
  }

  if(!coap_registered) {
    LOG_ERR("CoAP registration failed after %u attempts\n", MAX_COAP_REG_ATTEMPTS);

    LOG_ERR("Unable to connect to the server: check your connection and switch caregiver node OFF and ON\n");

    while(1) {
      PROCESS_YIELD();
    }
  }

  LOG_INFO("CoAP registration completed. Starting MQTT\n");

  // Activate MQTT only after successful CoAP registration

  mqtt_register(&conn, &caregiver_process, client_id, mqtt_event, MAX_TCP_SEGMENT_SIZE);

  state = STATE_INIT;

  etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
  etimer_set(&blink_timer, ALARM_BLINK_PERIOD);
  etimer_set(&battery_timer, BATTERY_SIMULATION_PERIOD);

  while(1) {
    PROCESS_YIELD();

    if(battery_dead) {
      continue;
    }

    //bottone pressed: caregiver confirms the first alarm received 
    if(ev == button_hal_press_event) {
  char confirmed_id[NODE_ID_BUFFER_SIZE];
  uint8_t confirmed_event_type;

  LOG_INFO("PRESSED BUTTON TO HANDLE PATIENT NOTIFICATION\n");

  if(get_next_pending_event(confirmed_id, &confirmed_event_type)) {

    if(confirmed_event_type == PATIENT_EVENT_FALL) {
      LOG_INFO("Caregiver confirms FALL alarm for %s\n", confirmed_id);

      if(state == STATE_SUBSCRIBED) {
        publish_ack(confirmed_id);
      }

      mark_pending_event_seen(confirmed_id, confirmed_event_type);

    } else if(confirmed_event_type == PATIENT_EVENT_NODE_CRITICAL) {
      LOG_WARN("Caregiver has seen NODE_CRITICAL for %s. Patient remains critical until NODE_RECOVERED.\n", confirmed_id);

      mark_pending_event_seen(confirmed_id, confirmed_event_type);
    }

    update_alarm_indicator();

  } else {
    LOG_INFO("No unseen FALL/NODE_CRITICAL notifications to handle\n");
  }
}

    if(ev == PROCESS_EVENT_TIMER && data == &battery_timer) {
      if(battery_level > BATTERY_DRAIN_STEP_PERCENT) {
        battery_level -= BATTERY_DRAIN_STEP_PERCENT;
      } else {
        battery_level = 0;
      }

      LOG_INFO("Caregiver simulated battery level: %u%%\n", battery_level);

      if(battery_level == 0) {
        handle_battery_depleted();
        continue;
      }

      check_battery_thresholds();

      etimer_reset(&battery_timer);
    }

    if(ev == PROCESS_EVENT_TIMER && data == &yellow_blink_timer) {
      if(yellow_blink_active) {
        if(yellow_led_on) {
          leds_single_off(LEDS_YELLOW);
          yellow_led_on = 0;
          etimer_set(&yellow_blink_timer, YELLOW_BLINK_OFF_TIME);
        } else {
          leds_single_on(LEDS_YELLOW);
          yellow_led_on = 1;
          etimer_set(&yellow_blink_timer, YELLOW_BLINK_ON_TIME);
        }
      }
    }

    //red LED blinks when the alarm is active
    if(ev == PROCESS_EVENT_TIMER && data == &blink_timer) {
      if(alarm_pending) {
        leds_toggle(LEDS_RED);
        printf("\xF0\x9F\x94\x94 ALARM\n");
      }
      etimer_reset(&blink_timer);
    }

    if((ev == PROCESS_EVENT_TIMER && data == &periodic_timer) || ev == PROCESS_EVENT_POLL) {
      
      LOG_INFO("DEBUG state=%u battery=%u low_battery=%u battery_dead=%u alarm_pending=%u active_events=%u\n",
      state,
      battery_level,
      low_battery_mode,
      battery_dead,
      alarm_pending,
      count_active_patient_events());

      if(state == STATE_INIT) {
        if(have_connectivity()) {
          state = STATE_NET_OK;
        }
      }

      if(state == STATE_NET_OK) {
        LOG_INFO("Connecting to MQTT broker\n");
        snprintf(broker_address, CONFIG_IP_ADDR_STR_LEN, "%s", broker_ip);

        mqtt_connect(&conn, broker_address, DEFAULT_BROKER_PORT, (DEFAULT_PUBLISH_INTERVAL * 3) / CLOCK_SECOND, MQTT_CLEAN_SESSION_ON);
        state = STATE_CONNECTING;
      }

      if(state == STATE_CONNECTING) {
        connecting_ticks++;

        if(connecting_ticks >= CONNECTING_TIMEOUT_INTERVAL) {
          LOG_ERR("MQTT connecting timeout\n");
          mqtt_disconnect(&conn);
          connecting_ticks = 0;
          enter_reconnect_or_manual_restart();
        }
      }

      if(state == STATE_CONNECTED) {
        status = mqtt_subscribe(&conn, NULL, "alarm/+", MQTT_QOS_LEVEL_1);

        if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
          LOG_ERR("Tried to subscribe but command queue was full!\n");
          PROCESS_EXIT();
        }

        state = STATE_SUBSCRIBING;
      }

      if(state == STATE_SUBSCRIBED) {
        uint8_t mqtt_publish_attempted = 0;

        if(pending_battery_notify) {
          battery_notify_retry_counter++;

          if(battery_notify_retry_counter >= BATTERY_NOTIFY_RETRY_INTERVAL) {
            battery_notify_retry_counter = 0;
            mqtt_publish_attempted = 1;

            if(publish_battery_status(pending_battery_level)) {
              pending_battery_notify = 0;
              LOG_WARN("Pending battery notification delivered\n");
            } else {
              LOG_WARN("Pending battery notification not sent yet: MQTT queue busy\n");
            }
          }
        }

        if(!mqtt_publish_attempted && pending_mqtt_registration) {
          mqtt_registration_retry_counter++;

          if(mqtt_registration_retry_counter >= MQTT_REGISTRATION_RETRY_INTERVAL) {
            mqtt_registration_retry_counter = 0;
            mqtt_publish_attempted = 1;

            if(publish_mqtt_registration()) {
              pending_mqtt_registration = 0;
              LOG_INFO("Pending MQTT registration delivered\n");
            } else {
              LOG_WARN("Pending MQTT registration not sent yet: MQTT queue busy\n");
            }
          }
        }

        if(!mqtt_publish_attempted) {
          publish_counter++;

          if(publish_counter >= publish_every_n_ticks) {
            publish_counter = 0;
            publish_heartbeat();
          }
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

            LOG_INFO("MQTT reconnect attempt %u/%u\n", reconnect_attempts, MAX_TOTAL_RECONNECT_ATTEMPTS);

            snprintf(broker_address, CONFIG_IP_ADDR_STR_LEN, "%s", broker_ip);

            connecting_ticks = 0;

            mqtt_connect(&conn, broker_address, DEFAULT_BROKER_PORT, (DEFAULT_PUBLISH_INTERVAL * 3) / CLOCK_SECOND, MQTT_CLEAN_SESSION_ON);

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