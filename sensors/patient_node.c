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

#include "patient_data_generator.h"
#include "patient_tinyml.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define LOG_MODULE "patient"
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
#define TOPIC_BUFFER_SIZE 96 
#define APP_BUFFER_SIZE 512

static char client_id[BUFFER_SIZE];
static char heartbeat_topic[TOPIC_BUFFER_SIZE];
static char alarm_topic[TOPIC_BUFFER_SIZE];
static char ack_topic[TOPIC_BUFFER_SIZE];
static char registration_topic[TOPIC_BUFFER_SIZE];
static char app_buffer[APP_BUFFER_SIZE];
static char broker_address[CONFIG_IP_ADDR_STR_LEN];
static char battery_topic[TOPIC_BUFFER_SIZE];
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
#define STATE_SUBSCRIBING 7
#define STATE_MANUAL_RESTART_REQUIRED 8

static uint8_t reconnect_attempts = 0;
static unsigned long reconnect_ticks = 0;
static unsigned long connecting_ticks = 0;
static uint8_t alarm_sound = 0;


#define FALL_ALARM_RETRY_INTERVAL (2 * CLOCK_SECOND / STATE_MACHINE_PERIODIC)

#define MAX_TOTAL_RECONNECT_ATTEMPTS 32

static uint8_t alarm_grace_ticks = 0;
#define ALARM_HEARTBEAT_GRACE_TICKS 2 //skip the first 2 ticks of accelerated heartbit after the alarm 
//sometimes the message received in the queue "Alarm" is the one of the heartbit queue

#define MAX_FAST_RECONNECT_ATTEMPTS 5 // Da scegliere
#define FAST_RECONNECT_INTERVAL (5 * CLOCK_SECOND)/(STATE_MACHINE_PERIODIC) 
#define SLOW_RECONNECT_INTERVAL (60 * CLOCK_SECOND)/(STATE_MACHINE_PERIODIC) 
#define CONNECTING_TIMEOUT_INTERVAL (20 * CLOCK_SECOND)/(STATE_MACHINE_PERIODIC)

#define STATE_MACHINE_PERIODIC (CLOCK_SECOND >> 1)
static struct etimer periodic_timer;

#define SAMPLE_PERIOD (CLOCK_SECOND/5)
static struct etimer sampling_timer;

// Batteria

#define BATTERY_SIMULATION_PERIOD      (30 * CLOCK_SECOND)
#define BATTERY_DRAIN_STEP_PERCENT     1                    //ogni 30 secondi si scarica dell'1%

#define BATTERY_LOW_20                 20
#define BATTERY_LOW_10                 10
#define BATTERY_LOW_5                  5

#define NORMAL_STATUS_INTERVAL         30
#define LOW_BATTERY_STATUS_INTERVAL    120

#define YELLOW_BLINK_ON_TIME           (5 * CLOCK_SECOND)
#define YELLOW_BLINK_OFF_TIME          (3 * CLOCK_SECOND)

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
#define BATTERY_NOTIFY_RETRY_INTERVAL (10 * CLOCK_SECOND / STATE_MACHINE_PERIODIC)
static unsigned long battery_notify_retry_counter = 0;

// Mqtt Registration Publish

static uint8_t pending_mqtt_registration = 1;

#define MQTT_REGISTRATION_RETRY_INTERVAL (2 * CLOCK_SECOND / STATE_MACHINE_PERIODIC)

static unsigned long mqtt_registration_retry_counter = 0;


//CoAP registration
#define REGISTRATION_SERVER_EP "coap://[fd00::1]:5683"
#define REGISTRATION_PATH "/registration"

// Coap Registration Logic
#define MAX_COAP_REG_ATTEMPTS 3
#define COAP_REG_RETRY_INTERVAL (5 * CLOCK_SECOND)

static uint8_t coap_registered = 0;
static uint8_t coap_registration_attempts = 0;

static uint8_t pending_alarm_resolved = 0; 
static uint8_t pending_fall_alarm = 0;

#define FALL_ALARM_RETRY_INTERVAL (2 * CLOCK_SECOND / STATE_MACHINE_PERIODIC)
static unsigned long fall_alarm_retry_counter = 0;

#define RESOLVED_RETRY_INTERVAL (5 * CLOCK_SECOND / STATE_MACHINE_PERIODIC)
static unsigned long resolved_retry_counter = 0;

#define ALARM_SPEEDUP_FACTOR 10 //alarm interval becomes equal to publish_every_n_ticks / FACTOR 
//FACTOR is sent by cloud application to regulate the congestion

// Resource CoAP /config
extern coap_resource_t res_config;

//patient status                                        
typedef enum {
  PATIENT_NORMAL = 0,
  PATIENT_FALL = 1
} patient_state_t;

static patient_state_t patient_state = PATIENT_NORMAL;

//periodic_timer: every 0.5s to change state (INIT, NET_OK, etc)
//publication will be done every publish_every_n_ticks tick 
static unsigned long publish_counter = 0;
static unsigned long publish_every_n_ticks = DEFAULT_PUBLISH_INTERVAL / STATE_MACHINE_PERIODIC;

// Window size for sensor data
#define WINDOW_SIZE 20
static patient_sample_t window[WINDOW_SIZE];
static uint8_t window_index = 0;
static uint8_t window_full = 0;
static uint8_t force_fall_sequence = 0;  // 0 NORMAL, 1 FALL
static uint8_t alarm_active = 0;
static uint8_t force_sound_on_fall = 0;

PROCESS(patient_node_process, "Patient node");
AUTOSTART_PROCESSES(&patient_node_process);

static uint8_t publish_mqtt_registration(void) {
  mqtt_status_t publish_status;

  if(state != STATE_SUBSCRIBED) {
    LOG_INFO("Cannot publish MQTT registration: MQTT not subscribed\n");
    return 0;
  }

  snprintf(app_buffer, APP_BUFFER_SIZE,
           "{\"node_id\":\"%s\",\"type\":\"patient\",\"protocol\":\"mqtt\",\"event\":\"ONLINE\"}",
           client_id);

  publish_status = mqtt_publish(&conn, NULL, registration_topic,
                                (uint8_t *)app_buffer,
                                strlen(app_buffer),
                                MQTT_QOS_LEVEL_0,
                                MQTT_RETAIN_OFF);

  if(publish_status == MQTT_STATUS_OK) {
    LOG_INFO("MQTT registration queued successfully on %s\n", registration_topic);
    return 1;
  }

  LOG_WARN("MQTT registration could not be queued on %s: status=%d\n",
           registration_topic, publish_status);
  return 0;
}

//check if the node has network connectivity (global IPv6 address + default route)
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL ||
     uip_ds6_defrt_choose() == NULL) {
    return false;
  }
  return true;
}

static unsigned long get_alarm_interval_ticks(void) {
  unsigned long ticks = publish_every_n_ticks / ALARM_SPEEDUP_FACTOR;
  return (ticks < 1) ? 1 : ticks;
}

static patient_state_t check_patient_status(void) {
  int predicted_class;

  if(!window_full) {
    return PATIENT_NORMAL;
  }

  predicted_class = tinyml_predict_window( window, WINDOW_SIZE);

  if(predicted_class == TINYML_CLASS_FALL) {
    return PATIENT_FALL;
  }

  return PATIENT_NORMAL;
}

static void reset_window(void){
  window_index = 0;
  window_full = 0;
}

static void  add_sample_to_window(patient_sample_t sample){
  window[window_index] = sample;
  window_index++;

  if(window_index >= WINDOW_SIZE) {
    window_index = 0;
    window_full = 1;
  }
}

//actual publication rate heartbit in seconds (used for GET/config)
int get_publish_period(void) {
  return (int)((publish_every_n_ticks * STATE_MACHINE_PERIODIC) / CLOCK_SECOND);
}

//sets a new rate (used for PUT /config - to handle te adaptive mechanism of the congestion)
void set_publish_period(int seconds) {
  if(seconds > 0) {
    publish_every_n_ticks = (unsigned long)seconds * CLOCK_SECOND / STATE_MACHINE_PERIODIC;
    LOG_INFO("Rate aggiornato dalla Cloud App: %d s\n", seconds);
  }
}

//notifies Cloud App that the alarm has been resolved so that the application closes the active record in alarms
//used by the patient to notify that he/she is ok 
//used also by the caregiver that has served the alarm request
static uint8_t publish_alarm_resolved(void) {
  mqtt_status_t publish_status;

  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"event\":\"RESOLVED\"}", client_id);

  publish_status = mqtt_publish(&conn, NULL, alarm_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_1, MQTT_RETAIN_OFF);

  if(publish_status == MQTT_STATUS_OK) {
    LOG_WARN("RESOLVED event queued successfully\n");
    return 1;
  }

  LOG_ERR("RESOLVED event could not be queued: status=%d\n", publish_status);
  return 0;
}

static uint8_t chunk_contains(const uint8_t *chunk, uint16_t chunk_len, const char *needle) {
  uint16_t needle_len = strlen(needle);
  uint16_t i;

  if(chunk == NULL || needle == NULL || needle_len == 0 || chunk_len < needle_len) {
    return 0;
  }

  for(i = 0; i <= chunk_len - needle_len; i++) {
    if(memcmp(chunk + i, needle, needle_len) == 0) {
      return 1;
    }
  }

  return 0;
}

//handles MQTT messagges: if an ack on the alarm arrives, node status comes back to NORMAL
static void pub_handler(const char *topic, uint16_t topic_len, const uint8_t *chunk, uint16_t chunk_len) {

  if(topic_len != strlen(ack_topic) || strncmp(topic, ack_topic, topic_len) != 0) {
    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"CAREGIVER_CRITICAL\"")) {
    force_sound_on_fall = 1;
    LOG_WARN("Caregiver is CRITICAL: local sound will be enabled on fall\n");

    if(alarm_active && !alarm_sound) {
      alarm_sound = 1;
      LOG_INFO("Alarm Sound ON because caregiver is critical\n");
    }

    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"CAREGIVER_RECOVERED\"")) {
    force_sound_on_fall = 0;

    if(alarm_sound && state == STATE_SUBSCRIBED) {
      alarm_sound = 0;
      LOG_INFO("Caregiver recovered: forced local sound disabled\n");
    }

    return;
  }

  if(chunk_contains(chunk, chunk_len, "\"event\":\"ACK\"")) {
    alarm_active = 0;
    patient_state = PATIENT_NORMAL;
    force_fall_sequence = 0;
    reset_window();
    leds_single_off(LEDS_RED);

    if(alarm_sound) {
      alarm_sound = 0;
      LOG_INFO("Alarm Sound OFF\n");
    }

    LOG_INFO("Allarme confermato dal caregiver\n");
    pending_alarm_resolved = 1;
    return;
  }

  LOG_INFO("Unknown message received on ack topic\n");
}

//publish MQTT heartbeat with status=NORMAL/FALL in JSON format
//QoS is set to 0 since we are publishing periodic heartbits for communiting that the node is still alive
static uint8_t publish_heartbeat(void) {
  const char *state_str = (patient_state == PATIENT_FALL) ? "FALL" : "NORMAL";
  mqtt_status_t publish_status;

  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"patient\",\"state\":\"%s\"}", client_id, state_str);

  publish_status = mqtt_publish(&conn, NULL, heartbeat_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);

  if(publish_status != MQTT_STATUS_OK) {
    LOG_WARN("Heartbeat could not be queued: status=%d\n", publish_status);
    return 0;
  }

  return 1;
}

static uint8_t publish_battery_status(uint8_t level) {
  mqtt_status_t publish_status;

  if(state != STATE_SUBSCRIBED) {
    LOG_INFO("Cannot publish battery status: MQTT not subscribed\n");
    return 0;
  }

  snprintf(battery_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"patient\",\"event\":\"BATTERY_LOW\",\"battery\":%u,\"new_rate\":%u}", client_id, level, LOW_BATTERY_STATUS_INTERVAL);
  publish_status = mqtt_publish(&conn, NULL, battery_topic, (uint8_t *)battery_buffer, strlen(battery_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);

  if(publish_status == MQTT_STATUS_OK) {
    LOG_WARN("Battery low notification sent: %u%%, new_rate=%us\n", level, LOW_BATTERY_STATUS_INTERVAL);
    return 1;
  }

  LOG_WARN("Battery low notification could not be queued: status=%d\n", publish_status);
  return 0;
}

static void enter_reconnect_or_manual_restart(void) {
  reconnect_ticks = 0;
  connecting_ticks = 0;

  if(reconnect_attempts >= MAX_TOTAL_RECONNECT_ATTEMPTS) {
    state = STATE_MANUAL_RESTART_REQUIRED;
    LOG_ERR("MQTT reconnection failed after %u attempts. Manual power cycle required: switch patient node OFF and ON.\n",
            reconnect_attempts);
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

static void enable_low_battery_mode(void) {
  if(!low_battery_mode) {
    low_battery_mode = 1;

    publish_every_n_ticks = ((unsigned long)LOW_BATTERY_STATUS_INTERVAL * CLOCK_SECOND) / STATE_MACHINE_PERIODIC;

    publish_counter = 0;

    LOG_WARN("Low battery mode enabled: NORMAL heartbeat interval set to %u seconds\n", LOW_BATTERY_STATUS_INTERVAL);
  }
}

static void handle_battery_depleted(void) {
  uint8_t old_state;

  if(battery_dead) {
    return;
  }

  battery_dead = 1;
  old_state = state;

  LOG_ERR("Battery depleted. Patient node is offline. Manual restart required: switch patient node OFF and ON.\n");

  alarm_active = 0;
  alarm_sound = 0;
  force_fall_sequence = 0;
  patient_state = PATIENT_NORMAL;

  yellow_blink_active = 0;
  yellow_led_on = 0;

  leds_single_off(LEDS_RED);
  leds_single_off(LEDS_YELLOW);

  etimer_stop(&sampling_timer);
  etimer_stop(&battery_timer);
  etimer_stop(&yellow_blink_timer);

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

//FALL detected - QoS goes to 1 to receive MQTT ACK
//the record in the tabLe will be active until caregiver doesn't confirm
static uint8_t publish_alarm(void) {
  mqtt_status_t publish_status;

  snprintf(app_buffer, APP_BUFFER_SIZE,
           "{\"node_id\":\"%s\",\"event\":\"FALL\"}",
           client_id);

  publish_status = mqtt_publish(&conn, NULL, alarm_topic,
                                (uint8_t *)app_buffer,
                                strlen(app_buffer),
                                MQTT_QOS_LEVEL_1,
                                MQTT_RETAIN_OFF);

  if(publish_status == MQTT_STATUS_OK) {
    LOG_WARN("FALL alarm queued successfully\n");
    return 1;
  }

  LOG_ERR("FALL alarm could not be queued: status=%d\n", publish_status);
  return 0;
}

//callback function called when an MQTT event arrives
//updates status / calls pub_handler if a message has been received
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
        LOG_ERR("MQTT disconnected because battery is depleted. Manual restart required.\n");
        break;
      }

      enter_reconnect_or_manual_restart();
      process_poll(&patient_node_process);
      break;

    case MQTT_EVENT_PUBLISH: {
      struct mqtt_message *msg = data;
      pub_handler(msg->topic, strlen(msg->topic), msg->payload_chunk, msg->payload_length);
      break;
    }

    case MQTT_EVENT_SUBACK: {
      LOG_INFO("MQTT subscribed\n");
      state = STATE_SUBSCRIBED;

      pending_mqtt_registration = 1;
      mqtt_registration_retry_counter = MQTT_REGISTRATION_RETRY_INTERVAL;

      if(alarm_active) {
        pending_fall_alarm = 1;
        fall_alarm_retry_counter = FALL_ALARM_RETRY_INTERVAL;
      }

      break;
    }

    default:
      LOG_INFO("Unhandled MQTT event: %i\n", event);
      break;
    }
}


//callback t handle response to COAP registration
static void client_chunk_handler(coap_message_t *response) {
  const uint8_t *chunk;
  int len;

  //response did not arrive within the timeout
  if(response == NULL) {
    LOG_INFO("Registration: timeout\n");
    coap_registered = 0;
    return;
  }

  //othrwise we print the response (debug/logging)
  len = coap_get_payload(response, &chunk);
  LOG_INFO("Registration response: %.*s\n", len, (char *)chunk);
  coap_registered = 1;
}



PROCESS_THREAD(patient_node_process, ev, data) {
  coap_endpoint_t server_ep;
  static coap_message_t request[1];

  PROCESS_BEGIN();

  coap_activate_resource(&res_config, "config");


  // Build client_id used in MQTT topics and CoAP registration from MAC address.
  snprintf(client_id, BUFFER_SIZE, "%02x%02x%02x%02x%02x%02x",
           linkaddr_node_addr.u8[0], linkaddr_node_addr.u8[1],
           linkaddr_node_addr.u8[2], linkaddr_node_addr.u8[5],
           linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);

  LOG_INFO("Patient node process started (%s)\n", client_id);


  // Topics based on client_id
  snprintf(heartbeat_topic, TOPIC_BUFFER_SIZE, "health/%s/heartbeat", client_id);
  snprintf(alarm_topic, TOPIC_BUFFER_SIZE, "alarm/%s", client_id);
  snprintf(ack_topic, TOPIC_BUFFER_SIZE, "alarm/%s/ack", client_id);
  snprintf(battery_topic, TOPIC_BUFFER_SIZE, "battery/%s", client_id);
  snprintf(registration_topic, TOPIC_BUFFER_SIZE, "registration/%s", client_id);
  
  // Bootstrap 
  //The node waits for IPv6/RPL connectivity before attempting CoAP registration

  while(!have_connectivity()) {
    LOG_INFO("Waiting for IPv6/RPL connectivity before CoAP registration\n");
    PROCESS_PAUSE();
  }

  // CoAP registration 
  // The registration is blocking during bootstrap
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

    snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"patient\",\"protocol\":\"mqtt\"}", client_id);

    coap_set_payload(request, (uint8_t *)app_buffer, strlen(app_buffer));

    COAP_BLOCKING_REQUEST(&server_ep, request, client_chunk_handler);

    if(!coap_registered) {
      LOG_INFO("CoAP registration failed. Retrying in %u seconds...\n",
               (unsigned int)(COAP_REG_RETRY_INTERVAL / CLOCK_SECOND));

      etimer_set(&periodic_timer, COAP_REG_RETRY_INTERVAL);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));
    }
  }

  if(!coap_registered) {
    LOG_ERR("CoAP registration failed after %u attempts\n",
            MAX_COAP_REG_ATTEMPTS);

    LOG_ERR("Unable to connect to the server: check your connection and switch patient node OFF and ON\n");

    while(1) {
      PROCESS_YIELD();
    }
  }

  LOG_INFO("CoAP registration completed. Starting MQTT\n");

  // Activate MQTT and periodic sampling only after successful CoAP registration

  mqtt_register(&conn, &patient_node_process, client_id, mqtt_event, MAX_TCP_SEGMENT_SIZE);

  state = STATE_INIT;

  etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
  etimer_set(&sampling_timer, SAMPLE_PERIOD);
  etimer_set(&battery_timer, BATTERY_SIMULATION_PERIOD);

  while(1) {
    PROCESS_YIELD();

    if(battery_dead) {
      continue;
    }

    //Button pressed -> false allarm, cancel 
    if(ev == button_hal_press_event && alarm_active) {
      LOG_INFO("FALSE ALLARM PRESSED\n");

      alarm_active = 0;
      patient_state = PATIENT_NORMAL;
      force_fall_sequence = 0;
      reset_window();
      leds_single_off(LEDS_RED);

      if(alarm_sound) {
        alarm_sound = 0;
        LOG_INFO("Alarm Sound OFF\n");
      }

      pending_alarm_resolved = 1;
    }

    //Button pressed >=3s -> FALL
    if(ev == button_hal_periodic_event) {
      button_hal_button_t *btn = (button_hal_button_t *)data;
      if(btn->press_duration_seconds >= 3) {
        if(!alarm_active) { //control that alarm is not active (otherwise we are in the case in which we want to stop the alarm)
          reset_window();
          patient_generator_start_fall_event();
          force_fall_sequence = 1;
          LOG_INFO("Fall sequence generation requested by button\n");
        }
      }
    }

    if(ev == PROCESS_EVENT_TIMER && data == &battery_timer) {
      if(battery_level > BATTERY_DRAIN_STEP_PERCENT) {
        battery_level -= BATTERY_DRAIN_STEP_PERCENT;
      } else {
        battery_level = 0;
      }

      LOG_INFO("Simulated battery level: %u%%\n", battery_level);

      if(battery_level == 0) {
        handle_battery_depleted();
        continue;
      }

      check_battery_thresholds();

      etimer_reset(&battery_timer);
    }
    
    // LED giallo acceso 5 secondi
    // LED giallo spento 3 secondi
    // ripete
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

    //Sample Generation
    if(ev == PROCESS_EVENT_TIMER && data == &sampling_timer) {
      patient_sample_t sample;
      patient_state_t predicted_state;

      if(force_fall_sequence) {
        sample = patient_generate_sample(PATIENT_MODE_FALL);
      } else {
        sample = patient_generate_sample(PATIENT_MODE_NORMAL);
      }

      add_sample_to_window(sample);

      // Classification is possible only if the window is full

      if(window_full) { 
        predicted_state = check_patient_status();

        if(predicted_state == PATIENT_FALL) {
          patient_state = PATIENT_FALL;
          LOG_INFO("FALL detected, patient state changed to FALL\n");
          leds_single_on(LEDS_RED);
          if((state != STATE_SUBSCRIBED || force_sound_on_fall) && !alarm_sound){ // in tutti i casi in cui non può inviare messaggi, oppure se il caregiver è CRITICAL 
            alarm_sound = 1;
            LOG_INFO("Alarm Sound ON!\n"); // Simulare suono allarme
          }

          if(!alarm_active) {
            alarm_active = 1;
            alarm_grace_ticks = ALARM_HEARTBEAT_GRACE_TICKS;

            pending_fall_alarm = 1;
            fall_alarm_retry_counter = FALL_ALARM_RETRY_INTERVAL;

            LOG_WARN("FALL detected: alarm set as pending\n");
          }

        } else {
          if(!alarm_active) {
            patient_state = PATIENT_NORMAL;
            leds_single_off(LEDS_RED);
          }
        }
        reset_window();
      }
      
      // If the FALL sequence ended, force_fall_sequence is set back to 0
      // Warning: patient_state will be set back to normal only upon 
      // confirmation from the caregiver/false alarm from the patient.
      if(force_fall_sequence && !patient_generator_fall_event_active()) {
        force_fall_sequence = 0;
        LOG_INFO("Fall sequence completed\n");
      }

      etimer_reset(&sampling_timer);
    }

    if((ev == PROCESS_EVENT_TIMER && data == &periodic_timer) || ev == PROCESS_EVENT_POLL) {

      LOG_INFO("DEBUG state=%u battery=%u low_battery=%u battery_dead=%u alarm_active=%u\n",
         state, battery_level, low_battery_mode, battery_dead, alarm_active);

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

      // Codice per evitare di rimanere indefinitely in stato CONNECTING se non arriva mai
      // un mqtt event connected/disconnected
      if(state == STATE_CONNECTING) {
        connecting_ticks++;

        if(connecting_ticks >= CONNECTING_TIMEOUT_INTERVAL) {
          LOG_ERR("MQTT connecting timeout\n");
          mqtt_disconnect(&conn);
          enter_reconnect_or_manual_restart();
        }
      }

      if(state == STATE_CONNECTED) {
        status = mqtt_subscribe(&conn, NULL, ack_topic, MQTT_QOS_LEVEL_0);

        if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
          LOG_ERR("Tried to subscribe but command queue was full!\n");
          PROCESS_EXIT();
        }
        state = STATE_SUBSCRIBING;
      }

      if(state == STATE_SUBSCRIBED) {
        uint8_t mqtt_publish_attempted = 0;

        /* 1. FALL pending: massima priorità */
        if(pending_fall_alarm) {
          fall_alarm_retry_counter++;

          if(fall_alarm_retry_counter >= FALL_ALARM_RETRY_INTERVAL) {
            fall_alarm_retry_counter = 0;
            mqtt_publish_attempted = 1;

            if(publish_alarm()) {
              pending_fall_alarm = 0;
              LOG_WARN("Pending FALL alarm delivered\n");
            } else {
              LOG_WARN("Pending FALL alarm not sent yet: MQTT queue busy\n");
            }
          }
        }

        /* 2. RESOLVED pending */
        if(!mqtt_publish_attempted && pending_alarm_resolved) {
          resolved_retry_counter++;

          if(resolved_retry_counter >= RESOLVED_RETRY_INTERVAL) {
            resolved_retry_counter = 0;
            mqtt_publish_attempted = 1;

            if(publish_alarm_resolved()) {
              pending_alarm_resolved = 0;
              LOG_WARN("Pending RESOLVED delivered\n");
            } else {
              LOG_WARN("Pending RESOLVED not sent yet: MQTT queue busy\n");
            }
          }
        }

        /* 3. BATTERY pending */
        if(!mqtt_publish_attempted && pending_battery_notify) {
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

        /* 4. MQTT registration pending */
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

        /* 5. Heartbeat solo se non ho appena tentato altro */
        if(!mqtt_publish_attempted) {
          publish_counter++;

          if(alarm_active && alarm_grace_ticks > 0) {
            alarm_grace_ticks--;
          } else if(alarm_active) {
            if(publish_counter >= get_alarm_interval_ticks()) {
              publish_counter = 0;
              publish_heartbeat();
            }
          } else if(publish_counter >= publish_every_n_ticks) {
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
        LOG_ERR("Patient node offline. Manual restart required: switch node OFF and ON. MQTT retries stopped to save battery.\n");
      }

      etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
    }

  }
  PROCESS_END();
}