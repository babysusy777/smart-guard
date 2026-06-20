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
static char app_buffer[APP_BUFFER_SIZE];
static char broker_address[CONFIG_IP_ADDR_STR_LEN];

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

static uint8_t reconnect_attempts = 0;
static unsigned long reconnect_ticks = 0;
static unsigned long connecting_ticks = 0;
static uint8_t reconnect_mode_slow = 0;
static uint8_t alarm_sound = 0;

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

//CoAP registration
#define REGISTRATION_SERVER_EP "coap://[fd00::1]:5683"
#define REGISTRATION_PATH "/registration"

// Coap Registration Logic
#define MAX_COAP_REG_ATTEMPTS 3
#define COAP_REG_RETRY_INTERVAL (5 * CLOCK_SECOND)

static uint8_t coap_registered = 0;
static uint8_t coap_registration_attempts = 0;

static uint8_t pending_alarm_resolved = 0; 

#define ALARM_SPEEDUP_FACTOR 60 //alarm interval becomes equal to publish_every_n_ticks / FACTOR 
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

PROCESS(patient_node_process, "Patient node");
AUTOSTART_PROCESSES(&patient_node_process);

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
static void publish_alarm_resolved(void) {
  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"event\":\"RESOLVED\"}", client_id);
  mqtt_publish(&conn, NULL, alarm_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_1, MQTT_RETAIN_OFF);
}

//handles MQTT messagges: if an ack on the alarm arrives, node status comes back to NORMAL
static void pub_handler(const char *topic, uint16_t topic_len, const uint8_t *chunk, uint16_t chunk_len) {
  if(topic_len == strlen(ack_topic) && strncmp(topic, ack_topic, topic_len) == 0) {
    alarm_active= 0;
    patient_state = PATIENT_NORMAL;
    force_fall_sequence = 0;
    reset_window();
    leds_single_off(LEDS_RED);
    if(alarm_sound){
          alarm_sound = 0;
          LOG_INFO("Alarm Sound OFF\n");
        }
    LOG_INFO("Allarme confermato dal caregiver\n");
    pending_alarm_resolved = 1;
  }
}

//publish MQTT heartbeat with status=NORMAL/FALL in JSON format
//QoS is set to 0 since we are publishing periodic heartbits for communiting that the node is still alive
static void publish_heartbeat(void) {
  const char *state_str = (patient_state == PATIENT_FALL) ? "FALL" : "NORMAL";
  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"state\":\"%s\"}", client_id, state_str);
  mqtt_publish(&conn, NULL, heartbeat_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
}

//FALL detected - QoS goes to 1 to receive MQTT ACK
//the record in the tabLe will be active until caregiver doesn't confirm
static void publish_alarm(void) {
  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"event\":\"FALL\"}", client_id);
  mqtt_publish(&conn, NULL, alarm_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_1, MQTT_RETAIN_OFF);
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
    reconnect_mode_slow = 0;
    state = STATE_CONNECTED;
    break;

  case MQTT_EVENT_DISCONNECTED:
    LOG_INFO("MQTT disconnected. Reason %u\n", *((mqtt_event_t *)data));
    reconnect_ticks = 0;
    connecting_ticks = 0;

    if(reconnect_attempts >= MAX_FAST_RECONNECT_ATTEMPTS) {
      reconnect_mode_slow = 1;
      state = STATE_RECONNECTING_SLOW;
    } else {
      state = STATE_RECONNECTING_FAST;
    }
    process_poll(&patient_node_process);
    break;

  case MQTT_EVENT_PUBLISH: {
    struct mqtt_message *msg = data;
    pub_handler(msg->topic, strlen(msg->topic), msg->payload_chunk, msg->payload_length);
    break;
  }

  case MQTT_EVENT_SUBACK:
    #if MQTT_311
      {
        mqtt_suback_event_t *suback_event = (mqtt_suback_event_t *)data;
        if(suback_event->success) {
          LOG_INFO("MQTT subscribed successfully\n");
          state = STATE_SUBSCRIBED;
          if(alarm_active) {
            publish_alarm();
          }
        } else {
          LOG_ERR("MQTT subscribe FAILED (return code %x)\n", suback_event->return_code);
        }
      }
    #else
      LOG_INFO("MQTT subscribed\n");
      state = STATE_SUBSCRIBED;
      if(alarm_active) {
        publish_alarm();
      }
    #endif
      break;

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

  while(1) {
    PROCESS_YIELD();

    //Button pressed -> false allarm, cancel 
    if(ev == button_hal_press_event && alarm_active) {
      LOG_INFO("FALSE ALLARM PRESSED\n");
      alarm_active = 0;
      patient_state = PATIENT_NORMAL;
      force_fall_sequence = 0;
      reset_window();
      leds_single_off(LEDS_RED);
      if(alarm_sound){
        alarm_sound = 0;
        LOG_INFO("Alarm Sound OFF\n");
      }

      if(state == STATE_SUBSCRIBED) {
        publish_alarm_resolved();
      }
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

    //Sample Generation
    if(ev == PROCESS_EVENT_TIMER && data == &sampling_timer) {
      LOG_INFO("If ev == Sampling Timer\n");
      patient_sample_t sample;
      patient_state_t predicted_state;

      if(force_fall_sequence) {
        LOG_INFO("Force FALL sequence = 1\n");
        sample = patient_generate_sample(PATIENT_MODE_FALL);
        LOG_INFO("Generated FALL sample, index=%u\n", window_index);
      } else {
        LOG_INFO("Force FALL sequence = 0\n");
        sample = patient_generate_sample(PATIENT_MODE_NORMAL);
      }

      add_sample_to_window(sample);

      // Classification is possible only if the window is full

      if(window_full) { 

        LOG_INFO("Window full: running classification\n");
        predicted_state = check_patient_status();

        LOG_INFO("Predicted state: %s\n", predicted_state == PATIENT_FALL ? "FALL" : "NORMAL");

        if(predicted_state == PATIENT_FALL) {
          patient_state = PATIENT_FALL;
          LOG_INFO("FALL detected - current MQTT state: %u\n", state);
          LOG_INFO("Patient state changed to FALL\n");
          leds_single_on(LEDS_RED);
          if(state != STATE_SUBSCRIBED && !alarm_sound){ // in tutti i casi in cui non può inviare messaggi 
            alarm_sound = 1;
            LOG_INFO("Alarm Sound ON!\n"); // Simulare suono allarme
          }

          if(!alarm_active) {
            alarm_active = 1;
            alarm_grace_ticks = ALARM_HEARTBEAT_GRACE_TICKS;

            if(state == STATE_SUBSCRIBED) {
              LOG_INFO("Publishing FALL alarm\n");
              publish_alarm();
            }
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

      // Codice per evitare di rimanere indefinitely in stato CONNECTING se non arriva mai
      // un mqtt event connected/disconnected
      if(state == STATE_CONNECTING) {
        connecting_ticks++;

        if(connecting_ticks >= CONNECTING_TIMEOUT_INTERVAL){
          LOG_INFO("MQTT Connecting timeout\n");

          connecting_ticks = 0;
          reconnect_ticks = 0;

          mqtt_disconnect(&conn);

          if(reconnect_attempts >= MAX_FAST_RECONNECT_ATTEMPTS) {
            reconnect_mode_slow = 1;
            state = STATE_RECONNECTING_SLOW;
            LOG_INFO("Switching to slow reconnect mode\n");
          } else {
            state = STATE_RECONNECTING_FAST;
            LOG_INFO("Retrying MQTT connection in fast reconnect mode\n");
          }
        }
      }

      if(state == STATE_CONNECTED) {
        status = mqtt_subscribe(&conn, NULL, ack_topic, MQTT_QOS_LEVEL_0);
        if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
          LOG_ERR("Tried to subscribe but command queue was full!\n");
          PROCESS_EXIT();
        }
      }

      if(state == STATE_SUBSCRIBED) {
        if(pending_alarm_resolved) {
          pending_alarm_resolved = 0;
          publish_alarm_resolved();
        }
        publish_counter++;
        //if alarm is active -> at each tick we will publish the message in the alarm list 
        if(alarm_active && alarm_grace_ticks > 0) {
          alarm_grace_ticks--;   //skip this tick to avoid collisions
        }else if(alarm_active) {
          if(publish_counter >= get_alarm_interval_ticks()) {
            publish_counter = 0;
            publish_heartbeat();
          }
        } else if(publish_counter >= publish_every_n_ticks) {
          publish_counter = 0;
          publish_heartbeat();
        }
      } 

      if(state == STATE_RECONNECTING_FAST) {
        reconnect_ticks++;

        if(reconnect_ticks >= FAST_RECONNECT_INTERVAL){

          reconnect_ticks = 0;

          if(have_connectivity()){
            
            LOG_INFO("Reconnect attempt %u to MQTT broker\n", reconnect_attempts + 1);

            memcpy(broker_address, broker_ip, strlen(broker_ip));
            reconnect_attempts++;
            connecting_ticks = 0;
            mqtt_connect(&conn, broker_address, DEFAULT_BROKER_PORT, (DEFAULT_PUBLISH_INTERVAL * 3) / CLOCK_SECOND, MQTT_CLEAN_SESSION_ON);
            state = STATE_CONNECTING;
          }
          else {
            LOG_INFO("No IPv6 route yet. Waiting before next reconnect attempt\n");
          }
        }
      }

      if(state == STATE_RECONNECTING_SLOW) {
        reconnect_ticks++;

        if(reconnect_ticks >= SLOW_RECONNECT_INTERVAL) {
          reconnect_ticks = 0;

          if(have_connectivity()) {
            LOG_INFO("Reconnect attempt %u to MQTT broker\n", reconnect_attempts + 1);

            memcpy(broker_address, broker_ip, strlen(broker_ip));
            mqtt_connect(&conn, broker_address, DEFAULT_BROKER_PORT, (DEFAULT_PUBLISH_INTERVAL * 3) / CLOCK_SECOND, MQTT_CLEAN_SESSION_ON);
            reconnect_attempts++;
            connecting_ticks = 0;
            state = STATE_CONNECTING;
          }
          else {
            LOG_INFO("Network routing unavailable\n");
          }
        }
      }

      etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
    }
  }

  PROCESS_END();
}