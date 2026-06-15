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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define LOG_MODULE "patient"
#define LOG_LEVEL  LOG_LEVEL_INFO

//MQTT BROKER
#define MQTT_CLIENT_BROKER_IP_ADDR "fd00::1"
static const char *broker_ip = MQTT_CLIENT_BROKER_IP_ADDR;

#define DEFAULT_BROKER_PORT       1883
#define DEFAULT_PUBLISH_INTERVAL  (30 * CLOCK_SECOND)

#define MAX_TCP_SEGMENT_SIZE      32
#define CONFIG_IP_ADDR_STR_LEN    64
#define BUFFER_SIZE               64
#define APP_BUFFER_SIZE           512

static char client_id[BUFFER_SIZE];
static char heartbeat_topic[BUFFER_SIZE];
static char alarm_topic[BUFFER_SIZE];
static char ack_topic[BUFFER_SIZE];
static char app_buffer[APP_BUFFER_SIZE];
static char broker_address[CONFIG_IP_ADDR_STR_LEN];

static struct mqtt_connection conn;
mqtt_status_t status;

static uint8_t state;
#define STATE_INIT          0
#define STATE_NET_OK        1
#define STATE_CONNECTING    2
#define STATE_CONNECTED     3
#define STATE_SUBSCRIBED    4
#define STATE_DISCONNECTED  5

#define STATE_MACHINE_PERIODIC (CLOCK_SECOND >> 1)
static struct etimer periodic_timer;

#define SAMPLE_PERIOD (CLOCK_SECOND/4)
static struct etimer sampling_timer;

//CoAP registration
#define REGISTRATION_SERVER_EP "coap://[fd00::1]:5683"
#define REGISTRATION_PATH      "/registration"

//patient status                                        
typedef enum {
  PATIENT_NORMAL  = 0,
  PATIENT_FALL    = 1
} patient_state_t;

static patient_state_t patient_state = PATIENT_NORMAL;

//periodic_timer: every 0.5s to change state (INIT, NET_OK, etc)
//publication will be done every PUBLISH_EVERY_N_TICKS tick (= DEFAULT_PUBLISH_INTERVAL)
static unsigned long publish_counter = 0;
#define PUBLISH_EVERY_N_TICKS (DEFAULT_PUBLISH_INTERVAL / STATE_MACHINE_PERIODIC)

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
static bool
have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL ||
     uip_ds6_defrt_choose() == NULL) {
    return false;
  }
  return true;
}


static patient_state_t
check_patient_status(void) {

  // By default, NORMAL is generated.
  // If force_fall_sequence == 1 due to the Button Press event,
  // a FALL sequence is generated.

  if(!window_full) {
    return PATIENT_NORMAL;
  }
  if(force_fall_sequence) {
    return PATIENT_FALL;
  }

  return PATIENT_NORMAL;


  // TODO TinyML
  
   
  //predicted_class = tinyml_predict_window(window, WINDOW_SIZE);
  // if(predicted_class == 1) {
  //   force_fall_sequence = 0;
  //   return PATIENT_FALL;
  // }
  // else{
  //   return PATIENT_NORMAL;
  // }
    
}

static void reset_window(void){
  window_index = 0;
  window_full = 0;
}

static void add_sample_to_window(patient_sample_t sample){
  
  window[window_index] = sample;
  window_index++;

  if(window_index >= WINDOW_SIZE) {
    window_index = 0;
    window_full = 1;
  }
}

//handles MQTT messagges: if an ack on the alarm arrives, node status comes back to NORMAL
static void
pub_handler(const char *topic, uint16_t topic_len, const uint8_t *chunk, uint16_t chunk_len) {
  if(topic_len == strlen(ack_topic) && strncmp(topic, ack_topic, topic_len) == 0) {
    alarm_active= 0;
    patient_state = PATIENT_NORMAL;
    force_fall_sequence = 0;
    reset_window();
    leds_single_off(LEDS_RED);
    LOG_INFO("Allarme confermato dal caregiver\n");
  }
}

//callback function called when an MQTT event arrives
//updates status / calls pub_handler if a message has been received
static void
mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
  case MQTT_EVENT_CONNECTED:
    LOG_INFO("MQTT connected\n");
    state = STATE_CONNECTED;
    break;

  case MQTT_EVENT_DISCONNECTED:
    LOG_INFO("MQTT disconnected. Reason %u\n", *((mqtt_event_t *)data));
    state = STATE_DISCONNECTED;
    process_poll(&patient_node_process);
    break;

  case MQTT_EVENT_PUBLISH: {
    struct mqtt_message *msg = data;
    pub_handler(msg->topic, strlen(msg->topic), msg->payload_chunk, msg->payload_length);
    break;
  }

  case MQTT_EVENT_SUBACK:
    LOG_INFO("MQTT subscribed\n");
    state = STATE_SUBSCRIBED;
    break;

  default:
    LOG_INFO("Unhandled MQTT event: %i\n", event);
    break;
  }
}

//publish MQTT heartbeat with status=NORMAL/FALL in JSON format
//QoS is set to 0 since we are publishing periodic heartbits for communiting that the node is still alive
static void
publish_heartbeat(void) {
  const char *state_str = (patient_state == PATIENT_FALL) ? "FALL" : "NORMAL";

  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"state\":\"%s\"}", client_id, state_str);
  mqtt_publish(&conn, NULL, heartbeat_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
}

//FALL detected - QoS goes to 1 to receive MQTT ACK
//the record in the tabLe will be active until caregiver doesn't confirm
static void
publish_alarm(void) {
  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"event\":\"FALL\"}", client_id);
  mqtt_publish(&conn, NULL, alarm_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_1, MQTT_RETAIN_OFF);
}

//notifies Cloud App that the alarm has been resolved so that the application closes the active record in alarms
//used by the patient to notify that he/she is ok 
//used also by the caregiver that has served the alarm request
static void
publish_alarm_resolved(void) {
  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"event\":\"RESOLVED\"}", client_id);
  mqtt_publish(&conn, NULL, alarm_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_1, MQTT_RETAIN_OFF);
}

//callback t handle response to COAP registration
static void
client_chunk_handler(coap_message_t *response) {
  const uint8_t *chunk;
  int len;

  //response did not arrive within the timeout
  if(response == NULL) {
    LOG_INFO("Registration: timeout\n");
    return;
  }

  //othrwise we print the response (debug/logging)
  len = coap_get_payload(response, &chunk);
  LOG_INFO("Registration response: %.*s\n", len, (char *)chunk);
}

//registration to cloud application
static void
register_to_cloud(void) {
  coap_endpoint_t server_ep; //server
  static coap_message_t request[1]; //CoAP message

  coap_endpoint_parse(REGISTRATION_SERVER_EP, strlen(REGISTRATION_SERVER_EP), &server_ep);

  //CON message
  coap_init_message(request, COAP_TYPE_CON, COAP_POST, 0);
  coap_set_header_uri_path(request, REGISTRATION_PATH);

  snprintf(app_buffer, APP_BUFFER_SIZE, "{\"node_id\":\"%s\",\"type\":\"patient\",\"protocol\":\"mqtt\"}", client_id);
  coap_set_payload(request, (uint8_t *)app_buffer, strlen(app_buffer));

  COAP_BLOCKING_REQUEST(&server_ep, request, client_chunk_handler);
}


PROCESS_THREAD(patient_node_process, ev, data)
{
  PROCESS_BEGIN();

  //client_id dal MAC address - node identity
  snprintf(client_id, BUFFER_SIZE, "%02x%02x%02x%02x%02x%02x",
           linkaddr_node_addr.u8[0], linkaddr_node_addr.u8[1],
           linkaddr_node_addr.u8[2], linkaddr_node_addr.u8[5],
           linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);

  LOG_INFO("Patient node process started (%s)\n", client_id);

  //topics based on client_id
  snprintf(heartbeat_topic, BUFFER_SIZE, "health/%s/heartbeat", client_id);
  snprintf(alarm_topic,     BUFFER_SIZE, "alarm/%s",            client_id);
  snprintf(ack_topic,       BUFFER_SIZE, "alarm/%s/ack",        client_id);

  mqtt_register(&conn, &patient_node_process, client_id, mqtt_event, MAX_TCP_SEGMENT_SIZE);

  state = STATE_INIT;
  etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
  etimer_set(&sampling_timer, SAMPLE_PERIOD);

  while(1) {
    PROCESS_YIELD();

    //Button pressed -> FALL
    if(ev == button_hal_press_event) {
       if(!alarm_active) { //control that alarm is not active (otherwise we are in the case in which we want to stop the alarm)
        reset_window();
        patient_generator_start_fall_event();
        force_fall_sequence = 1;
        LOG_INFO("Fall sequence generation requested by button\n");
      }
    }

    // Button pressed >=3s during FALL -> false allarm, cancel 
    if(ev == button_hal_periodic_event) {

      button_hal_button_t *btn = (button_hal_button_t *)data;
      if(btn->press_duration_seconds >= 3 && alarm_active) {
        alarm_active = 0;
        patient_state = PATIENT_NORMAL;
        force_fall_sequence = 0;
        reset_window();
        leds_single_off(LEDS_RED);

        if(state == STATE_SUBSCRIBED) {
          publish_alarm_resolved();
        }
      }
    }

    // Sample Generation
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
          leds_single_on(LEDS_RED);

          if(!alarm_active) {
            alarm_active = 1;
        
            if(state == STATE_SUBSCRIBED) {
              publish_alarm();
            }
          }
        } else {
          if(!alarm_active) {
            patient_state = PATIENT_NORMAL;
            leds_single_off(LEDS_RED);
          }
        }
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

      if(state == STATE_CONNECTED) {
        status = mqtt_subscribe(&conn, NULL, ack_topic, MQTT_QOS_LEVEL_0);
        if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
          LOG_ERR("Tried to subscribe but command queue was full!\n");
          PROCESS_EXIT();
        }
        register_to_cloud();
      }

      if(state == STATE_SUBSCRIBED) {
        publish_counter++;
        //if alarm is active -> at each tick we will publish the message in the alarm list 
        if(alarm_active || publish_counter >= publish_every_n_ticks) {
          publish_counter = 0;
          publish_heartbeat();
        }
      } else if(state == STATE_DISCONNECTED) {
        LOG_ERR("Disconnected from MQTT broker\n");
        /* TODO: gestione riconnessione, se necessario */
      }

      etimer_set(&periodic_timer, STATE_MACHINE_PERIODIC);
    }
  }

  PROCESS_END();
}