#include "coap-engine.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int  get_publish_period(void);
extern void set_publish_period(int seconds);

static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset);
static void res_put_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset);

RESOURCE(res_config, "title=\"Node config\";rt=\"Control\"", res_get_handler, NULL, res_put_handler, NULL);

static void
res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  int length = snprintf((char *)buffer, preferred_size, "{\"rate\":%d}", get_publish_period());
  coap_set_payload(response, buffer, length);
}

static void
res_put_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  const char *rate_str = NULL;
  int len = coap_get_post_variable(request, "rate", &rate_str);

  if(len > 0) {
    set_publish_period(atoi(rate_str));
    coap_set_status_code(response, CHANGED_2_04);
  } else {
    coap_set_status_code(response, BAD_REQUEST_4_00);
  }
}