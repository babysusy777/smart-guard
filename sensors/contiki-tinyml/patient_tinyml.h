#ifndef PATIENT_TINYML_H_
#define PATIENT_TINYML_H_

#include "patient_data_generator.h"

#include <stdint.h>

#define TINYML_WINDOW_SIZE 20

#define TINYML_CLASS_NORMAL 0
#define TINYML_CLASS_FALL   1

int tinyml_predict_window(
  const patient_sample_t *window,
  uint8_t window_size
);

float tinyml_get_last_score(void);

#endif /* PATIENT_TINYML_H_ */
