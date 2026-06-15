#ifndef PATIENT_DATA_GENERATOR_H_
#define PATIENT_DATA_GENERATOR_H_

#include <stdint.h>

#define PATIENT_MODE_NORMAL 0
#define PATIENT_MODE_FALL   1

typedef struct {
  int16_t adxl_x;  /* milli-g */
  int16_t adxl_y;  /* milli-g */
  int16_t adxl_z;  /* milli-g */

  int16_t itg_x;   /* deg/s */
  int16_t itg_y;   /* deg/s */
  int16_t itg_z;   /* deg/s */

  int16_t mma_x;   /* milli-g */
  int16_t mma_y;   /* milli-g */
  int16_t mma_z;   /* milli-g */
} patient_sample_t;

void patient_generator_start_fall_event(void);
uint8_t patient_generator_fall_event_active(void);

patient_sample_t patient_generate_normal_sample(void);
patient_sample_t patient_generate_fall_event_sample(void);

/*
 * If mode == PATIENT_MODE_FALL, this starts or continues a full fall sequence.
 * If mode == PATIENT_MODE_NORMAL, this returns a quantile-based normal sample.
 */
patient_sample_t patient_generate_sample(uint8_t mode);

#endif
