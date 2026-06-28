#include "patient_tinyml.h"

#include "fall_feature_scaler.h"
#include "fall_model.h"
#include "eml_net.h"

#include <float.h>
#include <stdint.h>

#define FEATURES_PER_SENSOR 7
#define NUM_FEATURES FALL_NUM_FEATURES
#define NUM_CLASSES 2

static float last_score = 0.0f;

static float square_magnitude(int16_t x, int16_t y, int16_t z) {
  float xf = (float)x;
  float yf = (float)y;
  float zf = (float)z;

  return xf * xf + yf * yf + zf * zf;
}

static void calculate_signal_features(const float *signal, uint8_t n, float *features) {
  uint8_t i;

  float sum = 0.0f;
  float mean;
  float variance_sum = 0.0f;

  float min_value = FLT_MAX;
  float max_value = -FLT_MAX;

  float absolute_difference_sum = 0.0f;
  float maximum_absolute_difference = 0.0f;

  for(i = 0; i < n; i++) {
    float value = signal[i];

    sum += value;

    if(value < min_value) {
      min_value = value;
    }

    if(value > max_value) {
      max_value = value;
    }

    if(i > 0) {
      float difference = value - signal[i - 1];

      if(difference < 0.0f) {
        difference = -difference;
      }

      absolute_difference_sum += difference;

      if(difference > maximum_absolute_difference) {
        maximum_absolute_difference = difference;
      }
    }
  }

  mean = sum / (float)n;

  for(i = 0; i < n; i++) {
    float difference = signal[i] - mean;
    variance_sum += difference * difference;
  }

  features[0] = mean;
  features[1] = variance_sum / (float)n;
  features[2] = min_value;
  features[3] = max_value;
  features[4] = max_value - min_value;

  if(n > 1) {
    features[5] = absolute_difference_sum / (float)(n - 1);
  } else {
    features[5] = 0.0f;
  }

  features[6] = maximum_absolute_difference;
}

static void extract_features(const patient_sample_t *window, uint8_t window_size, float *features) {
  uint8_t i;

  float adxl_magnitude[TINYML_WINDOW_SIZE];
  float gyro_magnitude[TINYML_WINDOW_SIZE];
  float mma_magnitude[TINYML_WINDOW_SIZE];

  for(i = 0; i < window_size; i++) {
    adxl_magnitude[i] = square_magnitude(
      window[i].adxl_x,
      window[i].adxl_y,
      window[i].adxl_z
    );

    gyro_magnitude[i] = square_magnitude(
      window[i].itg_x,
      window[i].itg_y,
      window[i].itg_z
    );

    mma_magnitude[i] = square_magnitude(
      window[i].mma_x,
      window[i].mma_y,
      window[i].mma_z
    );
  }

  calculate_signal_features(
    adxl_magnitude,
    window_size,
    &features[0]
  );

  calculate_signal_features(
    gyro_magnitude,
    window_size,
    &features[FEATURES_PER_SENSOR]
  );

  calculate_signal_features(
    mma_magnitude,
    window_size,
    &features[2 * FEATURES_PER_SENSOR]
  );
}

static void normalize_features(float *features) {
  uint8_t i;

  for(i = 0; i < NUM_FEATURES; i++) {
    if(FALL_FEATURE_SCALE[i] > 0.0f) {
      features[i] =
        (features[i] - FALL_FEATURE_MEAN[i]) / FALL_FEATURE_SCALE[i];
    } else {
      features[i] = 0.0f;
    }
  }
}

static int argmax(const float *values, uint8_t n) {
  uint8_t i;
  uint8_t best = 0;

  for(i = 1; i < n; i++) {
    if(values[i] > values[best]) {
      best = i;
    }
  }

  return (int)best;
}

static void suppress_emlearn_unused_warnings(void) {
  volatile const void *p1 = (const void *)eml_error_str;
  volatile const void *p2 = (const void *)eml_net_activation_function_strs;

  (void)p1;
  (void)p2;
}

int tinyml_predict_window(const patient_sample_t *window,
                          uint8_t window_size) {
  float features[NUM_FEATURES];
  float outputs[NUM_CLASSES];
  int predicted_class;
  uint8_t i;

  suppress_emlearn_unused_warnings();

  if(window == NULL || window_size != TINYML_WINDOW_SIZE) {
    last_score = -FLT_MAX;
    return TINYML_CLASS_NORMAL;
  }

  extract_features(window, window_size, features);
  normalize_features(features);

  for(i = 0; i < NUM_CLASSES; i++) {
    outputs[i] = 0.0f;
  }



  //  outputs[0] = NORMAL
  // outputs[1] = FALL

  eml_net_predict_proba(
    &fall_model,
    features,
    NUM_FEATURES,
    outputs,
    NUM_CLASSES
  );

  predicted_class = argmax(outputs, NUM_CLASSES);
  last_score = outputs[TINYML_CLASS_FALL];

  if(predicted_class == TINYML_CLASS_FALL) {
    return TINYML_CLASS_FALL;
  }

  return TINYML_CLASS_NORMAL;
}

float tinyml_get_last_score(void) {
  return last_score;
}
