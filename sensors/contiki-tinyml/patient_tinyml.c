#include "patient_tinyml.h"
#include "fall_binary_model.h"

#include <float.h>
#include <stdint.h>
#include <stdio.h>

#define FEATURES_PER_SENSOR 7
#define NUM_FEATURES FALL_BINARY_NUM_FEATURES

static float last_score = 0.0f;

static float square_magnitude(int16_t x, int16_t y, int16_t z) {
  float xf = (float)x;
  float yf = (float)y;
  float zf = (float)z;

  return xf * xf + yf * yf + zf * zf;
}

static void calculate_signal_features(const float *signal,
                                      uint8_t n,
                                      float *features) {
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

static void extract_features(const patient_sample_t *window,
                             uint8_t window_size,
                             float *features) {
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

static float calculate_binary_score(const float *features) {
  uint8_t feature_id;
  float score = FALL_BINARY_BIAS;

  for(feature_id = 0; feature_id < FALL_BINARY_NUM_FEATURES; feature_id++) {
    float scale = FALL_BINARY_FEATURE_SCALE[feature_id];
    float normalized_feature;

    if(scale <= 0.0f) {
      continue;
    }

    normalized_feature =
      (features[feature_id] - FALL_BINARY_FEATURE_MEAN[feature_id]) / scale;

    score += FALL_BINARY_WEIGHTS[feature_id] * normalized_feature;
  }

  return score;
}

int tinyml_predict_window(const patient_sample_t *window,
                          uint8_t window_size) {
  float features[NUM_FEATURES];

  if(window == NULL || window_size != TINYML_WINDOW_SIZE) {
    last_score = -FLT_MAX;
    return TINYML_CLASS_NORMAL;
  }

  extract_features(window, window_size, features);

  last_score = calculate_binary_score(features);

  //printf(
  //  "TinyML binary score=%ld threshold=%ld\n",
  //  (long)(last_score * 1000.0f),
  //  (long)(FALL_BINARY_DECISION_THRESHOLD * 1000.0f)
  //);

  if(last_score >= FALL_BINARY_DECISION_THRESHOLD) {
    return TINYML_CLASS_FALL;
  }

  return TINYML_CLASS_NORMAL;
}

float tinyml_get_last_score(void) {
  return last_score;
}