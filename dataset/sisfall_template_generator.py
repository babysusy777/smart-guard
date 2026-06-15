#!/usr/bin/env python3
"""
SisFall -> Contiki generator with:
1. NORMAL behavior generated from quantile-preserving distributions.
2. FALL behavior generated from real fall templates extracted from SisFall,
   downsampled/compressed and perturbed with small noise in Contiki.

This is preferable to simple min/max random generation because:
- NORMAL keeps a coarse approximation of the empirical distribution.
- FALL keeps the temporal structure of a real fall:
  PRE_FALL -> IMPACT -> POST_FALL.

Generated files:
- patient_data_generator.h
- patient_data_generator.c
- generator_metadata.json

Assumed SisFall column order:
0: ADXL345_x
1: ADXL345_y
2: ADXL345_z
3: ITG3200_x
4: ITG3200_y
5: ITG3200_z
6: MMA8451Q_x
7: MMA8451Q_y
8: MMA8451Q_z

Units in generated C code:
- ADXL345: milli-g
- ITG3200: deg/s
- MMA8451Q: milli-g

Example:
python3 sisfall_template_generator.py \
  --dataset ./SisFall_dataset \
  --out ./contiki_generated \
  --num-fall-templates 8 \
  --template-len 20
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np


COLUMN_NAMES = [
    "adxl_x", "adxl_y", "adxl_z",
    "itg_x", "itg_y", "itg_z",
    "mma_x", "mma_y", "mma_z",
]

ADXL_TO_MG = (2 * 16 / (2 ** 13)) * 1000.0
ITG_TO_DPS = (2 * 2000 / (2 ** 16))
MMA_TO_MG = (2 * 8 / (2 ** 14)) * 1000.0

PHYSICAL_LIMITS = {
    "adxl_x": (-16000, 16000),
    "adxl_y": (-16000, 16000),
    "adxl_z": (-16000, 16000),
    "itg_x": (-2000, 2000),
    "itg_y": (-2000, 2000),
    "itg_z": (-2000, 2000),
    "mma_x": (-8000, 8000),
    "mma_y": (-8000, 8000),
    "mma_z": (-8000, 8000),
}


def parse_sisfall_file(path: Path) -> np.ndarray:
    rows: List[List[float]] = []

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            nums = re.findall(r"[-+]?\d+(?:\.\d+)?", line)
            if len(nums) >= 9:
                rows.append([float(x) for x in nums[:9]])

    if not rows:
        return np.empty((0, 9), dtype=np.float32)

    return np.asarray(rows, dtype=np.float32)


def convert_raw_to_units(raw: np.ndarray) -> np.ndarray:
    out = np.empty_like(raw, dtype=np.float32)

    out[:, 0:3] = raw[:, 0:3] * ADXL_TO_MG
    out[:, 3:6] = raw[:, 3:6] * ITG_TO_DPS
    out[:, 6:9] = raw[:, 6:9] * MMA_TO_MG

    return out


def collect_files(dataset_dir: Path) -> Tuple[List[Path], List[Path]]:
    files = sorted(dataset_dir.rglob("*.txt"))

    normal_files = []
    fall_files = []

    for p in files:
        name = p.name.upper()
        if name.startswith("D"):
            normal_files.append(p)
        elif name.startswith("F"):
            fall_files.append(p)

    return normal_files, fall_files


def load_many(paths: Iterable[Path], max_files: int | None = None) -> np.ndarray:
    arrays: List[np.ndarray] = []

    selected = list(paths)
    if max_files is not None:
        selected = selected[:max_files]

    for p in selected:
        raw = parse_sisfall_file(p)
        if raw.shape[0] > 0:
            arrays.append(convert_raw_to_units(raw))

    if not arrays:
        return np.empty((0, 9), dtype=np.float32)

    return np.vstack(arrays)


def clip_i16(name: str, value: float) -> int:
    lo, hi = PHYSICAL_LIMITS[name]
    value = int(round(value))
    return max(lo, min(hi, value))


def compute_normal_quantile_bins(
    normal_data: np.ndarray,
    quantiles: List[float],
) -> Dict[str, List[int]]:
    """
    For each column, compute empirical quantiles.
    Example quantiles:
    [1, 10, 25, 50, 75, 90, 99]

    In C, the generator samples mostly central bins and rarely external bins.
    """
    if normal_data.shape[0] == 0:
        raise ValueError("Empty normal dataset.")

    result: Dict[str, List[int]] = {}

    q_values = np.percentile(normal_data, quantiles, axis=0)

    for col_idx, name in enumerate(COLUMN_NAMES):
        values = [clip_i16(name, q_values[i, col_idx]) for i in range(len(quantiles))]

        # Ensure non-decreasing order after integer rounding.
        for i in range(1, len(values)):
            if values[i] < values[i - 1]:
                values[i] = values[i - 1]

        result[name] = values

    return result


def acceleration_magnitude_for_impact(data: np.ndarray) -> np.ndarray:
    adxl_mag = np.sqrt(np.sum(np.square(data[:, 0:3]), axis=1))
    mma_mag = np.sqrt(np.sum(np.square(data[:, 6:9]), axis=1))
    return np.maximum(adxl_mag, mma_mag)


def extract_fall_window(
    data: np.ndarray,
    sample_rate_hz: int,
    pre_seconds: float,
    post_seconds: float,
) -> np.ndarray:
    """
    Extracts a window centered around the estimated impact index.
    Impact index is the peak acceleration magnitude.
    """
    if data.shape[0] == 0:
        return np.empty((0, 9), dtype=np.float32)

    score = acceleration_magnitude_for_impact(data)
    impact_idx = int(np.argmax(score))

    pre_n = max(1, int(pre_seconds * sample_rate_hz))
    post_n = max(1, int(post_seconds * sample_rate_hz))

    start = max(0, impact_idx - pre_n)
    end = min(data.shape[0], impact_idx + post_n + 1)

    return data[start:end]


def downsample_to_len(window: np.ndarray, template_len: int) -> np.ndarray:
    """
    Compress a variable-length fall window to a fixed number of samples.
    Uses index-based resampling, intentionally simple and deterministic.
    """
    if window.shape[0] == 0:
        return np.empty((0, 9), dtype=np.int16)

    if window.shape[0] == template_len:
        sampled = window
    else:
        indices = np.linspace(0, window.shape[0] - 1, template_len)
        indices = np.round(indices).astype(int)
        sampled = window[indices]

    result = np.empty((template_len, 9), dtype=np.int16)

    for j, name in enumerate(COLUMN_NAMES):
        for i in range(template_len):
            result[i, j] = clip_i16(name, sampled[i, j])

    return result


def build_fall_templates(
    fall_files: List[Path],
    num_templates: int,
    template_len: int,
    sample_rate_hz: int,
    pre_seconds: float,
    post_seconds: float,
    seed: int,
) -> Tuple[np.ndarray, List[str]]:
    """
    Extracts real fall windows, sorts them by impact strength, and selects
    representative templates across the impact-strength distribution.
    """
    candidates: List[Tuple[float, str, np.ndarray]] = []

    for p in fall_files:
        raw = parse_sisfall_file(p)
        if raw.shape[0] == 0:
            continue

        data = convert_raw_to_units(raw)
        window = extract_fall_window(data, sample_rate_hz, pre_seconds, post_seconds)

        if window.shape[0] < 3:
            continue

        score = float(np.max(acceleration_magnitude_for_impact(window)))
        template = downsample_to_len(window, template_len)

        candidates.append((score, p.name, template))

    if not candidates:
        raise RuntimeError("No valid fall templates could be extracted.")

    # Sort by impact score, then select evenly across the distribution.
    candidates.sort(key=lambda x: x[0])

    if len(candidates) <= num_templates:
        selected = candidates
    else:
        idxs = np.linspace(0, len(candidates) - 1, num_templates)
        idxs = np.round(idxs).astype(int)
        selected = [candidates[i] for i in idxs]

    templates = np.stack([x[2] for x in selected], axis=0)
    names = [x[1] for x in selected]

    return templates, names


def c_int_array(values: List[int]) -> str:
    return "{ " + ", ".join(str(v) for v in values) + " }"


def c_quantiles_initializer(q: Dict[str, List[int]]) -> str:
    rows = []
    for name in COLUMN_NAMES:
        rows.append("  " + c_int_array(q[name]))
    return "{\n" + ",\n".join(rows) + "\n}"


def c_template_initializer(templates: np.ndarray) -> str:
    """
    templates shape:
    [NUM_TEMPLATES, TEMPLATE_LEN, 9]
    """
    template_blocks = []

    for t in range(templates.shape[0]):
        rows = []
        for i in range(templates.shape[1]):
            vals = ", ".join(str(int(v)) for v in templates[t, i, :])
            rows.append(f"    {{ {vals} }}")
        template_blocks.append("  {\n" + ",\n".join(rows) + "\n  }")

    return "{\n" + ",\n".join(template_blocks) + "\n}"


def generate_header() -> str:
    return """#ifndef PATIENT_DATA_GENERATOR_H_
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
"""


def generate_source(
    normal_quantiles: Dict[str, List[int]],
    fall_templates: np.ndarray,
    normal_noise: int,
    fall_noise_acc_mg: int,
    fall_noise_gyro_dps: int,
) -> str:
    num_templates = fall_templates.shape[0]
    template_len = fall_templates.shape[1]
    num_quantiles = len(next(iter(normal_quantiles.values())))

    if num_quantiles != 7:
        raise ValueError("This C generator expects exactly 7 quantiles: p01,p10,p25,p50,p75,p90,p99.")

    return f"""#include "patient_data_generator.h"
#include "lib/random.h"

/*
 * Generated from SisFall.
 *
 * NORMAL:
 *   Quantile-based random generation.
 *
 * FALL:
 *   Real fall templates extracted from SisFall, downsampled to fixed length,
 *   then perturbed with small noise.
 *
 * Units:
 *   ADXL345 and MMA8451Q -> milli-g
 *   ITG3200 -> deg/s
 */

#define NUM_FEATURES 9
#define NUM_QUANTILES {num_quantiles}
#define NUM_FALL_TEMPLATES {num_templates}
#define FALL_TEMPLATE_LEN {template_len}

#define NORMAL_NOISE {normal_noise}
#define FALL_NOISE_ACC_MG {fall_noise_acc_mg}
#define FALL_NOISE_GYRO_DPS {fall_noise_gyro_dps}

static const int16_t normal_quantiles[NUM_FEATURES][NUM_QUANTILES] =
{c_quantiles_initializer(normal_quantiles)};

static const patient_sample_t fall_templates[NUM_FALL_TEMPLATES][FALL_TEMPLATE_LEN] =
{c_template_initializer(fall_templates)};

static uint8_t fall_active = 0;
static uint8_t fall_template_id = 0;
static uint8_t fall_index = 0;

static int16_t random_range_i16(int16_t min, int16_t max) {{
  if(max <= min) {{
    return min;
  }}

  return min + (random_rand() % ((uint16_t)(max - min + 1)));
}}

static int16_t add_noise_i16(int16_t value, int16_t noise) {{
  if(noise <= 0) {{
    return value;
  }}

  return value + random_range_i16(-noise, noise);
}}

/*
 * Quantile-preserving normal generation.
 *
 * Quantiles are:
 * q0=p01, q1=p10, q2=p25, q3=p50, q4=p75, q5=p90, q6=p99
 *
 * The generator samples mostly central values and rarely tails:
 * - 60% from [p25, p75]
 * - 25% from [p10, p90]
 * - 15% from [p01, p99]
 */
static int16_t sample_normal_feature(uint8_t feature_id) {{
  uint16_t r = random_rand() % 100;
  const int16_t *q = normal_quantiles[feature_id];

  if(r < 60) {{
    return add_noise_i16(random_range_i16(q[2], q[4]), NORMAL_NOISE);
  }}

  if(r < 85) {{
    return add_noise_i16(random_range_i16(q[1], q[5]), NORMAL_NOISE);
  }}

  return add_noise_i16(random_range_i16(q[0], q[6]), NORMAL_NOISE);
}}

patient_sample_t patient_generate_normal_sample(void) {{
  patient_sample_t s;

  s.adxl_x = sample_normal_feature(0);
  s.adxl_y = sample_normal_feature(1);
  s.adxl_z = sample_normal_feature(2);

  s.itg_x = sample_normal_feature(3);
  s.itg_y = sample_normal_feature(4);
  s.itg_z = sample_normal_feature(5);

  s.mma_x = sample_normal_feature(6);
  s.mma_y = sample_normal_feature(7);
  s.mma_z = sample_normal_feature(8);

  return s;
}}

void patient_generator_start_fall_event(void) {{
  fall_active = 1;
  fall_template_id = random_rand() % NUM_FALL_TEMPLATES;
  fall_index = 0;
}}

uint8_t patient_generator_fall_event_active(void) {{
  return fall_active;
}}

patient_sample_t patient_generate_fall_event_sample(void) {{
  patient_sample_t s;

  if(!fall_active) {{
    return patient_generate_normal_sample();
  }}

  s = fall_templates[fall_template_id][fall_index];

  s.adxl_x = add_noise_i16(s.adxl_x, FALL_NOISE_ACC_MG);
  s.adxl_y = add_noise_i16(s.adxl_y, FALL_NOISE_ACC_MG);
  s.adxl_z = add_noise_i16(s.adxl_z, FALL_NOISE_ACC_MG);

  s.itg_x = add_noise_i16(s.itg_x, FALL_NOISE_GYRO_DPS);
  s.itg_y = add_noise_i16(s.itg_y, FALL_NOISE_GYRO_DPS);
  s.itg_z = add_noise_i16(s.itg_z, FALL_NOISE_GYRO_DPS);

  s.mma_x = add_noise_i16(s.mma_x, FALL_NOISE_ACC_MG);
  s.mma_y = add_noise_i16(s.mma_y, FALL_NOISE_ACC_MG);
  s.mma_z = add_noise_i16(s.mma_z, FALL_NOISE_ACC_MG);

  fall_index++;

  if(fall_index >= FALL_TEMPLATE_LEN) {{
    fall_active = 0;
    fall_index = 0;
  }}

  return s;
}}

patient_sample_t patient_generate_sample(uint8_t mode) {{
  if(mode == PATIENT_MODE_FALL) {{
    if(!fall_active) {{
      patient_generator_start_fall_event();
    }}

    return patient_generate_fall_event_sample();
  }}

  return patient_generate_normal_sample();
}}
"""


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)

    parser.add_argument("--num-fall-templates", type=int, default=8)
    parser.add_argument("--template-len", type=int, default=20)

    parser.add_argument("--sample-rate-hz", type=int, default=200)
    parser.add_argument("--pre-seconds", type=float, default=1.5)
    parser.add_argument("--post-seconds", type=float, default=2.0)

    parser.add_argument("--max-normal-files", type=int, default=None)
    parser.add_argument("--max-fall-files", type=int, default=None)

    parser.add_argument("--normal-noise", type=int, default=0)
    parser.add_argument("--fall-noise-acc-mg", type=int, default=30)
    parser.add_argument("--fall-noise-gyro-dps", type=int, default=5)

    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    if not args.dataset.exists():
        raise FileNotFoundError(f"Dataset directory not found: {args.dataset}")

    args.out.mkdir(parents=True, exist_ok=True)

    normal_files, fall_files = collect_files(args.dataset)

    if args.max_normal_files is not None:
        normal_files = normal_files[: args.max_normal_files]

    if args.max_fall_files is not None:
        fall_files = fall_files[: args.max_fall_files]

    if not normal_files:
        raise RuntimeError("No NORMAL D*.txt files found.")

    if not fall_files:
        raise RuntimeError("No FALL F*.txt files found.")

    print(f"Found NORMAL files: {len(normal_files)}")
    print(f"Found FALL files:   {len(fall_files)}")

    normal_data = load_many(normal_files)

    quantiles = [1, 10, 25, 50, 75, 90, 99]
    normal_quantiles = compute_normal_quantile_bins(normal_data, quantiles)

    fall_templates, selected_template_names = build_fall_templates(
        fall_files=fall_files,
        num_templates=args.num_fall_templates,
        template_len=args.template_len,
        sample_rate_hz=args.sample_rate_hz,
        pre_seconds=args.pre_seconds,
        post_seconds=args.post_seconds,
        seed=args.seed,
    )

    header = generate_header()
    source = generate_source(
        normal_quantiles=normal_quantiles,
        fall_templates=fall_templates,
        normal_noise=args.normal_noise,
        fall_noise_acc_mg=args.fall_noise_acc_mg,
        fall_noise_gyro_dps=args.fall_noise_gyro_dps,
    )

    (args.out / "patient_data_generator.h").write_text(header, encoding="utf-8")
    (args.out / "patient_data_generator.c").write_text(source, encoding="utf-8")

    metadata = {
        "dataset": str(args.dataset),
        "units": {
            "ADXL345": "milli-g",
            "ITG3200": "deg/s",
            "MMA8451Q": "milli-g",
        },
        "conversions": {
            "ADXL345_mg_per_raw_bit": ADXL_TO_MG,
            "ITG3200_deg_s_per_raw_bit": ITG_TO_DPS,
            "MMA8451Q_mg_per_raw_bit": MMA_TO_MG,
        },
        "normal_generation": {
            "method": "quantile_bins",
            "quantiles": quantiles,
            "sampling_rule": {
                "central_p25_p75": 0.60,
                "medium_p10_p90": 0.25,
                "tail_p01_p99": 0.15,
            },
            "files_used": len(normal_files),
        },
        "fall_generation": {
            "method": "real_fall_templates_plus_noise",
            "num_templates": int(fall_templates.shape[0]),
            "template_len": int(fall_templates.shape[1]),
            "pre_seconds": args.pre_seconds,
            "post_seconds": args.post_seconds,
            "sample_rate_hz": args.sample_rate_hz,
            "selected_templates": selected_template_names,
            "files_used": len(fall_files),
            "noise": {
                "accelerometers_mg": args.fall_noise_acc_mg,
                "gyroscope_deg_s": args.fall_noise_gyro_dps,
            },
        },
        "generated_files": [
            "patient_data_generator.h",
            "patient_data_generator.c",
            "generator_metadata.json",
        ],
    }

    (args.out / "generator_metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(f"Generated files in: {args.out}")
    print(" - patient_data_generator.h")
    print(" - patient_data_generator.c")
    print(" - generator_metadata.json")
    print()
    print("Selected fall templates:")
    for name in selected_template_names:
        print(f" - {name}")


if __name__ == "__main__":
    main()
