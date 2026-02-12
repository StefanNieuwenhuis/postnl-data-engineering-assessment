# KPI Definitions: Units & Assumptions

Gold layer KPIs for `route_performance`, with units, formulas, and edge cases.


## 1. delay_minutes

| Property | Value |
|----------|-------|
| **Unit** | Minutes (decimal) |
| **Formula** | `(actual_arrival_epoch - planned_arrival_epoch) / 60` |
| **Range** | Unbounded; negative = early, positive = late |

**Assumptions**

- Timestamps use `norm_actual_arrival` and `norm_planned_arrival` (normalized in silver).
- Time difference is cast to epoch seconds, then divided by 60.

**Edge cases**

| Condition | Result |
|-----------|--------|
| `norm_planned_arrival` or `norm_actual_arrival` is NULL | `delay_minutes` = NULL |
| Actual < planned | Negative value (early arrival) |
| Actual > planned | Positive value (late arrival) |
| Actual = planned | 0 (on time) |


## 2. arrival_status

| Property | Value |
|----------|-------|
| **Unit** | Categorical |
| **Values** | `LATE`, `EARLY`, `ON_TIME`, `NULL` |
| **Formula** | Derived from `delay_minutes` |

**Rules**

| delay_minutes | arrival_status |
|--------------|----------------|
| > 0 | LATE |
| < 0 | EARLY |
| = 0 | ON_TIME |
| NULL | NULL |

**Edge cases**

- Only set when `delay_minutes` is not NULL.

## 3. emission_kg

| Property | Value |
|----------|-------|
| **Unit** | Kilograms of CO₂ equivalent (kg CO₂e) |
| **Formula** | `distance_km × emission_kg_per_km` |

**Assumptions**

- `emission_kg_per_km` from vehicles table = CO₂ per km for the vehicle.
- `distance_km` from routes table = distance for the shipment’s route.

**Edge cases**

| Condition | Result |
|-----------|--------|
| `distance_km` NULL | NULL |
| `emission_kg_per_km` NULL | NULL |
| `distance_km` = 0 | 0 |
| `emission_kg_per_km` = 0 | 0 |


## 4. expected_minutes

| Property | Value |
|----------|-------|
| **Unit** | Minutes |
| **Formula** | `distance_km / avg_speed_kmh × 60` |

**Assumptions**

- `avg_speed_kmh` is the typical speed for the route (km/h).
- Used as the baseline for on-time performance.

**Edge cases**

| Condition | Result |
|-----------|--------|
| `avg_speed_kmh` NULL or ≤ 0 | NULL |
| `distance_km` NULL or < 0 | NULL |
| Division by zero | NULL (guarded) |


## 5. delay_pct

| Property | Value |
|----------|-------|
| **Unit** | Unitless ratio (e.g. 0.05 = 5% delay) |
| **Formula** | `delay_minutes / expected_minutes` |

**Assumptions**

- Positive = late relative to expected duration.
- Negative = early relative to expected duration.

**Edge cases**

| Condition | Result |
|-----------|--------|
| `expected_minutes` NULL or ≤ 0 | NULL |
| `delay_minutes` NULL | NULL |
| Division by zero | NULL (guarded) |

## 6. efficiency_score

| Property | Value |
|----------|-------|
| **Unit** | Scale 0–100 (percentage-like) |
| **Formula** | `min(100, max(0, round(100 × (1 - delay_pct), 2)))` |

**Assumptions**

- 100 = no delay vs expected.
- 0 = heavily delayed.
- Values outside 0–100 are clamped.

**Edge cases**

| Condition | Result |
|-----------|--------|
| `delay_pct` NULL | NULL |
| `delay_pct` > 1 | Score clamped to 0 |
| `delay_pct` < 0 (early) | Score can exceed 100, clamped to 100 |
