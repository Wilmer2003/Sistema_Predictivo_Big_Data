# Evidencia 3.6 y 3.7 (Anexo C) — IRA, Malaria, SENAMHI

Generado: 2026-11-05 16:39:38

Script fuente: `src/benchmarking/benchmark_rendimiento.py` (reproducible, no requiere HDFS; Spark local[2], 1.5 GB driver). Corre sobre los 3 datasets reales del proyecto.


## 3.6 Optimizacion del rendimiento (los 3 datasets)

**Tecnica aplicada:** `.cache()` del DataFrame tras los filtros, antes de las acciones repetidas (`count`, `groupBy`, `distinct`, `dropDuplicates`). Es la misma tecnica aplicada en `limpiar_ira.py`, `limpiar_malaria.py` y `limpiar_senamhi.py`.


### IRA

| Repeticion | Sin cache (s) | Con cache (s) |
|---|---|---|
| 1 | 2.12 | 1.79 |
| 2 | 1.8 | 1.63 |
| 3 | 1.58 | 1.56 |
| **Promedio** | **1.83** | **1.66** |

Mejora: **9.5%** menos tiempo con cache. Planes: `plan_sin_cache_IRA.txt` / `plan_con_cache_IRA.txt`.


### MALARIA

| Repeticion | Sin cache (s) | Con cache (s) |
|---|---|---|
| 1 | 0.98 | 1.04 |
| 2 | 0.96 | 0.98 |
| 3 | 0.98 | 0.98 |
| **Promedio** | **0.97** | **1.00** |

Mejora: **-2.7%** menos tiempo con cache. Planes: `plan_sin_cache_MALARIA.txt` / `plan_con_cache_MALARIA.txt`.


### SENAMHI

| Repeticion | Sin cache (s) | Con cache (s) |
|---|---|---|
| 1 | 1.83 | 1.5 |
| 2 | 1.89 | 1.35 |
| 3 | 1.75 | 1.32 |
| **Promedio** | **1.82** | **1.39** |

Mejora: **23.8%** menos tiempo con cache. Planes: `plan_sin_cache_SENAMHI.txt` / `plan_con_cache_SENAMHI.txt`.


## 3.7 Prueba de escalabilidad (los 3 datasets)

Replicas x10 y x100 de cada conjunto real, usadas solo para medir el pipeline (no se usan en el analisis exploratorio, el entrenamiento ni conclusiones de negocio).


### IRA

| Factor | Filas | Tiempo (s) | CPU prom (%) | CPU max (%) | Mem prom (MB) | Mem pico (MB) |
|---|---|---|---|---|---|---|
| x1 | 38086 | 1.56 | 2.6 | 5.8 | 711.7 | 860.7 |
| x10 | 380860 | 3.42 | 1.7 | 3.9 | 818.7 | 943.5 |
| x100 | 3808600 | 17.86 | 1.6 | 5.8 | 917.7 | 953.7 |

- x10: volumen x10, tiempo x2.19 (sublineal/estable).

- x100: volumen x100, tiempo x11.45 (sublineal/estable).

### MALARIA

| Factor | Filas | Tiempo (s) | CPU prom (%) | CPU max (%) | Mem prom (MB) | Mem pico (MB) |
|---|---|---|---|---|---|---|
| x1 | 490 | 0.93 | 3.1 | 7.8 | 886.5 | 1108.3 |
| x10 | 4900 | 1.05 | 2.6 | 7.9 | 925.2 | 1113.6 |
| x100 | 49000 | 1.56 | 2.3 | 5.9 | 960.4 | 1124.2 |

- x10: volumen x10, tiempo x1.13 (sublineal/estable).

- x100: volumen x100, tiempo x1.68 (sublineal/estable).

### SENAMHI

| Factor | Filas | Tiempo (s) | CPU prom (%) | CPU max (%) | Mem prom (MB) | Mem pico (MB) |
|---|---|---|---|---|---|---|
| x1 | 36530 | 1.44 | 2.7 | 3.9 | 926.0 | 1159.3 |
| x10 | 365300 | 3.84 | 2.6 | 5.9 | 1043.5 | 1162.2 |
| x100 | 3653000 | 17.87 | 1.6 | 3.9 | 1155.3 | 1253.5 |

- x10: volumen x10, tiempo x2.67 (sublineal/estable).

- x100: volumen x100, tiempo x12.41 (sublineal/estable).