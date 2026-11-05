# Evidencia 3.6 y 3.7 (Anexo C)

Generado: 2026-11-05 16:17:52

Script fuente: `src/benchmarking/benchmark_rendimiento.py` (reproducible, no requiere HDFS; Spark local[2], 1.5 GB driver).


## 3.6 Optimizacion del rendimiento

**Tecnica aplicada:** cacheo (`.cache()`) del DataFrame ya filtrado (Departamento = La Libertad, periodo 2015-2024, semanas validas), antes de las acciones repetidas (`count`, `groupBy`, `distinct`, `dropDuplicates`).

**Por que funciona:** sin cache, cada accion posterior recalcula el plan logico completo desde la lectura del CSV (hay 4 acciones encadenadas sobre el mismo filtrado). Con `.cache()` el resultado filtrado se materializa en memoria tras la primera accion y las siguientes lo reutilizan en vez de reprocesar el CSV y los filtros.


### Tabla de repeticiones (3 corridas por modo, dataset real)

| Repeticion | Sin cache (s) | Con cache (s) |
|---|---|---|
| 1 | 1.99 | 1.69 |
| 2 | 1.75 | 1.81 |
| 3 | 1.78 | 1.57 |
| **Promedio** | **1.84** | **1.69** |

Mejora promedio: **8.2%** menos tiempo con cache.

Planes de ejecucion capturados: `docs/evidencias/plan_sin_cache.txt` y `docs/evidencias/plan_con_cache.txt` (buscar `InMemoryTableScan` en el segundo, ausente en el primero).


## 3.7 Prueba de escalabilidad

Conjunto real: `IRA_La_Libertad_2015_2024.csv` (38086 filas). Replicas x10 y x100 generadas solo para medir el pipeline (no se usan en el analisis exploratorio, el entrenamiento ni conclusiones de negocio).


### Tabla de tiempos y consumo de recursos

| Factor | Filas | Tiempo (s) | CPU prom (%) | CPU max (%) | Mem prom (MB) | Mem pico (MB) |
|---|---|---|---|---|---|---|
| x1 | 38086 | 1.62 | 2.6 | 5.8 | 606.2 | 731.9 |
| x10 | 380860 | 3.7 | 2.1 | 3.9 | 711.9 | 803.5 |
| x100 | 3808600 | 17.57 | 1.6 | 4.0 | 1006.7 | 1038.5 |

### Analisis

- x10: volumen x10, tiempo x2.28 (sublineal/estable).
- x100: volumen x100, tiempo x10.85 (sublineal/estable).

_(Completar manualmente tras ver los numeros: indicar si el crecimiento del tiempo fue proporcional al volumen o se degrado antes, y en que factor una sola maquina con estos recursos dejaria de alcanzar, segun la tendencia de memoria pico observada.)_
