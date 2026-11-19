# Contrato de la Capa Silver

Este documento describe el "contrato" de salida de la capa Silver para los tres
datasets del proyecto (IRA, Malaria, SENAMHI): qué columnas produce cada pipeline
de limpieza, con qué tipo de dato, y qué reglas de negocio y de protección de
privacidad ya se aplicaron antes de llegar ahí. También sirve de guía rápida de
cómo están organizados los módulos `src/ingestion/` y `src/preprocessing/`.

> **Nota:** este contrato describe el comportamiento **correcto/esperado** de cada
> pipeline. En particular, el contrato de Malaria asume que el PR de la rama
> `test/silver-limpieza` (restauración de `edad_rango`/`localidad_hash`) ya fue
> fusionado a `main`. Si se consulta este documento antes de esa fusión, el código
> de `main` puede no coincidir todavía con lo descrito aquí para Malaria.

## 1. Flujo general (Bronze → Silver)

```
data/raw/<fuente>/fecha_ingesta=YYYY-MM-DD/*.csv   (Bronze)
              │
              ▼  transformar_datos_<fuente>()
data/processed/<fuente>/parquet/  (particionado por año)   (Silver)
```

1. **Ingesta** (`src/ingestion/raw_ingestor.py`): copia el archivo crudo a Bronze,
   calcula su hash SHA-256 y lo registra en el control de lotes
   (`src/ingestion/batch_manager.py`). Si el archivo pesa 0 bytes o ya fue
   ingerido antes (mismo hash), no se ingiere.
2. **Limpieza** (`src/preprocessing/limpiar_<fuente>.py`): lee todos los CSV
   pendientes de Bronze, aplica `transformar_datos_<fuente>()` y escribe el
   resultado en Parquet particionado en Silver.
3. **Control de lotes**: si el paso 2 falla, el lote se marca `FAILED` (no avanza
   a Silver). Si tiene éxito, se marca `PROCESSED_SILVER`.

### Estados posibles de un lote (`BatchMetadata.status`)

| Estado | Significado |
|---|---|
| `REGISTERED` | Ingerido a Bronze, pendiente de procesar a Silver |
| `PROCESSED_SILVER` | Ya se limpió y escribió en Silver |
| `PROCESSED_GOLD` | Ya se usó para construir la capa Gold/analítica |
| `FAILED` | Falló su procesamiento a Silver; no avanza. Se puede reintentar con un archivo corregido (hash distinto) |

## 2. IRA — `limpiar_ira.py`

**Reglas de negocio aplicadas:**
- Filtro geográfico: solo departamento La Libertad (código `13`).
- Periodo: años 2015–2024; el año 2024 solo hasta la semana epidemiológica 26.
- Semana epidemiológica válida: 1 a 53.
- Deduplicación por `Codigo_Ubigeo + Ano + Semana_Epidemiologica`.
- Dataset agregado por distrito/semana (no es registro por persona).

**Protección de privacidad:** bandeo de celda pequeña sobre las columnas de
conteo (umbral `< 5`), porque incluye casos en menores de edad y es un riesgo
conocido de reidentificación cuando el conteo es muy bajo en un distrito pequeño.

**Columnas de salida (Silver):**

| Columna | Tipo | Notas |
|---|---|---|
| `Departamento`, `Provincia`, `Distrito` | string | |
| `Ano` | int | 2015–2024 |
| `Semana_Epidemiologica` | int | 1–53 |
| `Codigo_Departamento`, `Codigo_Ubigeo` | string | Ubigeo normalizado |
| `IRA_menores_2_meses`, `IRA_2_a_11_meses`, `IRA_1_a_4_anos` | int | Conteo exacto (uso interno/modelado) |
| `IRA_menores_2_meses_rango`, `IRA_2_a_11_meses_rango`, `IRA_1_a_4_anos_rango` | string | Banda: `0`, `1-4`, `5-9`, `10-19`, `20+` (uso en reportes/salidas públicas) |
| `Neumonia_menores_2_meses`, `Neumonia_2_a_11_meses`, `Neumonia_1_a_4_anos` | int | Conteo exacto |
| `Neumonia_menores_2_meses_rango`, `Neumonia_2_a_11_meses_rango`, `Neumonia_1_a_4_anos_rango` | string | Banda (igual que arriba) |
| `Defunciones` | int | Conteo exacto |
| `Defunciones_rango` | string | Banda |

## 3. Malaria — `limpiar_malaria.py`

**Reglas de negocio aplicadas:**
- Filtro geográfico: solo departamento La Libertad (código `13`).
- Periodo: años 2015–2024, semana 1–53.
- Solo diagnósticos oficiales de malaria: `B50` (P. falciparum) y `B51` (P. vivax).
- Deduplicación por `ubigeo + ano + semana + localidad + diagnostic + sexo + edad`.
- Dataset a nivel de **caso individual** (no agregado).

**Protección de privacidad:** `edad` y `localidad` son cuasi-identificadores a
nivel de caso individual. Se generaliza la edad a rango y se seudonimiza la
localidad con hash, para reducir el riesgo de reidentificación en localidades
con pocos casos. Ver punto 5.1 del informe para el detalle y el riesgo residual.

**Columnas de salida (Silver):**

| Columna | Tipo | Notas |
|---|---|---|
| `departamento`, `provincia`, `distrito` | string | |
| `localidad_hash` | string | Hash SHA-256 (12 caracteres) — **no** el nombre real de la localidad |
| `enfermedad` | string | |
| `ano` | int | 2015–2024 |
| `semana` | int | 1–53 |
| `diagnostic` | string | `B50` o `B51` |
| `diresa`, `ubigeo`, `localcod` | string | Ubigeo (6 díg.) y localcod (10 díg.) normalizados |
| `edad_rango` | string | `0-4`, `5-14`, `15-44`, `45-64`, `65+` — **no** la edad exacta |
| `tipo_edad`, `sexo` | string | |
| `Malaria_total`, `B50`, `B51` | int | Conteos |

## 4. SENAMHI — `limpiar_senamhi.py`

**Reglas de negocio aplicadas:**
- Filtro geográfico: solo departamento La Libertad.
- Periodo: años 2015–2024.
- Consistencia física: `Precipitacion_acumulada >= 0`; `Temperatura_minima <= Temperatura_maxima`.
- Deduplicación por `Estacion + Fecha`.

**Protección de privacidad:** no aplica — el dataset contiene únicamente
variables meteorológicas por estación, sin ningún dato de persona.

**Columnas de salida (Silver):**

| Columna | Tipo | Notas |
|---|---|---|
| `Departamento`, `Estacion` | string | |
| `Ano`, `Mes`, `Dia` | int | |
| `Fecha` | date | Construida a partir de Año/Mes/Día |
| `Semana_Epidemiologica` | int | Calculada a partir de `Fecha` |
| `Precipitacion_acumulada`, `Temperatura_maxima`, `Temperatura_minima`, `tc` | double | |

## 5. Dónde están las pruebas

- `tests/test_ingestion.py`: control de lotes (nuevo, duplicado, archivo vacío, columnas distintas).
- `tests/test_ira.py`, `tests/test_malaria.py`, `tests/test_senamhi.py`: filtros, protección de privacidad e idempotencia de cada pipeline.
- `tests/test_silver_errores.py`: manejo de errores (un lote con error se marca `FAILED` y no avanza).
