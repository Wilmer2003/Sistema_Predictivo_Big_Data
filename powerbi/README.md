# Conexión y Visualización en Power BI Desktop

Este directorio contiene las especificaciones y guía de conexión para los tableros de control de **Power BI Desktop** del Sistema Predictivo Big Data (La Libertad) desplegado en **Google Cloud (Cloud SQL)** o en entorno Local.

---

## 1. Opciones de Conexión

### Opción A: Conexión a Cloud SQL para PostgreSQL en Google Cloud (Producción)
Tal como se describe en el diagrama de arquitectura, Power BI se conecta a la instancia gestionada de **Cloud SQL para PostgreSQL** a través del puerto 5432 (redes autorizadas):

1. **Requisito previo en Google Cloud Console**:
   * Ir a la instancia de **Cloud SQL** -> **Conexiones** -> **Seguridad / Redes autorizadas**.
   * Agregar la dirección IP pública de la laptop del equipo para permitir tráfico entrante por el puerto `5432`.
2. **Abrir Power BI Desktop**:
   * Seleccionar **Obtener datos** -> **Base de datos PostgreSQL**.
   * **Servidor:** `<IP_PUBLICA_CLOUD_SQL>:5432` (o `localhost:5432` en local).
   * **Base de datos:** `epidemiologia_db`.
   * **Modo de conectividad:** `DirectQuery` (para ver los datos y estado del pipeline en vivo) o `Importar` (con actualización programada).
   * **Credenciales:** Usuario `postgres` y contraseña configurada en `.env`.
3. **Seleccionar las tablas del modelo**:
   * `dim_ubigeo`: Dimensión geográfica de distritos de La Libertad y presencia de estaciones SENAMHI.
   * `dim_tiempo`: Dimensión temporal estructurada por año, semana epidemiológica, trimestre y mes.
   * `fact_vigilancia_clima`: Tabla de hechos consolidada con casos de IRA, defunciones, Malaria y mediciones SENAMHI.
   * `predicciones_vigilancia`: Predicciones de Machine Learning frente a valores reales observados y error residual.
   * `lotes`: Registro de auditoría del pipeline (archivo, hash SHA-256, fecha de carga y estado actual del lote).

### Opción B: Conexión Directa a Archivos Parquet (Modo Local sin Servidor)
1. Abrir **Power BI Desktop**.
2. Seleccionar **Obtener datos** -> **Parquet**.
3. Navegar a las carpetas curadas en `data/curated/epidemiologia_clima/`:
   * `dim_ubigeo/`
   * `dim_tiempo/`
   * `fact_vigilancia/`
   * `features_ml/`

---

## 2. Modelo Relacional en Power BI (Esquema en Estrella)

Relacionar en la vista de modelo de Power BI:
* `dim_ubigeo[ubigeo]` (1) $\longrightarrow$ $(\infty)$ `fact_vigilancia_clima[ubigeo]`
* `dim_tiempo[tiempo_id]` (1) $\longrightarrow$ $(\infty)$ `fact_vigilancia_clima[tiempo_id]`
* `dim_ubigeo[ubigeo]` (1) $\longrightarrow$ $(\infty)$ `predicciones_vigilancia[ubigeo]`
* `dim_tiempo[tiempo_id]` (1) $\longrightarrow$ $(\infty)$ `predicciones_vigilancia[tiempo_id]`

---

## 3. Páginas del Dashboard de Power BI

De acuerdo con el diagrama de arquitectura del proyecto:

1. **Página 1: Vigilancia Epidemiológica Departamental**:
   * Mapa coroplético por distrito de La Libertad (filtrable por año y semana epidemiológica).
   * Curvas epidemiológicas semanales de IRA y Malaria.
   * Correlación climática: Gráficos multieje comparando Temperatura media / Precipitación rezagada vs Casos.
2. **Página 2: Estado del Pipeline (Auditoría de Ingesta y Lotes)**:
   * Tarjetas KPI con total de lotes procesados y última fecha de carga.
   * Tabla de auditoría (`lotes`): Nombre de archivo, hash SHA-256, fuente, estado (`REGISTERED`, `PROCESSED_SILVER`, `PROCESSED_GOLD`) y fecha de actualización.
3. **Página 3: Monitoreo de Modelos Predictivos y Alertas Tempranas**:
   * Comparativa de casos reales vs predicción del modelo de Spark MLlib.
   * Semáforo de alerta temprana distrital ante anomalías predictivas.
