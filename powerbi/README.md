# Conexión y Visualización en Power BI Desktop

Este directorio contiene las especificaciones y guía de conexión para los tableros de control de **Power BI Desktop** del Sistema Predictivo Big Data (La Libertad).

---

## 1. Opciones de Conexión

### Opción A: Conexión Directa a PostgreSQL (Recomendada para Producción)
1. Abrir **Power BI Desktop**.
2. Seleccionar **Obtener datos** -> **Base de datos PostgreSQL**.
3. Ingresar los parámetros configurados en tu archivo `.env`:
   * **Servidor:** `localhost:5432` (o endpoint RDS en AWS).
   * **Base de datos:** `epidemiologia_db`.
   * **Modo de conectividad:** `Importar` (o `DirectQuery`).
4. Seleccionar las tablas del esquema en estrella:
   * `dim_ubigeo`: Tabla dimensional con nombres de distritos, provincias y presencia de estaciones.
   * `dim_tiempo`: Dimensión temporal estructurada por año, semana epidemiológica, trimestre y mes.
   * `fact_vigilancia_clima`: Tabla de hechos consolidada con casos de IRA, Malaria y mediciones SENAMHI.
   * `predicciones_vigilancia`: Predicciones de Machine Learning frente a valores reales y error residual.

### Opción B: Conexión Directa a Archivos Parquet (Modo Local sin Servidor de Base de Datos)
Si aún no tienes PostgreSQL instalado o prefieres conectar directamente a los archivos generados:
1. Abrir **Power BI Desktop**.
2. Seleccionar **Obtener datos** -> **Parquet**.
3. Navegar a la carpeta Gold:
   * `data/curated/epidemiologia_clima/dim_ubigeo/`
   * `data/curated/epidemiologia_clima/dim_tiempo/`
   * `data/curated/epidemiologia_clima/fact_vigilancia/`
   * `data/curated/epidemiologia_clima/features_ml/`

---

## 2. Modelo Relacional en Power BI (Esquema en Estrella)

Relacionar en la vista de modelo de Power BI:
* `dim_ubigeo[ubigeo]` (1) $\longrightarrow$ $(\infty)$ `fact_vigilancia_clima[ubigeo]`
* `dim_tiempo[tiempo_id]` (1) $\longrightarrow$ $(\infty)$ `fact_vigilancia_clima[tiempo_id]`
* `dim_ubigeo[ubigeo]` (1) $\longrightarrow$ $(\infty)$ `predicciones_vigilancia[ubigeo]`
* `dim_tiempo[tiempo_id]` (1) $\longrightarrow$ $(\infty)$ `predicciones_vigilancia[tiempo_id]`

---

## 3. Páginas del Dashboard Sugeridas

1. **Vigilancia Epidemiológica Departamental**:
   * Mapa coroplético por distrito de La Libertad (filtrable por semana y año).
   * Curvas epidemiológicas semanales de IRA y Malaria.
2. **Impacto Climático y Correlación**:
   * Gráficos de dispersión y líneas multieje comparando Temperatura / Precipitación rezagada vs Casos.
3. **Monitoreo de Modelos Predictivos y Alertas Tempranas**:
   * Comparativa de casos reales vs predicción del modelo de MLllib.
   * Semáforo de alerta temprana por distrito (semanas con incremento anómalo predicho).
