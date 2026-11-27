-- ==============================================================================
-- SISTEMA PREDICTIVO BIG DATA - ESQUEMA RELACIONAL (AMAZON RDS POSTGRESQL / LOCAL)
-- Modelo dimensional optimizado para BI (Power BI), predicciones y control de lotes
-- ==============================================================================

-- 0. Tabla de Control de Lotes (Auditoría e Idempotencia en Amazon RDS PostgreSQL)
CREATE TABLE IF NOT EXISTS public.lotes (
    batch_id VARCHAR(100) PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) NOT NULL UNIQUE,
    file_size_bytes BIGINT NOT NULL,
    load_date DATE NOT NULL,
    ingest_timestamp TIMESTAMP NOT NULL,
    destination_path TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lotes_file_hash ON public.lotes (file_hash);
CREATE INDEX IF NOT EXISTS idx_lotes_source_status ON public.lotes (source, status);

-- 1. Dimensión Geográfica (Ubigeo)
CREATE TABLE IF NOT EXISTS dim_ubigeo (
    ubigeo VARCHAR(6) PRIMARY KEY,
    departamento VARCHAR(50) NOT NULL,
    provincia VARCHAR(50) NOT NULL,
    distrito VARCHAR(50) NOT NULL,
    tiene_estacion_senamhi BOOLEAN DEFAULT FALSE
);

-- 2. Dimensión Temporal (Semana Epidemiológica)
CREATE TABLE IF NOT EXISTS dim_tiempo (
    tiempo_id INT PRIMARY KEY,               -- Formato YYYYWW (ej: 202401)
    ano INT NOT NULL,
    semana_epidemiologica INT NOT NULL,
    trimestre INT NOT NULL,
    mes_aproximado INT NOT NULL
);

-- 3. Tabla de Hechos (Vigilancia Epidemiológica y Clima)
CREATE TABLE IF NOT EXISTS fact_vigilancia_clima (
    id SERIAL PRIMARY KEY,
    ubigeo VARCHAR(6) NOT NULL REFERENCES dim_ubigeo(ubigeo),
    tiempo_id INT NOT NULL REFERENCES dim_tiempo(tiempo_id),
    ano INT NOT NULL,
    semana_epidemiologica INT NOT NULL,
    
    -- Métricas Epidemiológicas IRA
    ira_total_menores_5 INT DEFAULT 0,
    neumonia_total_menores_5 INT DEFAULT 0,
    defunciones INT DEFAULT 0,
    ira_menores_2m INT DEFAULT 0,
    ira_2_a_11m INT DEFAULT 0,
    ira_1_a_4a INT DEFAULT 0,
    neumonia_menores_2m INT DEFAULT 0,
    neumonia_2_a_11m INT DEFAULT 0,
    neumonia_1_a_4a INT DEFAULT 0,

    -- Métricas Epidemiológicas Malaria
    malaria_casos_total INT DEFAULT 0,
    malaria_b50 INT DEFAULT 0,
    malaria_b51 INT DEFAULT 0,

    -- Métricas Meteorológicas SENAMHI
    temp_media_promedio NUMERIC(5, 2),
    temp_max_promedio NUMERIC(5, 2),
    temp_min_promedio NUMERIC(5, 2),
    precipitacion_total NUMERIC(7, 2),
    dias_con_lluvia INT DEFAULT 0,

    CONSTRAINT uq_fact_ubigeo_tiempo UNIQUE (ubigeo, tiempo_id)
);

-- 4. Tabla de Predicciones de Machine Learning
CREATE TABLE IF NOT EXISTS predicciones_vigilancia (
    id SERIAL PRIMARY KEY,
    ubigeo VARCHAR(6) NOT NULL REFERENCES dim_ubigeo(ubigeo),
    tiempo_id INT NOT NULL REFERENCES dim_tiempo(tiempo_id),
    ano INT NOT NULL,
    semana_epidemiologica INT NOT NULL,
    casos_reales NUMERIC(10, 2),
    prediccion_casos NUMERIC(10, 2) NOT NULL,
    error_residual NUMERIC(10, 2),
    fecha_generacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_pred_ubigeo_tiempo UNIQUE (ubigeo, tiempo_id)
);

-- ==============================================================================
-- ÍNDICES DE RENDIMIENTO PARA CONSULTAS ANALÍTICAS Y POWER BI
-- ==============================================================================
CREATE INDEX IF NOT EXISTS idx_fact_ubigeo ON fact_vigilancia_clima (ubigeo);
CREATE INDEX IF NOT EXISTS idx_fact_tiempo ON fact_vigilancia_clima (tiempo_id);
CREATE INDEX IF NOT EXISTS idx_fact_ano_semana ON fact_vigilancia_clima (ano, semana_epidemiologica);
CREATE INDEX IF NOT EXISTS idx_pred_ubigeo ON predicciones_vigilancia (ubigeo);
CREATE INDEX IF NOT EXISTS idx_pred_tiempo ON predicciones_vigilancia (tiempo_id);

-- 5. Métricas de evaluación por ejecución de entrenamiento.
CREATE TABLE IF NOT EXISTS public.metricas_evaluacion (
    run_id VARCHAR(100) PRIMARY KEY,
    timestamp_evaluacion TIMESTAMPTZ NOT NULL,
    modelo TEXT NOT NULL,
    target TEXT NOT NULL,
    train_max_year INTEGER NOT NULL,
    test_min_year INTEGER NOT NULL,
    rmse DOUBLE PRECISION NOT NULL,
    mae DOUBLE PRECISION NOT NULL,
    r2 DOUBLE PRECISION NOT NULL,
    baseline_rmse DOUBLE PRECISION NOT NULL,
    baseline_mae DOUBLE PRECISION NOT NULL,
    baseline_r2 DOUBLE PRECISION NOT NULL,
    reduccion_rmse_porcentaje DOUBLE PRECISION NOT NULL,
    reduccion_mae_porcentaje DOUBLE PRECISION NOT NULL,
    supera_al_baseline BOOLEAN NOT NULL,
    artifact_uri TEXT NOT NULL,
    reporte JSONB NOT NULL,
    registrado_en TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (train_max_year < test_min_year)
);
