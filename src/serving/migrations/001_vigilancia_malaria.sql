-- Aplicar con psql --single-transaction -v ON_ERROR_STOP=1.
CREATE TABLE IF NOT EXISTS public.malaria_publicaciones (
    run_id varchar(100) PRIMARY KEY CHECK (run_id ~ '^[A-Za-z0-9_-]+$'),
    sha256_bronze text NOT NULL CHECK (sha256_bronze ~ '^[a-f0-9]{64}$'),
    uri_gold text NOT NULL,
    total_registros bigint NOT NULL CHECK (total_registros > 0),
    publicado_en timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS public.dim_ubigeo_malaria (
    run_id varchar(100) REFERENCES public.malaria_publicaciones,
    ubigeo varchar(6) CHECK (ubigeo ~ '^13[0-9]{4}$'),
    departamento text,
    provincia text,
    distrito text,
    PRIMARY KEY (run_id, ubigeo)
);

CREATE TABLE IF NOT EXISTS public.dim_tiempo_malaria (
    run_id varchar(100) REFERENCES public.malaria_publicaciones,
    tiempo_id integer,
    ano integer NOT NULL CHECK (ano BETWEEN 2015 AND 2024),
    semana_epidemiologica integer NOT NULL CHECK (semana_epidemiologica BETWEEN 1 AND 53),
    CHECK (tiempo_id = ano * 100 + semana_epidemiologica),
    PRIMARY KEY (run_id, tiempo_id)
);

CREATE TABLE IF NOT EXISTS public.fact_vigilancia_malaria (
    run_id varchar(100) NOT NULL,
    ubigeo varchar(6) NOT NULL,
    tiempo_id integer NOT NULL,
    especie_codigo varchar(3) CHECK (especie_codigo IN ('B50', 'B51')),
    registros_fuente bigint NOT NULL CHECK (registros_fuente > 0),
    PRIMARY KEY (run_id, ubigeo, tiempo_id, especie_codigo),
    FOREIGN KEY (run_id, ubigeo) REFERENCES public.dim_ubigeo_malaria,
    FOREIGN KEY (run_id, tiempo_id) REFERENCES public.dim_tiempo_malaria
);

CREATE TABLE IF NOT EXISTS public.malaria_publicacion_activa (
    unica boolean PRIMARY KEY CHECK (unica),
    run_id varchar(100) REFERENCES public.malaria_publicaciones
);

INSERT INTO public.malaria_publicacion_activa (unica)
VALUES (true) ON CONFLICT (unica) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.malaria_cambios_publicacion (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    anterior varchar(100) REFERENCES public.malaria_publicaciones,
    siguiente varchar(100) REFERENCES public.malaria_publicaciones,
    operacion text NOT NULL CHECK (operacion IN ('publicar', 'rollback')),
    actor text NOT NULL DEFAULT current_user,
    registrado_en timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE VIEW public.vw_vigilancia_malaria AS
SELECT f.run_id, f.ubigeo, f.tiempo_id, t.ano, t.semana_epidemiologica,
       g.departamento, g.provincia, g.distrito, f.especie_codigo, f.registros_fuente
FROM public.malaria_publicacion_activa a
JOIN public.fact_vigilancia_malaria f ON f.run_id = a.run_id
JOIN public.dim_ubigeo_malaria g ON (g.run_id, g.ubigeo) = (f.run_id, f.ubigeo)
JOIN public.dim_tiempo_malaria t ON (t.run_id, t.tiempo_id) = (f.run_id, f.tiempo_id);

CREATE OR REPLACE FUNCTION public.restaurar_publicacion_malaria(
    esperada text, destino text
) RETURNS void LANGUAGE plpgsql SECURITY INVOKER AS $$
DECLARE
    vigente text;
BEGIN
    SELECT run_id INTO STRICT vigente
    FROM public.malaria_publicacion_activa WHERE unica FOR UPDATE;
    IF vigente IS DISTINCT FROM esperada THEN
        RAISE EXCEPTION 'La publicación activa cambió';
    END IF;
    IF destino IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM public.malaria_publicaciones WHERE run_id = destino
    ) THEN
        RAISE EXCEPTION 'La versión de destino no existe';
    END IF;
    IF vigente IS DISTINCT FROM destino THEN
        UPDATE public.malaria_publicacion_activa SET run_id = destino WHERE unica;
        INSERT INTO public.malaria_cambios_publicacion (anterior, siguiente, operacion)
        VALUES (vigente, destino, 'rollback');
    END IF;
END;
$$;
