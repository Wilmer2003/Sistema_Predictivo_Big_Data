# Conectar la vigilancia de malaria

El PBIX existente se conserva como referencia. La migración agrega una
consulta de importación y medidas con nombres propios. Conserva las consultas
`fact_vigilancia_clima`, `dim_ubigeo`, `dim_tiempo`,
`predicciones_vigilancia`, `Control_Lotes_Local` y `metricas_evaluacion`
con sus pasos, relaciones y medidas actuales.

1. En Power Query crea la consulta en blanco `Vigilancia_Malaria`. Si esa consulta ya fue creada en una prueba anterior, sustituye **solo** su código del Editor avanzado por este bloque.
2. Usa los parámetros de conexión existentes y un rol de lectura con permiso sobre `public.vw_vigilancia_malaria`.
3. Mantén la tabla nueva sin relaciones con las dimensiones anteriores. Su cobertura de distrito-semana procede de malaria.

```powerquery
let
    Origen = PostgreSQL.Database(PostgresServidor, PostgresBaseDatos),
    Vigilancia = Origen{[Schema="public", Item="vw_vigilancia_malaria"]}[Data],
    Tipos = Table.TransformColumnTypes(Vigilancia, {
        {"run_id", type text}, {"ubigeo", type text}, {"tiempo_id", Int64.Type},
        {"ano", Int64.Type}, {"semana_epidemiologica", Int64.Type},
        {"departamento", type text}, {"provincia", type text}, {"distrito", type text},
        {"especie_codigo", type text}, {"registros_fuente", Int64.Type}
    })
in
    Tipos
```

```dax
Registros fuente de malaria =
SUM ( 'Vigilancia_Malaria'[registros_fuente] )

Registros fuente vivax =
CALCULATE ( [Registros fuente de malaria], KEEPFILTERS ( 'Vigilancia_Malaria'[especie_codigo] = "B51" ) )

Registros fuente falciparum =
CALCULATE ( [Registros fuente de malaria], KEEPFILTERS ( 'Vigilancia_Malaria'[especie_codigo] = "B50" ) )
```

Crea primero una página de validación con los campos de segmentación
`ano`, `semana_epidemiologica`, `provincia`, `distrito` y
`especie_codigo` de `Vigilancia_Malaria`. Comprueba 490 registros
fuente en la primera revisión histórica, 477 vivax y 13 falciparum.
El informe legado puede seguir mostrando 470 según su cobertura original.

Después de validar, en la página existente sustituye **solo el campo de
valor** de la tarjeta de malaria por `[Registros fuente de malaria]` y
cambia su título a **Registros fuente de malaria** si la tarjeta se usa
sin segmentadores históricos. Para conservar los segmentadores
históricos en esa tarjeta, usa esta medida adicional. Cuando no hay
selección en una dimensión, mantiene toda la cobertura de malaria.

```dax
Registros fuente de malaria (filtros del tablero) =
VAR FiltraGeo = ISCROSSFILTERED ( 'dim_ubigeo' )
VAR FiltraTiempo = ISCROSSFILTERED ( 'dim_tiempo' )
VAR Geo = VALUES ( 'dim_ubigeo'[ubigeo] )
VAR Tiempo = VALUES ( 'dim_tiempo'[tiempo_id] )
RETURN
    SWITCH (
        TRUE (),
        FiltraGeo && FiltraTiempo,
            CALCULATE ( [Registros fuente de malaria],
                KEEPFILTERS ( TREATAS ( Geo, 'Vigilancia_Malaria'[ubigeo] ) ),
                KEEPFILTERS ( TREATAS ( Tiempo, 'Vigilancia_Malaria'[tiempo_id] ) ) ),
        FiltraGeo,
            CALCULATE ( [Registros fuente de malaria],
                KEEPFILTERS ( TREATAS ( Geo, 'Vigilancia_Malaria'[ubigeo] ) ) ),
        FiltraTiempo,
            CALCULATE ( [Registros fuente de malaria],
                KEEPFILTERS ( TREATAS ( Tiempo, 'Vigilancia_Malaria'[tiempo_id] ) ) ),
        [Registros fuente de malaria]
    )
```

Los segmentos históricos siguen limitados a los valores de su dimensión.
Para navegar claves ausentes de esas dimensiones, usar la página de
malaria con segmentadores propios. Revisar la actualización y la
interacción de los visuales de IRA, predicciones y auditoría antes de guardar
una copia nueva del informe. Para revertir la tarjeta, vuelve a seleccionar
su medida histórica y título original. La revisión conserva la unidad
`registros_fuente` hasta que el proveedor acredite la semántica de cada fila.
