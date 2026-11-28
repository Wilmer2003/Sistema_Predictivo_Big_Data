# Contrato Silver de malaria `detalle_v1`

El grano es una **ocurrencia del extracto Bronze**. `registros_fuente` vale
uno por ocurrencia y su suma mide filas del extracto. La unidad epidemiológica
queda pendiente de acreditación por fuente y versión.

## Entrada

- CSV UTF-8, separado por comas y con encabezado. Campos requeridos
  `ubigeo`, `ano`, `semana` y `enfermedad`.
- Campos opcionales `departamento`, `provincia`, `distrito`, `localidad`,
  `diagnostic`, `diresa`, `localcod`, `edad`, `tipo_edad`, `sexo` y
  `edad_anios`. Este último se conserva como texto de origen.
- Se evalúan La Libertad, los años 2015–2024 y semanas 1–53. Un código mal
  formado, una contradicción entre departamento y ubigeo o una semana
  inválida en el ámbito evaluado rechazan el lote. Los campos opcionales
  pueden faltar. Si se informa edad, debe convertirse a entero no negativo.
- La especie se obtiene de `enfermedad` tras recortar espacios externos y
  convertir a mayúsculas. Se aceptan `MALARIA POR P. VIVAX` y
  `MALARIA P. VIVAX` como `B51`, y `MALARIA P. FALCIPARUM` y
  `MALARIA POR P. FALCIPARUM` como `B50`. Un `diagnostic` informado
  debe concordar. El rechazo registra el motivo y el número de filas,
  sin imprimir los valores demográficos desde la validación de Silver.
- Un encabezado con `Malaria_total`, `B50` o `B51` rechaza todo el
  archivo por formato agregado sin acreditar. Los encabezados incompatibles
  también rechazan el lote.

## Salida y ejecución

Se publica un solo extracto completo por ejecución en
`silver/malaria/parquet_detalle_v1` o su equivalente local. Si Bronze
contiene varios CSV, seleccionar uno con
`python -m src.preprocessing.limpiar_malaria --archivo <ruta-bronze>`.
La sustitución del contenido evita acumular reintentos o arrastrar
particiones de años ausentes. La idempotencia por hash en Bronze sigue
gestionada por el registro de lotes.

Cada fila incluye `especie_codigo`, `formato_origen`, `archivo_origen`
y `registros_fuente`. La URI de Bronze permite cruzar con el destino del
lote y su SHA-256 en el registro de ingesta, cuando el archivo está registrado.
`ocurrencias_clave_coincidente` indica cuántas filas comparten ubigeo, año,
semana, localidad, sexo y edad dentro del extracto. Se conservan todas,
incluidas coincidencias exactas. El indicador no acredita identidad.

## Transición a Gold

Para una publicación operativa usar el driver independiente
`src.orchestration.run_malaria`. Escribe archivos Silver y Gold por revisión
y deja un manifiesto para Serving. Este camino conserva los estados
históricos de lotes y usa auditoría propia. La orden Silver aislada
`src.preprocessing.limpiar_malaria` permite publicar un extracto de trabajo
en `parquet_detalle_v1`; para esa orden la escritura y el marcado del
lote son operaciones separadas.

El pipeline general ejecuta IRA, SENAMHI y el modelo existente. Gold IRA
puede conservar las medidas históricas de compatibilidad desde una referencia
Gold congelada, diferente del destino del job. La vigilancia semanal de
malaria tiene cobertura propia y usa `registros_fuente`. Consulta el
[procedimiento de despliegue](arquitectura/despliegue_malaria_aws.md).
