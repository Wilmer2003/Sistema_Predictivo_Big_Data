# Datos

Esta carpeta **no se versiona**: Git ignora todo su contenido, salvo este archivo y los `.gitkeep` que conservan la estructura.

| Carpeta local | Capa | Prefijo en S3 |
|---|---|---|
| `raw/` | Bronze | `bronze/` |
| `processed/` | Silver | `silver/` |
| `curated/` | Gold | `gold/` |

Cada capa tiene una subcarpeta por fuente (`ira/`, `malaria/`, `senamhi/`; en Gold, `epidemiologia_clima/`).

Los archivos se comparten por el Drive del grupo y, más adelante, por el bucket de S3. Para trabajar en local, coloca cada archivo en la carpeta de su capa y fuente.
