# Cómo trabajamos en el repositorio

## Ramas
- `main` siempre debe estar estable. No se hace commit directo en `main`.
- Una rama por tarea, creada desde `main` actualizado, con este formato:
  - `feat/<area>-<descripcion>` para funcionalidad nueva (`feat/silver-clean-ira`)
  - `fix/<descripcion>` para correcciones
  - `infra/<descripcion>` para AWS y despliegue
  - `docs/<descripcion>` para documentación
  - `chore/<descripcion>` para orden y mantenimiento
- Las ramas viven pocos días. Se borran al hacer merge.

## Commits
Formato `tipo(area): mensaje corto`, en español:
`feat(silver): limpieza de IRA como función`, `fix(config): ruta de S3`, `docs(readme): estado del proyecto`, `test(epi-week): fechas de borde`.

## Pull requests
1. Actualiza tu rama con `main` antes de abrir el PR.
2. Completa la plantilla del PR.
3. Corre `pytest -q` en local. El PR no se mezcla si CI falla.
4. Otro integrante lo revisa y aprueba. Se mezcla con *Squash and merge*.

## Reglas del código
- Sin rutas fijas: todas las rutas salen de `src/common/config.py` (`config.path(...)`).
- Funciones puras y pequeñas, con su prueba en `tests/`.
- Los datos de prueba son CSV pequeños en `tests/fixtures/`.

## Nunca en el repositorio
- Datos (`data/` está ignorado), archivos `.crc` ni `_SUCCESS` de Hadoop/Spark.
- Credenciales: `.env`, claves `.pem`, contraseñas. Usa `.env.example` como guía.
- Archivos `.pbix` de Power BI (pueden traer datos). Se versiona la plantilla `.pbit`.
