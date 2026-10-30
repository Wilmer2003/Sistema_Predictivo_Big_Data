import subprocess

# ============================================================
# 1. RUTAS
# ============================================================
# Zona Bronze (raw): archivo tal cual llega de la fuente, sin
# ninguna transformación. Se mantiene el mismo nombre que
# espera src/preprocessing/limpiar_senamhi.py.

ruta_local = (
    "/home/scaipotru/Documentos/Sistema_Predictivo_Big_Data/"
    "data/raw/senamhi/Clima_LaLibertad_2015_2024_Completo.csv"
)

ruta_hdfs_dir = "/datalake/raw/senamhi"

ruta_hdfs_archivo = f"{ruta_hdfs_dir}/dataset_SENAMHI.csv"

# ============================================================
# 2. CREAR DIRECTORIO EN HDFS
# ============================================================

print("\n========== CREANDO DIRECTORIO EN HDFS ==========")

subprocess.run(
    ["hdfs", "dfs", "-mkdir", "-p", ruta_hdfs_dir],
    check=True
)

# ============================================================
# 3. CARGAR ARCHIVO A LA ZONA RAW (BRONZE)
# ============================================================

print("\n========== CARGANDO ARCHIVO A HDFS ==========")

subprocess.run(
    ["hdfs", "dfs", "-put", "-f", ruta_local, ruta_hdfs_archivo],
    check=True
)

print("Archivo cargado en:", ruta_hdfs_archivo)

# ============================================================
# 4. VERIFICACIÓN
# ============================================================

print("\n========== VERIFICANDO CARGA ==========")

subprocess.run(
    ["hdfs", "dfs", "-ls", "-h", ruta_hdfs_dir],
    check=True
)

print("\nIngesta de SENAMHI finalizada correctamente.")
