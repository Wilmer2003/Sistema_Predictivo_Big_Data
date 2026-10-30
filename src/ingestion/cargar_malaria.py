import subprocess

# ============================================================
# 1. RUTAS
# ============================================================
# Zona Bronze (raw): archivo tal cual llega de la fuente, sin
# ninguna transformación. Se renombra a la convención del
# proyecto (dataset_<Fuente>.csv) para que
# src/preprocessing/limpiar_malaria.py lo encuentre.

ruta_local = (
    "/home/scaipotru/Documentos/Sistema_Predictivo_Big_Data/"
    "data/raw/malaria/malaria_dataset.csv"
)

ruta_hdfs_dir = "/datalake/raw/malaria"

ruta_hdfs_archivo = f"{ruta_hdfs_dir}/dataset_Malaria.csv"

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

print("\nIngesta de Malaria finalizada correctamente.")
