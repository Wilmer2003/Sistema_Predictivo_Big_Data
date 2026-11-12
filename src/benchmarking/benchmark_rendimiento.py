"""
Benchmark para los puntos 3.6 (optimizacion del rendimiento) y
3.7 (prueba de escalabilidad) del informe, sobre los 3 datasets
reales del proyecto (IRA, Malaria, SENAMHI).

Se ejecuta en modo local con recursos limitados a proposito
(local[2], 1.5 GB de memoria de driver, UI de Spark apagada) para
no competir por CPU/RAM con otras aplicaciones (videollamada,
navegador) mientras corre.

Datasets reales usados (salidas ya filtradas a La Libertad):
  - IRA_La_Libertad_2015_2024.csv       (38 086 filas)
  - Malaria_La_Libertad_2015_2024.csv   (490 filas)
  - Clima_La_Libertad_2015_2024.csv     (36 530 filas, dataset SENAMHI)

Las replicas x10 y x100 se generan solo para medir el comportamiento
del pipeline: no se usan en el analisis exploratorio, en el
entrenamiento del modelo ni en conclusiones de negocio.

Salidas (docs/evidencias/), por dataset <N> en {IRA, MALARIA, SENAMHI}:
  - resultados_3_6_optimizacion_<N>.csv
  - resultados_3_7_escalabilidad_<N>.csv
  - plan_sin_cache_<N>.txt / plan_con_cache_<N>.txt   (planes de ejecucion)
  - log_ejecucion.txt                                 (log unico, con hora real)
  - resumen_3_6_3_7.md                                (tablas listas para pegar, las 3)
"""

import os
import shutil
import statistics
import threading
import time
from contextlib import redirect_stdout
from datetime import datetime

import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, regexp_replace, trim, upper

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.path.join(BASE_DIR, "docs", "evidencias")
WORK_DIR = os.path.join(OUT_DIR, "_tmp_benchmark")

REPETICIONES = 3
FACTORES_ESCALABILIDAD = [1, 10, 100]

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(WORK_DIR, exist_ok=True)

LOG_PATH = os.path.join(OUT_DIR, "log_ejecucion.txt")
_log_file = open(LOG_PATH, "a", encoding="utf-8")


def log(mensaje):
    linea = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {mensaje}"
    print(linea)
    _log_file.write(linea + "\n")
    _log_file.flush()


# ============================================================
# Transformaciones reales por dataset (fieles a src/preprocessing/*.py)
# ============================================================

def limpiar_ira(df, usar_cache):
    for columna in ["Departamento", "Provincia", "Distrito"]:
        if columna in df.columns:
            df = df.withColumn(columna, trim(col(columna)))

    if "Codigo_Ubigeo" in df.columns:
        df = (
            df
            .withColumn("Codigo_Ubigeo", regexp_replace(col("Codigo_Ubigeo").cast("string"), r"\.0$", ""))
            .withColumn("Codigo_Ubigeo", lpad(col("Codigo_Ubigeo"), 6, "0"))
        )

    df = df.withColumn("Ano", col("Ano").cast("integer"))
    df = df.withColumn("Semana_Epidemiologica", col("Semana_Epidemiologica").cast("integer"))
    df = df.dropna(how="all")
    df = df.filter(upper(trim(col("Departamento"))) == "LA LIBERTAD")
    df = df.filter((col("Ano") >= 2015) & (col("Ano") <= 2024))
    df = df.filter((col("Semana_Epidemiologica") >= 1) & (col("Semana_Epidemiologica") <= 53))

    if usar_cache:
        df = df.cache()
        df.count()

    df.groupBy("Ano").count().orderBy("Ano").collect()
    df.select("Departamento").distinct().collect()
    df = df.dropDuplicates()
    return df, df.count()


def limpiar_malaria(df, usar_cache):
    for columna in ["departamento", "provincia", "distrito", "localidad", "enfermedad", "diagnostic", "diresa", "tipo_edad", "sexo"]:
        if columna in df.columns:
            df = df.withColumn(columna, trim(col(columna)))

    if "ubigeo" in df.columns:
        df = (
            df
            .withColumn("ubigeo", regexp_replace(col("ubigeo").cast("string"), r"\.0$", ""))
            .withColumn("ubigeo", lpad(col("ubigeo"), 6, "0"))
        )
    if "localcod" in df.columns:
        df = (
            df
            .withColumn("localcod", regexp_replace(col("localcod").cast("string"), r"\.0$", ""))
            .withColumn("localcod", lpad(col("localcod"), 10, "0"))
        )

    for columna in ["ano", "semana", "edad", "Malaria_total", "B50", "B51"]:
        if columna in df.columns:
            df = df.withColumn(columna, col(columna).cast("integer"))

    df = df.dropna(how="all")
    df = df.filter(upper(trim(col("departamento"))) == "LA LIBERTAD")
    df = df.filter((col("ano") >= 2015) & (col("ano") <= 2024))
    df = df.filter((col("semana") >= 1) & (col("semana") <= 53))

    if usar_cache:
        df = df.cache()
        df.count()

    df.groupBy("ano").count().orderBy("ano").collect()
    df.select("provincia").distinct().collect()
    df = df.dropDuplicates()
    return df, df.count()


def limpiar_senamhi(df, usar_cache):
    df = df.withColumn("Departamento", trim(col("Departamento")))
    for columna in ["Ano", "Mes", "Dia"]:
        if columna in df.columns:
            df = df.withColumn(columna, col(columna).cast("integer"))
    for columna in ["Precipitacion_acumulada", "Temperatura_maxima", "Temperatura_minima", "tc"]:
        if columna in df.columns:
            df = df.withColumn(columna, col(columna).cast("double"))

    df = df.dropna(how="all")
    df = df.filter(upper(trim(col("Departamento"))) == "LA LIBERTAD")
    df = df.filter((col("Ano") >= 2015) & (col("Ano") <= 2024))

    if usar_cache:
        df = df.cache()
        df.count()

    registros_antes = df.count()
    df = df.filter(
        (col("Precipitacion_acumulada") >= 0)
        & (col("Temperatura_minima") <= col("Temperatura_maxima"))
    )
    log(f"  control de rango: {registros_antes} -> {df.count()}")

    df.groupBy("Ano").count().orderBy("Ano").collect()
    df.select("Estacion").distinct().collect()
    df = df.dropDuplicates(["Estacion", "Fecha"])
    return df, df.count()


DATASETS = [
    {
        "nombre": "IRA",
        "csv": os.path.join(BASE_DIR, "IRA_La_Libertad_2015_2024.csv"),
        "sep": ";",
        "limpiar": limpiar_ira,
    },
    {
        "nombre": "MALARIA",
        "csv": os.path.join(BASE_DIR, "Malaria_La_Libertad_2015_2024.csv"),
        "sep": ",",
        "limpiar": limpiar_malaria,
    },
    {
        "nombre": "SENAMHI",
        "csv": os.path.join(BASE_DIR, "Clima_La_Libertad_2015_2024.csv"),
        "sep": ",",
        "limpiar": limpiar_senamhi,
    },
]


class MonitorRecursos:
    """Muestrea CPU% y RSS (MB) del proceso python y de sus hijos
    (la JVM de Spark corre como proceso hijo en modo local)."""

    def __init__(self, intervalo=0.5):
        self.intervalo = intervalo
        self.muestras_cpu = []
        self.muestras_mem = []
        self._parar = threading.Event()
        self._hilo = None
        self._vistos = {}
        # con local[2] el proceso no puede pasar de ~200%; un margen generoso
        # descarta picos espurios de medicion sin ocultar un uso real alto
        self._cpu_max_valido = 300.0

    def _loop(self):
        raiz = psutil.Process(os.getpid())
        while not self._parar.is_set():
            try:
                procesos = [raiz] + raiz.children(recursive=True)
                cpu_total = 0.0
                mem_total = 0
                for p in procesos:
                    try:
                        if p.pid not in self._vistos:
                            p.cpu_percent(None)
                            self._vistos[p.pid] = True
                            continue
                        cpu_total += p.cpu_percent(None)
                        mem_total += p.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                        continue
                if 0 <= cpu_total <= self._cpu_max_valido:
                    self.muestras_cpu.append(cpu_total)
                self.muestras_mem.append(mem_total / (1024 * 1024))
            except Exception:
                pass
            time.sleep(self.intervalo)

    def iniciar(self):
        self._hilo = threading.Thread(target=self._loop, daemon=True)
        self._hilo.start()

    def detener(self):
        self._parar.set()
        if self._hilo:
            self._hilo.join(timeout=2)

    def resumen(self):
        cpu = self.muestras_cpu or [0.0]
        mem = self.muestras_mem or [0.0]
        return {
            "cpu_prom_pct": round(statistics.mean(cpu), 1),
            "cpu_max_pct": round(max(cpu), 1),
            "mem_prom_mb": round(statistics.mean(mem), 1),
            "mem_pico_mb": round(max(mem), 1),
        }


def nueva_sesion():
    return (
        SparkSession.builder
        .appName("Benchmark_3.6_3.7")
        .master("local[2]")
        .config("spark.driver.memory", "1536m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.constraintPropagation.enabled", "false")
        .getOrCreate()
    )


def leer_csv(spark, ruta, sep):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .option("sep", sep)
        .csv(ruta)
    )


def correr_pipeline(ds, ruta_csv, usar_cache, capturar_plan=None):
    spark = nueva_sesion()
    try:
        inicio = time.perf_counter()
        df = leer_csv(spark, ruta_csv, ds["sep"])
        df_final, total = ds["limpiar"](df, usar_cache)
        fin = time.perf_counter()

        if capturar_plan:
            with open(capturar_plan, "w", encoding="utf-8") as f:
                with redirect_stdout(f):
                    df_final.explain(mode="formatted")

        return fin - inicio, total
    finally:
        spark.stop()
        time.sleep(1)  # deja liberar el puerto/JVM antes de la siguiente corrida


def generar_replica(csv_real, factor, destino):
    with open(csv_real, "r", encoding="utf-8") as f:
        lineas = f.readlines()
    encabezado, filas = lineas[0], lineas[1:]
    with open(destino, "w", encoding="utf-8") as f:
        f.write(encabezado)
        for _ in range(factor):
            f.writelines(filas)
    return len(filas) * factor


def parte_3_6(ds):
    nombre = ds["nombre"]
    log(f"===== 3.6 OPTIMIZACION DEL RENDIMIENTO ({nombre}) =====")
    resultados = []

    correr_pipeline(ds, ds["csv"], usar_cache=False,
                     capturar_plan=os.path.join(OUT_DIR, f"plan_sin_cache_{nombre}.txt"))
    correr_pipeline(ds, ds["csv"], usar_cache=True,
                     capturar_plan=os.path.join(OUT_DIR, f"plan_con_cache_{nombre}.txt"))

    for rep in range(1, REPETICIONES + 1):
        for modo, usar_cache in [("sin_cache", False), ("con_cache", True)]:
            tiempo, total = correr_pipeline(ds, ds["csv"], usar_cache)
            log(f"3.6 [{nombre}] rep={rep} modo={modo} tiempo={tiempo:.2f}s filas_final={total}")
            resultados.append({"repeticion": rep, "modo": modo, "tiempo_segundos": round(tiempo, 2)})
            time.sleep(1)

    ruta_csv = os.path.join(OUT_DIR, f"resultados_3_6_optimizacion_{nombre}.csv")
    with open(ruta_csv, "w", encoding="utf-8") as f:
        f.write("repeticion,modo,tiempo_segundos\n")
        for r in resultados:
            f.write(f"{r['repeticion']},{r['modo']},{r['tiempo_segundos']}\n")

    return resultados


def parte_3_7(ds):
    nombre = ds["nombre"]
    log(f"===== 3.7 PRUEBA DE ESCALABILIDAD ({nombre}) =====")
    resultados = []

    for factor in FACTORES_ESCALABILIDAD:
        ruta_replica = os.path.join(WORK_DIR, f"replica_{nombre}_x{factor}.csv")
        filas = generar_replica(ds["csv"], factor, ruta_replica)
        log(f"[{nombre}] Replica x{factor} generada: {filas} filas, "
            f"{os.path.getsize(ruta_replica) / (1024*1024):.1f} MB")

        monitor = MonitorRecursos(intervalo=0.5)
        monitor.iniciar()
        tiempo, total = correr_pipeline(ds, ruta_replica, usar_cache=True)
        monitor.detener()
        resumen = monitor.resumen()

        log(f"[{nombre}] x{factor}: tiempo={tiempo:.2f}s filas_final={total} "
            f"cpu_prom={resumen['cpu_prom_pct']}% cpu_max={resumen['cpu_max_pct']}% "
            f"mem_prom={resumen['mem_prom_mb']}MB mem_pico={resumen['mem_pico_mb']}MB")

        resultados.append({
            "factor": factor, "filas_entrada": filas, "filas_final": total,
            "tiempo_segundos": round(tiempo, 2), **resumen,
        })
        os.remove(ruta_replica)
        time.sleep(2)  # pausa entre corridas para no mantener la CPU cargada

    ruta_csv = os.path.join(OUT_DIR, f"resultados_3_7_escalabilidad_{nombre}.csv")
    with open(ruta_csv, "w", encoding="utf-8") as f:
        f.write("factor,filas_entrada,filas_final,tiempo_segundos,cpu_prom_pct,cpu_max_pct,mem_prom_mb,mem_pico_mb\n")
        for r in resultados:
            f.write(
                f"{r['factor']},{r['filas_entrada']},{r['filas_final']},{r['tiempo_segundos']},"
                f"{r['cpu_prom_pct']},{r['cpu_max_pct']},{r['mem_prom_mb']},{r['mem_pico_mb']}\n"
            )

    return resultados


def tabla_3_6_md(nombre, res_3_6):
    por_modo = {}
    for r in res_3_6:
        por_modo.setdefault(r["modo"], []).append(r["tiempo_segundos"])
    prom_sin = statistics.mean(por_modo["sin_cache"])
    prom_con = statistics.mean(por_modo["con_cache"])
    mejora_pct = (prom_sin - prom_con) / prom_sin * 100

    lineas = [f"\n### {nombre}\n"]
    lineas.append("| Repeticion | Sin cache (s) | Con cache (s) |")
    lineas.append("|---|---|---|")
    for rep in range(1, REPETICIONES + 1):
        sin = next(r["tiempo_segundos"] for r in res_3_6 if r["repeticion"] == rep and r["modo"] == "sin_cache")
        con = next(r["tiempo_segundos"] for r in res_3_6 if r["repeticion"] == rep and r["modo"] == "con_cache")
        lineas.append(f"| {rep} | {sin} | {con} |")
    lineas.append(f"| **Promedio** | **{prom_sin:.2f}** | **{prom_con:.2f}** |")
    lineas.append(f"\nMejora: **{mejora_pct:.1f}%** menos tiempo con cache. "
                  f"Planes: `plan_sin_cache_{nombre}.txt` / `plan_con_cache_{nombre}.txt`.\n")
    return "\n".join(lineas)


def tabla_3_7_md(nombre, res_3_7):
    lineas = [f"\n### {nombre}\n"]
    lineas.append("| Factor | Filas | Tiempo (s) | CPU prom (%) | CPU max (%) | Mem prom (MB) | Mem pico (MB) |")
    lineas.append("|---|---|---|---|---|---|---|")
    for r in res_3_7:
        lineas.append(
            f"| x{r['factor']} | {r['filas_entrada']} | {r['tiempo_segundos']} | "
            f"{r['cpu_prom_pct']} | {r['cpu_max_pct']} | {r['mem_prom_mb']} | {r['mem_pico_mb']} |"
        )
    tiempo_1x = res_3_7[0]["tiempo_segundos"]
    for r in res_3_7[1:]:
        factor_volumen = r["factor"] / res_3_7[0]["factor"]
        factor_tiempo = r["tiempo_segundos"] / tiempo_1x
        lineas.append(
            f"\n- x{r['factor']}: volumen x{factor_volumen:.0f}, tiempo x{factor_tiempo:.2f} "
            f"({'sublineal/estable' if factor_tiempo <= factor_volumen else 'se degrada mas rapido que el volumen'})."
        )
    return "\n".join(lineas)


def generar_resumen_md(resultados_por_dataset):
    lineas = []
    lineas.append("# Evidencia 3.6 y 3.7 (Anexo C) — IRA, Malaria, SENAMHI\n")
    lineas.append(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    lineas.append("Script fuente: `src/benchmarking/benchmark_rendimiento.py` "
                   "(reproducible, no requiere HDFS; Spark local[2], 1.5 GB driver). "
                   "Corre sobre los 3 datasets reales del proyecto.\n")

    lineas.append("\n## 3.6 Optimizacion del rendimiento (los 3 datasets)\n")
    lineas.append("**Tecnica aplicada:** `.cache()` del DataFrame tras los filtros, antes de "
                   "las acciones repetidas (`count`, `groupBy`, `distinct`, `dropDuplicates`). "
                   "Es la misma tecnica aplicada en `limpiar_ira.py`, `limpiar_malaria.py` y "
                   "`limpiar_senamhi.py`.\n")
    for nombre, (res_3_6, _res_3_7) in resultados_por_dataset.items():
        lineas.append(tabla_3_6_md(nombre, res_3_6))

    lineas.append("\n## 3.7 Prueba de escalabilidad (los 3 datasets)\n")
    lineas.append("Replicas x10 y x100 de cada conjunto real, usadas solo para medir el "
                   "pipeline (no se usan en el analisis exploratorio, el entrenamiento ni "
                   "conclusiones de negocio).\n")
    for nombre, (_res_3_6, res_3_7) in resultados_por_dataset.items():
        lineas.append(tabla_3_7_md(nombre, res_3_7))

    ruta_md = os.path.join(OUT_DIR, "resumen_3_6_3_7.md")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))
    log(f"Resumen escrito en {ruta_md}")


def main():
    log("Inicio de benchmark 3.6 / 3.7 (IRA, Malaria, SENAMHI)")
    resultados_por_dataset = {}
    for ds in DATASETS:
        res_3_6 = parte_3_6(ds)
        res_3_7 = parte_3_7(ds)
        resultados_por_dataset[ds["nombre"]] = (res_3_6, res_3_7)
    generar_resumen_md(resultados_por_dataset)
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    log("Benchmark finalizado.")


if __name__ == "__main__":
    main()
