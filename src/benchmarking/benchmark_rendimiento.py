"""
Benchmark para los puntos 3.6 (optimizacion del rendimiento) y
3.7 (prueba de escalabilidad) del informe.

Se ejecuta en modo local con recursos limitados a proposito
(local[2], 1.5 GB de memoria de driver, UI de Spark apagada) para
no competir por CPU/RAM con otras aplicaciones (videollamada,
navegador) mientras corre.

Dataset real usado: IRA_La_Libertad_2015_2024.csv (salida ya
filtrada a La Libertad, 38 087 filas). Las replicas x10 y x100
se generan solo para medir el comportamiento del pipeline, tal
como exige la guia: no se usan en el analisis exploratorio ni en
el entrenamiento del modelo.

Salidas (docs/evidencias/):
  - resultados_3_6_optimizacion.csv
  - resultados_3_7_escalabilidad.csv
  - plan_sin_cache.txt / plan_con_cache.txt   (planes de ejecucion)
  - log_ejecucion.txt                         (log completo, con hora real)
  - resumen_3_6_3_7.md                        (tablas listas para pegar)
"""

import json
import os
import shutil
import statistics
import sys
import threading
import time
from contextlib import redirect_stdout
from datetime import datetime

import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, regexp_replace, trim, upper

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATASET_REAL = os.path.join(BASE_DIR, "IRA_La_Libertad_2015_2024.csv")
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
                            # primer avistamiento: solo establece la linea base,
                            # no se usa esta lectura (evita picos espurios)
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


def leer_csv(spark, ruta):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .option("sep", ";")
        .csv(ruta)
    )


def limpiar(df, usar_cache):
    """Replica las transformaciones reales de src/preprocessing/limpiar_ira.py."""
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

    # Mismas acciones que dispara el script real (count/groupBy/distinct/dropDuplicates)
    df.groupBy("Ano").count().orderBy("Ano").collect()
    df.select("Departamento").distinct().collect()
    df = df.dropDuplicates()
    total = df.count()
    return df, total


def correr_pipeline(ruta_csv, usar_cache, capturar_plan=None):
    spark = nueva_sesion()
    try:
        inicio = time.perf_counter()
        df = leer_csv(spark, ruta_csv)
        df_final, total = limpiar(df, usar_cache)
        fin = time.perf_counter()

        if capturar_plan:
            with open(capturar_plan, "w", encoding="utf-8") as f:
                with redirect_stdout(f):
                    df_final.explain(mode="formatted")

        return fin - inicio, total
    finally:
        spark.stop()
        time.sleep(1)  # deja liberar el puerto/JVM antes de la siguiente corrida


def generar_replica(factor, destino):
    with open(DATASET_REAL, "r", encoding="utf-8") as f:
        lineas = f.readlines()
    encabezado, filas = lineas[0], lineas[1:]
    with open(destino, "w", encoding="utf-8") as f:
        f.write(encabezado)
        for _ in range(factor):
            f.writelines(filas)
    return len(filas) * factor


def parte_3_6():
    log("===== 3.6 OPTIMIZACION DEL RENDIMIENTO =====")
    resultados = []

    correr_pipeline(
        DATASET_REAL, usar_cache=False,
        capturar_plan=os.path.join(OUT_DIR, "plan_sin_cache.txt"),
    )
    correr_pipeline(
        DATASET_REAL, usar_cache=True,
        capturar_plan=os.path.join(OUT_DIR, "plan_con_cache.txt"),
    )

    for rep in range(1, REPETICIONES + 1):
        for modo, usar_cache in [("sin_cache", False), ("con_cache", True)]:
            tiempo, total = correr_pipeline(DATASET_REAL, usar_cache)
            log(f"3.6 rep={rep} modo={modo} tiempo={tiempo:.2f}s filas_final={total}")
            resultados.append({"repeticion": rep, "modo": modo, "tiempo_segundos": round(tiempo, 2)})
            time.sleep(1)

    ruta_csv = os.path.join(OUT_DIR, "resultados_3_6_optimizacion.csv")
    with open(ruta_csv, "w", encoding="utf-8") as f:
        f.write("repeticion,modo,tiempo_segundos\n")
        for r in resultados:
            f.write(f"{r['repeticion']},{r['modo']},{r['tiempo_segundos']}\n")

    return resultados


def parte_3_7():
    log("===== 3.7 PRUEBA DE ESCALABILIDAD =====")
    resultados = []

    for factor in FACTORES_ESCALABILIDAD:
        ruta_replica = os.path.join(WORK_DIR, f"replica_x{factor}.csv")
        filas = generar_replica(factor, ruta_replica)
        log(f"Replica x{factor} generada: {filas} filas, "
            f"{os.path.getsize(ruta_replica) / (1024*1024):.1f} MB")

        monitor = MonitorRecursos(intervalo=0.5)
        monitor.iniciar()
        tiempo, total = correr_pipeline(ruta_replica, usar_cache=True)
        monitor.detener()
        resumen = monitor.resumen()

        log(f"x{factor}: tiempo={tiempo:.2f}s filas_final={total} "
            f"cpu_prom={resumen['cpu_prom_pct']}% cpu_max={resumen['cpu_max_pct']}% "
            f"mem_prom={resumen['mem_prom_mb']}MB mem_pico={resumen['mem_pico_mb']}MB")

        resultados.append({
            "factor": factor, "filas_entrada": filas, "filas_final": total,
            "tiempo_segundos": round(tiempo, 2), **resumen,
        })
        os.remove(ruta_replica)
        time.sleep(2)  # pausa entre corridas para no mantener la CPU cargada

    ruta_csv = os.path.join(OUT_DIR, "resultados_3_7_escalabilidad.csv")
    with open(ruta_csv, "w", encoding="utf-8") as f:
        f.write("factor,filas_entrada,filas_final,tiempo_segundos,cpu_prom_pct,cpu_max_pct,mem_prom_mb,mem_pico_mb\n")
        for r in resultados:
            f.write(
                f"{r['factor']},{r['filas_entrada']},{r['filas_final']},{r['tiempo_segundos']},"
                f"{r['cpu_prom_pct']},{r['cpu_max_pct']},{r['mem_prom_mb']},{r['mem_pico_mb']}\n"
            )

    return resultados


def generar_resumen_md(res_3_6, res_3_7):
    por_modo = {}
    for r in res_3_6:
        por_modo.setdefault(r["modo"], []).append(r["tiempo_segundos"])
    prom_sin = statistics.mean(por_modo["sin_cache"])
    prom_con = statistics.mean(por_modo["con_cache"])
    mejora_pct = (prom_sin - prom_con) / prom_sin * 100

    base = res_3_7[0]
    filas_1x = base["filas_entrada"]
    tiempo_1x = base["tiempo_segundos"]

    lineas = []
    lineas.append("# Evidencia 3.6 y 3.7 (Anexo C)\n")
    lineas.append(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    lineas.append("Script fuente: `src/benchmarking/benchmark_rendimiento.py` "
                   "(reproducible, no requiere HDFS; Spark local[2], 1.5 GB driver).\n")

    lineas.append("\n## 3.6 Optimizacion del rendimiento\n")
    lineas.append("**Tecnica aplicada:** cacheo (`.cache()`) del DataFrame ya filtrado "
                   "(Departamento = La Libertad, periodo 2015-2024, semanas validas), "
                   "antes de las acciones repetidas (`count`, `groupBy`, `distinct`, "
                   "`dropDuplicates`).\n")
    lineas.append("**Por que funciona:** sin cache, cada accion posterior recalcula el plan "
                   "logico completo desde la lectura del CSV (hay 4 acciones encadenadas sobre "
                   "el mismo filtrado). Con `.cache()` el resultado filtrado se materializa en "
                   "memoria tras la primera accion y las siguientes lo reutilizan en vez de "
                   "reprocesar el CSV y los filtros.\n")

    lineas.append("\n### Tabla de repeticiones (3 corridas por modo, dataset real)\n")
    lineas.append("| Repeticion | Sin cache (s) | Con cache (s) |")
    lineas.append("|---|---|---|")
    for rep in range(1, REPETICIONES + 1):
        sin = next(r["tiempo_segundos"] for r in res_3_6 if r["repeticion"] == rep and r["modo"] == "sin_cache")
        con = next(r["tiempo_segundos"] for r in res_3_6 if r["repeticion"] == rep and r["modo"] == "con_cache")
        lineas.append(f"| {rep} | {sin} | {con} |")
    lineas.append(f"| **Promedio** | **{prom_sin:.2f}** | **{prom_con:.2f}** |")
    lineas.append(f"\nMejora promedio: **{mejora_pct:.1f}%** menos tiempo con cache.\n")
    lineas.append("Planes de ejecucion capturados: `docs/evidencias/plan_sin_cache.txt` "
                   "y `docs/evidencias/plan_con_cache.txt` (buscar `InMemoryTableScan` en el "
                   "segundo, ausente en el primero).\n")

    lineas.append("\n## 3.7 Prueba de escalabilidad\n")
    lineas.append(f"Conjunto real: `IRA_La_Libertad_2015_2024.csv` ({filas_1x} filas). "
                   "Replicas x10 y x100 generadas solo para medir el pipeline (no se usan en "
                   "el analisis exploratorio, el entrenamiento ni conclusiones de negocio).\n")

    lineas.append("\n### Tabla de tiempos y consumo de recursos\n")
    lineas.append("| Factor | Filas | Tiempo (s) | CPU prom (%) | CPU max (%) | Mem prom (MB) | Mem pico (MB) |")
    lineas.append("|---|---|---|---|---|---|---|")
    for r in res_3_7:
        lineas.append(
            f"| x{r['factor']} | {r['filas_entrada']} | {r['tiempo_segundos']} | "
            f"{r['cpu_prom_pct']} | {r['cpu_max_pct']} | {r['mem_prom_mb']} | {r['mem_pico_mb']} |"
        )

    lineas.append("\n### Analisis\n")
    for r in res_3_7[1:]:
        factor_volumen = r["factor"] / res_3_7[0]["factor"]
        factor_tiempo = r["tiempo_segundos"] / tiempo_1x
        lineas.append(
            f"- x{r['factor']}: volumen x{factor_volumen:.0f}, tiempo x{factor_tiempo:.2f} "
            f"({'sublineal/estable' if factor_tiempo <= factor_volumen else 'se degrada mas rapido que el volumen'})."
        )

    lineas.append(
        "\n_(Completar manualmente tras ver los numeros: indicar si el crecimiento del tiempo "
        "fue proporcional al volumen o se degrado antes, y en que factor una sola maquina con "
        "estos recursos dejaria de alcanzar, segun la tendencia de memoria pico observada.)_\n"
    )

    ruta_md = os.path.join(OUT_DIR, "resumen_3_6_3_7.md")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))
    log(f"Resumen escrito en {ruta_md}")


def main():
    log("Inicio de benchmark 3.6 / 3.7")
    res_3_6 = parte_3_6()
    res_3_7 = parte_3_7()
    generar_resumen_md(res_3_6, res_3_7)
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    log("Benchmark finalizado.")


if __name__ == "__main__":
    main()
