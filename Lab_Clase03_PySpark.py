# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Laboratorio · Clase 3 — Procesamiento con PySpark
# MAGIC **Curso:** Ingeniería de Datos e IA con Databricks · **Caso:** sistema de ventas de la *Comercializadora Andina* (el mismo de la Clase 2)
# MAGIC
# MAGIC En la Clase 2 construimos el sistema de ventas con **SQL**. Hoy trabajamos sobre **las mismas tablas y los mismos archivos**,
# MAGIC pero con **PySpark**: la API de Python de Apache Spark. Primero resolvemos **las mismas necesidades del negocio** con
# MAGIC **métodos de DataFrame** y las graficamos; después vemos **cómo ejecutó Spark** ese código (driver, executors, planes);
# MAGIC y al final leemos los **archivos crudos** para construir una tabla oro.
# MAGIC
# MAGIC | Sección | Qué haces |
# MAGIC |---|---|
# MAGIC | 0 | Preparar el notebook y comprobar que existen las tablas de la Clase 2 |
# MAGIC | 1 | Manipular datos: métodos, funciones, UDF y funciones de ventana (lo de la Clase 2, ahora en PySpark) |
# MAGIC | 2 | Visualizar: `display()` y gráficos con `pandas .plot()` |
# MAGIC | 3 | Arquitectura: sesión de Spark, Spark Connect y particiones |
# MAGIC | 4 | Driver vs executors: qué se calcula en el clúster y qué llega al driver |
# MAGIC | 5 | Lazy evaluation, DAG, plan lógico y plan físico (`explain`) |
# MAGIC | 6 | Leer y escribir CSV, JSON, Parquet y Delta con PySpark |
# MAGIC | 7 | Laboratorio integrador: de los archivos a una tabla oro y su gráfico |
# MAGIC | 8 | Retos y limpieza |
# MAGIC
# MAGIC **Antes de empezar**
# MAGIC 1. Debes haber ejecutado el laboratorio de la **Clase 2** (secciones 4, 6 y 7): necesitamos el catálogo `comercializadora`,
# MAGIC    el schema `sistema_ventas`, sus tablas y los 3 archivos del volumen `archivos`.
# MAGIC 2. Pulsa **Connect** (arriba a la derecha) y elige **Serverless**.
# MAGIC 3. Ejecuta las celdas **en orden** con **Shift + Enter** y lee la explicación que está **antes** de cada celda.
# MAGIC
# MAGIC **Cómo leer este notebook**
# MAGIC - Es un notebook de **Python**. Cada celda tiene comentarios (`#`) que explican la línea.
# MAGIC - `spark` es la sesión de Spark que Databricks crea por ti. `F` será el módulo de funciones (`pyspark.sql.functions`).
# MAGIC - `display(df)` muestra un DataFrame como tabla interactiva.
# MAGIC
# MAGIC Fuentes oficiales: [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html) ·
# MAGIC [RDD Programming Guide › transformaciones y acciones](https://spark.apache.org/docs/latest/rdd-programming-guide.html) ·
# MAGIC [DataFrame.explain](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html) ·
# MAGIC [Spark Connect vs Spark Classic](https://learn.microsoft.com/azure/databricks/spark/connect-vs-classic) ·
# MAGIC [Límites de serverless](https://learn.microsoft.com/azure/databricks/compute/serverless/limitations) ·
# MAGIC [PySpark en Databricks](https://learn.microsoft.com/azure/databricks/pyspark/basics) ·
# MAGIC [UDFs](https://learn.microsoft.com/azure/databricks/udf/) ·
# MAGIC [pandas DataFrame.plot](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.plot.html). Probado en Databricks Free Edition (serverless), noviembre 2026.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Preparar el notebook
# MAGIC ### 0.1 · Importaciones y variables
# MAGIC - `from pyspark.sql import functions as F`: trae el módulo de **funciones** de Spark con el nombre corto `F` (`F.col`, `F.sum`, `F.when`…). Es la convención más usada.
# MAGIC - `from pyspark.sql.window import Window`: para las **funciones de ventana** (sección 1).
# MAGIC - `from pyspark.sql import types as T`: los **tipos de datos** (`T.StringType()`…), que usaremos en las UDF.
# MAGIC - Guardamos en **variables** los nombres que se repiten: si tu catálogo se llama distinto, solo cambias esta celda.
# MAGIC   - `f"…{CATALOGO}…"` es un **f-string**: Python reemplaza lo que va entre llaves por el valor de la variable.
# MAGIC - **Dónde quedan los archivos:** todo se guarda en **volúmenes de Unity Catalog** (rutas `/Volumes/<catálogo>/<schema>/<volumen>/…`), no en DBFS.
# MAGIC   - **Entrada:** el volumen `archivos` de la Clase 2 (solo lo leemos).
# MAGIC   - **Salida:** un volumen propio de la Clase 3, `analisis_pyspark.salidas`, que se crea en la celda 0.2.

# COMMAND ----------

from pyspark.sql import functions as F        # funciones de Spark: F.col, F.sum, F.when...
from pyspark.sql import types as T            # tipos de datos: T.StringType(), T.IntegerType()...
from pyspark.sql.window import Window         # funciones de ventana
import time                                   # para medir tiempos

CATALOGO = "comercializadora"                 # catálogo creado en la Clase 2
SCHEMA   = "sistema_ventas"                   # schema con las tablas del sistema
SALIDA   = "analisis_pyspark"                 # schema nuevo para lo que creemos hoy
VOLUMEN  = "salidas"                          # volumen nuevo para los archivos que escribamos hoy

ARCHIVOS = f"/Volumes/{CATALOGO}/{SCHEMA}/archivos"          # volumen de la Clase 2 (entrada: 3 archivos)
SALIDAS  = f"/Volumes/{CATALOGO}/{SALIDA}/{VOLUMEN}"         # volumen de la Clase 3 (salida)

print("Archivos de entrada:", ARCHIVOS)
print("Archivos de salida :", SALIDAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.2 · ¿Existen las tablas de la Clase 2?
# MAGIC - `spark.catalog.tableExists("catálogo.schema.tabla")` devuelve `True` si la tabla (o vista) existe.
# MAGIC - `for tabla in [...]:` recorre la lista y comprueba cada nombre.
# MAGIC - `"✔" if existe else "✘"` es un **condicional en una línea**: elige un símbolo según el resultado.
# MAGIC - Al final, `spark.sql("CREATE SCHEMA IF NOT EXISTS …")` crea el schema `analisis_pyspark` donde guardaremos las tablas de hoy,
# MAGIC   y `CREATE VOLUME IF NOT EXISTS …` crea dentro de él el **volumen gestionado** `salidas` para los archivos (Databricks administra el almacenamiento).
# MAGIC
# MAGIC **Qué esperar:** las 6 con ✔ y la ruta `/Volumes/comercializadora/analisis_pyspark/salidas`. Si alguna sale con ✘, vuelve a ejecutar el notebook de la Clase 2 (secciones 4, 6, 7 y 8).

# COMMAND ----------

tablas = ["clientes", "productos", "vendedores", "ventas", "ventas_detalle", "cumplimiento_metas"]
for tabla in tablas:
    existe = spark.catalog.tableExists(f"{CATALOGO}.{SCHEMA}.{tabla}")
    print("✔" if existe else "✘", f"{CATALOGO}.{SCHEMA}.{tabla}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SALIDA} COMMENT 'Resultados del laboratorio de la Clase 3 (PySpark)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SALIDA}.{VOLUMEN} COMMENT 'Archivos escritos en la Clase 3 (CSV, JSON, Parquet)'")

print(f"\nSchema de salida listo : {CATALOGO}.{SALIDA}")
print(f"Volumen de salida listo: {SALIDAS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.3 · Las tablas de la Clase 2 como DataFrames
# MAGIC - `spark.table("catálogo.schema.tabla")`: crea un **DataFrame** a partir de una tabla de Unity Catalog. Un DataFrame es una
# MAGIC   tabla distribuida, organizada en columnas con nombre y tipo.
# MAGIC - `ventas.printSchema()`: muestra las columnas, su tipo y si aceptan nulos (`nullable`).
# MAGIC - `ventas.columns`: la lista de nombres de columnas.
# MAGIC
# MAGIC **Qué esperar:** las 15 columnas de `ventas` que creamos en la Clase 2. Desde aquí, todo el notebook usa estos cuatro DataFrames.

# COMMAND ----------

ventas     = spark.table(f"{CATALOGO}.{SCHEMA}.ventas")       # DataFrame de la tabla ventas
clientes   = spark.table(f"{CATALOGO}.{SCHEMA}.clientes")
productos  = spark.table(f"{CATALOGO}.{SCHEMA}.productos")
vendedores = spark.table(f"{CATALOGO}.{SCHEMA}.vendedores")

ventas.printSchema()                                            # columnas, tipos y nullable
print(len(ventas.columns), "columnas")

# COMMAND ----------

display(ventas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Manipulación de datos con PySpark
# MAGIC Empezamos por lo que ya sabes: en la Clase 2 resolviste estas necesidades del negocio con **Spark SQL**. Ahora las resolvemos
# MAGIC con **métodos de DataFrame** sobre las mismas tablas, y comprobamos que dan los mismos resultados.
# MAGIC Equivalencias con SQL: `select` = `SELECT` · `filter`/`where` = `WHERE` · `groupBy().agg()` = `GROUP BY` · `orderBy` = `ORDER BY` ·
# MAGIC `join` = `JOIN` · `withColumn` = columna calculada · `F.when` = `CASE WHEN`.
# MAGIC
# MAGIC ### 1.1 · Seleccionar, renombrar y calcular columnas
# MAGIC *«Necesito el detalle de cada venta con el valor bruto y el descuento en pesos.»*
# MAGIC - `F.col("nombre")`: referencia a una columna. Con columnas se puede operar: `F.col("cantidad") * F.col("precio_unitario")`.
# MAGIC - `.alias("nuevo")`: nombre de la columna resultante (como `AS` en SQL).
# MAGIC - `F.coalesce(F.col("descuento_pct"), F.lit(0))`: usa 0 si el descuento es nulo. `F.lit(0)` crea una columna con un valor fijo.
# MAGIC - `.withColumn("nombre", expresión)`: agrega (o reemplaza) **una** columna.
# MAGIC - `F.round(x, 0)`: redondea.
# MAGIC
# MAGIC **Qué esperar:** 5 ventas con `valor_bruto`, `descuento_cop` y `valor_neto`.

# COMMAND ----------

detalle = (
    ventas.select(
        "id_venta", "cantidad", "precio_unitario", "descuento_pct", "valor_neto"
    )
    .withColumn("valor_bruto", F.col("cantidad") * F.col("precio_unitario"))
    .withColumn(
        "descuento_cop",
        F.round(F.col("valor_bruto") * F.coalesce(F.col("descuento_pct"), F.lit(0)), 0),
    )
)

# COMMAND ----------

display(detalle.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 · Filtrar con varias condiciones
# MAGIC *«Finanzas necesita las ventas en línea con tarjeta de crédito a 12 cuotas o más, del segundo semestre.»*
# MAGIC - En PySpark las condiciones se combinan con `&` (y), `|` (o) y `~` (no). **Cada condición va entre paréntesis.**
# MAGIC - `F.col("fecha_venta") >= "2025-07-01"`: compara fechas con un texto en formato año-mes-día.
# MAGIC - `.orderBy(F.desc("valor_neto"))`: de mayor a menor.
# MAGIC
# MAGIC **Qué esperar:** 107 ventas (igual que en la Clase 2 con SQL).

# COMMAND ----------

credito_largo = (ventas
                 .filter((F.col("canal") == "En línea") &
                         (F.col("metodo_pago") == "Tarjeta de crédito") &
                         (F.col("cuotas") >= 12) &
                         (F.col("fecha_venta") >= "2025-07-01"))
                 .select("id_venta", "fecha_venta", "cuotas", "valor_neto")
                 .orderBy(F.desc("valor_neto")))
print(credito_largo.count(), "ventas")
display(credito_largo.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 · Texto, listas y rangos
# MAGIC *«Buscar productos de escritorio, y las ventas de Oficina u Hogar de la primera semana de diciembre.»*
# MAGIC - `F.col("nombre_producto").ilike("%escritorio%")`: contiene «escritorio» sin importar mayúsculas (`%` = cualquier texto).
# MAGIC - `.isin("Hogar", "Oficina")`: el valor está en la lista.
# MAGIC - `.between("2025-12-01", "2025-12-07")`: entre dos valores, **incluidos** los extremos.
# MAGIC
# MAGIC **Qué esperar:** 2 productos (Escritorio en L y Lámpara LED de escritorio); y en esa semana, Hogar 12 ventas y Oficina 18.

# COMMAND ----------

display(productos.filter(F.col("nombre_producto").ilike("%escritorio%"))
                 .select("nombre_producto", "categoria", "precio_lista"))

semana = (ventas.join(productos.select("id_producto", "categoria"), "id_producto")
                .filter(F.col("categoria").isin("Hogar", "Oficina") &
                        F.col("fecha_venta").between("2025-12-01", "2025-12-07"))
                .groupBy("categoria").count())
display(semana)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 · Condicionales con `F.when` (el `CASE WHEN` de PySpark)
# MAGIC *«Marketing quiere segmentar las ventas por tamaño del ticket.»*
# MAGIC - `F.when(condición, valor).when(condición, valor).otherwise(valor)`: se evalúa **en orden**; `otherwise` cubre el resto.
# MAGIC - `.agg(F.count("*").alias("ventas"), F.sum("valor_neto").alias("ventas_netas"))`: varias agregaciones a la vez.
# MAGIC
# MAGIC **Qué esperar:** Pequeña 1407 · Mediana 482 · Grande 112 (mismos números que en la Clase 2).

# COMMAND ----------

tickets = (ventas
           .filter(F.col("estado") == "Completada")
           .withColumn("tamano_ticket",
                       F.when(F.col("valor_neto") >= 2_000_000, "3 · Grande")
                        .when(F.col("valor_neto") >=   500_000, "2 · Mediana")
                        .otherwise("1 · Pequeña"))
           .groupBy("tamano_ticket")
           .agg(F.count("*").alias("ventas"),
                F.sum("valor_neto").alias("ventas_netas"))
           .orderBy("tamano_ticket"))
display(tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 · Nulos
# MAGIC *«¿Cuántas ventas no tienen el descuento registrado, por canal?»*
# MAGIC - `F.col("descuento_pct").isNull()`: `True` si el valor es nulo.
# MAGIC - `F.count_if(condición)`: cuenta las filas que cumplen la condición.
# MAGIC - `.fillna({"descuento_pct": 0})`: reemplaza los nulos de esa columna por 0 (devuelve un **DataFrame nuevo**; la tabla no cambia).
# MAGIC
# MAGIC **Qué esperar:** Tienda física 135 · En línea 69 · Teléfono 26; y 0 nulos después de `fillna`.

# COMMAND ----------

display(ventas.groupBy("canal")
              .agg(F.count("*").alias("ventas"),
                   F.count_if(F.col("descuento_pct").isNull()).alias("sin_descuento")))

sin_nulos = ventas.fillna({"descuento_pct": 0})
print("Nulos después de fillna:", sin_nulos.filter(F.col("descuento_pct").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 · Fechas y agrupación por mes
# MAGIC *«Ventas netas por mes de 2025.»*
# MAGIC - `F.date_trunc("month", F.col("fecha_venta"))`: lleva cada fecha al primer día de su mes (devuelve un timestamp).
# MAGIC - `F.to_date(...)`: lo deja como fecha.
# MAGIC - `F.month(...)`, `F.year(...)`, `F.dayofweek(...)`: partes de una fecha.
# MAGIC
# MAGIC **Qué esperar:** 12 filas, de enero (79,3 M) a diciembre (186,0 M).

# COMMAND ----------

mensual = (ventas
           .filter(F.col("estado") == "Completada")
           .groupBy(F.to_date(F.date_trunc("month", "fecha_venta")).alias("mes"))
           .agg(F.sum("valor_neto").alias("ventas_netas"),
                F.count("*").alias("ventas"))
           .orderBy("mes"))
display(mensual)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 · Uniones (`join`)
# MAGIC *«Ver cada venta con el nombre del cliente, del producto y del vendedor.»*
# MAGIC - `ventas.join(clientes, "id_cliente")`: **inner join** por la columna común (solo filas con pareja).
# MAGIC - `how="left"`: conserva **todas** las ventas aunque no tengan vendedor (las ventas en línea).
# MAGIC - `F.coalesce(F.col("nombre_vendedor"), F.lit("Venta en línea"))`: texto cuando no hay vendedor.
# MAGIC - `.select(...)` con columnas de las tres tablas: tras el join, todas están disponibles por su nombre.
# MAGIC
# MAGIC **Qué esperar:** la venta 100004 aparece como «Venta en línea».

# COMMAND ----------

detalle_nombres = (ventas
                   .join(clientes,   "id_cliente")                    # inner join
                   .join(productos,  "id_producto")                   # inner join
                   .join(vendedores, "id_vendedor", how="left")       # left join: no perder ventas en línea
                   .select("id_venta", "fecha_venta",
                           F.col("nombre_completo").alias("cliente"),
                           "nombre_producto",
                           F.coalesce(F.col("nombre_vendedor"), F.lit("Venta en línea")).alias("vendedor"),
                           "canal", "valor_neto")
                   .filter(F.col("id_venta").between(100001, 100005))
                   .orderBy("id_venta"))
display(detalle_nombres)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 · Lo que no tiene pareja (`left_anti`)
# MAGIC *«¿Qué productos nunca se vendieron?»*
# MAGIC - `how="left_anti"`: filas de la izquierda **sin** pareja en la derecha.
# MAGIC
# MAGIC **Qué esperar:** Tableta gráfica y Banda elástica.

# COMMAND ----------

display(productos.join(ventas, "id_producto", how="left_anti")
                 .select("id_producto", "nombre_producto", "activo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 · UDF: funciones definidas por el usuario
# MAGIC Una **UDF** (*user-defined function*) es una función de Python que Spark aplica a cada fila. Sirve cuando la lógica **no existe**
# MAGIC como función de Spark. Ejemplo: *«generar un código de producto sin tildes ni espacios»* (`Portátil 14 pulgadas` → `PORTATIL-14-PULGADAS`).
# MAGIC - `unicodedata.normalize("NFKD", texto)` separa cada letra de su tilde; `encode("ascii", "ignore")` elimina las tildes.
# MAGIC - `@F.udf(returnType=T.StringType())`: **decorador** que convierte la función de Python en una UDF de Spark que devuelve texto.
# MAGIC - `if texto is None: return None`: las UDF deben manejar los nulos.
# MAGIC
# MAGIC **Qué esperar:** la columna `codigo` en mayúsculas, sin tildes y con guiones.

# COMMAND ----------

import unicodedata

@F.udf(returnType=T.StringType())                     # UDF de Python: se ejecuta fila por fila
def codigo_producto(texto):
    if texto is None:
        return None
    sin_tildes = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return sin_tildes.upper().replace(" ", "-").replace(",", "")

display(productos.select("nombre_producto", codigo_producto("nombre_producto").alias("codigo")).limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.10 · pandas UDF (vectorizada) y la alternativa nativa
# MAGIC - Una UDF de Python procesa **fila por fila**: cada valor viaja entre Spark y Python. Es lo más lento.
# MAGIC - Una **pandas UDF** (`@F.pandas_udf`) recibe **lotes** de filas como `pandas.Series` (usando Apache Arrow): según la documentación de Databricks puede ser hasta **100× más rápida**.
# MAGIC   - `serie.str.normalize(...)`, `.str.encode(...)`… aplican la operación a toda la serie a la vez.
# MAGIC - **Lo mejor, si existe:** una **función nativa** de Spark, optimizada por Catalyst. `F.translate` cambia caracteres uno a uno
# MAGIC   (cada letra con tilde por la misma sin tilde), `F.upper` pasa a mayúsculas y `F.regexp_replace` quita las comas y cambia los espacios por guiones.
# MAGIC - `.filter(... != ...)` al final cuenta las filas donde alguna versión difiere: debe dar 0.
# MAGIC
# MAGIC **Qué esperar:** las tres columnas con el mismo resultado (por ejemplo `Licuadora 1,5 litros` → `LICUADORA-15-LITROS`) y `Diferencias: 0`.

# COMMAND ----------

import pandas as pd

@F.pandas_udf(T.StringType())                         # pandas UDF: procesa lotes (Series)
def codigo_producto_vec(serie: pd.Series) -> pd.Series:
    return (serie.str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("ascii")
                 .str.upper().str.replace(" ", "-", regex=False).str.replace(",", "", regex=False))

sin_tildes_nativo = F.upper(F.translate("nombre_producto", "áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN"))   # quitar tildes
codigo_nativo = F.regexp_replace(F.regexp_replace(sin_tildes_nativo, ",", ""), " ", "-")      # sin comas; espacios -> "-"

comparacion = productos.select("nombre_producto",
                               codigo_producto("nombre_producto").alias("udf_python"),
                               codigo_producto_vec("nombre_producto").alias("pandas_udf"),
                               codigo_nativo.alias("nativa"))
display(comparacion.limit(10))                    # las 10 primeras (incluye "Licuadora 1,5 litros")
print("Diferencias:", comparacion.filter((F.col("udf_python") != F.col("pandas_udf")) |
                                         (F.col("udf_python") != F.col("nativa"))).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.11 · Funciones de ventana: ranking
# MAGIC *«Ranking de vendedores del año y aporte de cada uno al total.»*
# MAGIC - `Window.orderBy(F.desc("ventas_netas"))`: define **la ventana**: cómo se ordenan las filas para calcular.
# MAGIC - `F.rank().over(ventana)`: puesto según ese orden (los empates comparten puesto).
# MAGIC - `Window.partitionBy()` vacío: **todas** las filas en una sola ventana → `F.sum(...).over(...)` da el total general.
# MAGIC - Verás el aviso `No Partition Defined for Window operation! Moving all data to a single partition`: Spark avisa que, sin
# MAGIC   `partitionBy`, **todas las filas van a una sola partición** (un solo executor). Aquí son 8 filas y no importa; con millones
# MAGIC   de filas sería un cuello de botella. Lo entenderás a fondo en las secciones 3 y 4 (particiones, driver y executors).
# MAGIC
# MAGIC **Qué esperar:** el aviso y el ranking: 1.º Carolina Méndez (15,5 %) … 8.º Ricardo Pabón (6,1 %).

# COMMAND ----------

por_vendedor = (ventas
                .filter((F.col("estado") == "Completada") & F.col("id_vendedor").isNotNull())
                .join(vendedores, "id_vendedor")
                .groupBy("nombre_vendedor", "sede")
                .agg(F.sum("valor_neto").alias("ventas_netas")))

ventana_rank  = Window.orderBy(F.desc("ventas_netas"))       # ordenar de mayor a menor
ventana_total = Window.partitionBy()                          # todas las filas juntas

ranking = (por_vendedor
           .withColumn("puesto", F.rank().over(ventana_rank))
           .withColumn("participacion_pct",
                       F.round(F.col("ventas_netas") / F.sum("ventas_netas").over(ventana_total) * 100, 1))
           .orderBy("puesto"))
display(ranking)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.12 · Funciones de ventana: el mejor de cada grupo, mes anterior y acumulado
# MAGIC - `metas` es la tabla oro **`cumplimiento_metas`** que construiste en la Clase 2 con una CTE + CTAS (ventas de cada vendedor por mes frente a su meta).
# MAGIC - `Window.partitionBy("mes").orderBy(F.desc("ventas_mes"))`: la numeración **reinicia** en cada mes.
# MAGIC - `F.row_number().over(...) == 1`: el primero de cada mes (el equivalente de `QUALIFY` en SQL es filtrar después).
# MAGIC - `F.lag("ventas_netas").over(Window.orderBy("mes"))`: el valor de la **fila anterior**.
# MAGIC - `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`: desde la primera fila hasta la actual → **acumulado**.
# MAGIC
# MAGIC **Qué esperar:** el mejor vendedor de cada mes (12 filas) y, por mes, la variación frente al anterior y el acumulado (supera 1.000 M en noviembre).

# COMMAND ----------

metas = spark.table(f"{CATALOGO}.{SCHEMA}.cumplimiento_metas")
v_mes = Window.partitionBy("mes").orderBy(F.desc("ventas_mes"))
display(metas.withColumn("n", F.row_number().over(v_mes))       # 1, 2, 3... dentro de cada mes
             .filter(F.col("n") == 1)                            # el primero de cada mes
             .select("mes", "nombre_vendedor", "ventas_mes", "cumplimiento_pct")
             .orderBy("mes"))

# COMMAND ----------

v_tiempo = Window.orderBy("mes")                                                  # orden cronológico
v_acum   = Window.orderBy("mes").rowsBetween(Window.unboundedPreceding, Window.currentRow)

tendencia = (mensual
             .withColumn("mes_anterior", F.lag("ventas_netas").over(v_tiempo))        # fila anterior
             .withColumn("variacion_pct",
                         F.round((F.col("ventas_netas") / F.col("mes_anterior") - 1) * 100, 1))
             .withColumn("acumulado", F.sum("ventas_netas").over(v_acum)))            # suma acumulada
display(tendencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Visualización de datos con PySpark
# MAGIC Dos caminos:
# MAGIC 1. **`display(df)`**: tabla interactiva. Pulsa **+ › Visualization** sobre el resultado para crear un gráfico **sin código**
# MAGIC    (barras, líneas, tortas…), o **+ › Data Profile** para ver estadísticas de cada columna.
# MAGIC 2. **`df.toPandas().plot(...)`**: gráfico con código. Regla de oro (la explicamos en la sección 4): **agrega primero en Spark** y trae a pandas solo el resumen.
# MAGIC
# MAGIC Para el camino 2:
# MAGIC - `.toPandas()`: DataFrame de Spark → DataFrame de pandas (en el driver).
# MAGIC - `.astype(float)`: los montos `DECIMAL` llegan como objetos; los volvemos número. Dividimos entre `1e6` para ver **millones**.
# MAGIC - `pdf.plot(kind=..., x=..., y=..., title=...)`: `kind` = `"bar"`, `"barh"`, `"line"` o `"pie"`. `plt.show()` muestra el gráfico.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 · Visualización integrada (sin código)
# MAGIC Ejecuta la celda y, sobre la tabla del resultado, pulsa **+ › Visualization** → tipo **Bar** → eje X `canal`, eje Y `ventas_netas`.
# MAGIC Después prueba **+ › Data Profile**.

# COMMAND ----------

display(ventas.filter(F.col("estado") == "Completada")
              .groupBy("canal", "metodo_pago")
              .agg(F.sum("valor_neto").alias("ventas_netas")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 · Ventas por mes (línea)
# MAGIC `mensual` es el DataFrame de la sección 1.6 (ya agregado: 12 filas).
# MAGIC
# MAGIC **Qué esperar:** una línea con caída en febrero y pico en diciembre.

# COMMAND ----------

import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (10, 4)        # tamaño por defecto de los gráficos

pdf = mensual.toPandas()                        # 12 filas: ya está agregado en Spark
pdf["millones"] = pdf["ventas_netas"].astype(float) / 1e6
pdf.plot(kind="line", x="mes", y="millones", marker="o", legend=False,
         title="Ventas netas por mes, 2025 (millones de COP)")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 · Ranking de vendedores (barras horizontales)
# MAGIC - `kind="barh"`: barras horizontales, mejores para nombres largos.
# MAGIC - `pdf.iloc[::-1]`: invierte el orden de las filas para que el primero quede **arriba**.

# COMMAND ----------

pdf = ranking.toPandas()
pdf["millones"] = pdf["ventas_netas"].astype(float) / 1e6
pdf.iloc[::-1].plot(kind="barh", x="nombre_vendedor", y="millones", legend=False,
                    title="Ranking de vendedores 2025 (millones de COP)")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 · Tamaño del ticket (torta)
# MAGIC - `kind="pie"` con `y` = valores y `labels` = nombres; `autopct="%1.1f%%"` escribe el porcentaje de cada porción.

# COMMAND ----------

pdf = tickets.toPandas()
pdf["millones"] = pdf["ventas_netas"].astype(float) / 1e6
pdf.plot(kind="pie", y="millones", labels=pdf["tamano_ticket"], autopct="%1.1f%%",
         legend=False, ylabel="", figsize=(6, 6), title="Ventas netas por tamaño de ticket")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Arquitectura de Apache Spark
# MAGIC Todo lo que calculaste y graficaste en las secciones 1 y 2 lo ejecutó Spark. ¿Quién hizo el trabajo y cómo se repartió?
# MAGIC
# MAGIC Una **aplicación Spark** tiene un **driver** (el programa que coordina) y varios **executors** (procesos que hacen el trabajo
# MAGIC en los nodos del clúster). En **serverless**, Databricks administra ese clúster por ti; tu notebook es un **cliente** que le
# MAGIC envía las órdenes al servidor de Spark mediante **Spark Connect**.
# MAGIC
# MAGIC ### 3.1 · La sesión de Spark
# MAGIC - `spark.version`: versión del motor.
# MAGIC - `type(spark)`: la **clase** del objeto. Si su módulo contiene `connect`, tu notebook usa **Spark Connect** (cliente → servidor remoto).
# MAGIC
# MAGIC **Qué esperar:** la versión (por ejemplo `4.2.0`) y `pyspark.sql.connect.session.SparkSession`.

# COMMAND ----------

print("Versión de Spark:", spark.version)
print("Tipo de sesión  :", type(spark))      # ...connect... = Spark Connect (serverless)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · Particiones: la unidad de trabajo en paralelo
# MAGIC Spark divide los datos en **particiones**. Cada **tarea** (*task*) procesa **una** partición en un executor, así que más
# MAGIC particiones permiten más trabajo en paralelo.
# MAGIC - `F.spark_partition_id()`: devuelve el número de la partición en la que está cada fila.
# MAGIC - `.groupBy("particion").count()`: cuenta cuántas filas hay en cada partición.
# MAGIC - `.repartition(4)`: reparte las filas en **4** particiones (esto mueve datos entre executors: es un *shuffle*).
# MAGIC - `.orderBy("particion")`: ordena el resultado para leerlo mejor.
# MAGIC
# MAGIC **Qué esperar:** primero **una sola** partición con las 2238 filas (la tabla es pequeña) y luego 4 particiones de 559–560 filas.

# COMMAND ----------

por_particion = (ventas
                 .select(F.spark_partition_id().alias("particion"))   # n.º de partición de cada fila
                 .groupBy("particion").count()                        # filas por partición
                 .orderBy("particion"))
display(por_particion)

# COMMAND ----------

ventas_4 = ventas.repartition(4)                                       # repartir en 4 particiones
display(ventas_4.select(F.spark_partition_id().alias("particion"))
                .groupBy("particion").count()
                .orderBy("particion"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Driver vs executors
# MAGIC - Los **executors** procesan las particiones en paralelo (filtrar, agrupar, unir…).
# MAGIC - El **driver** coordina y recibe **solo** lo que pides de vuelta con una **acción**.
# MAGIC
# MAGIC | Acción | Qué llega al driver |
# MAGIC |---|---|
# MAGIC | `count()` | Un número |
# MAGIC | `show(n)` / `display(df)` | Una muestra para verla |
# MAGIC | `take(n)` / `first()` | `n` filas como objetos `Row` de Python |
# MAGIC | `collect()` | **Todas** las filas → cuidado con tablas grandes |
# MAGIC | `toPandas()` | **Todas** las filas como DataFrame de pandas → cuidado |
# MAGIC
# MAGIC ### 4.1 · Acciones que devuelven poco
# MAGIC - `ventas.count()`: los executors cuentan sus particiones y el driver solo suma los totales.
# MAGIC - `ventas.take(2)`: trae 2 filas como lista de objetos `Row`. `fila["canal"]` lee una columna de una fila.
# MAGIC
# MAGIC **Qué esperar:** `2238` y dos ventas.

# COMMAND ----------

total = ventas.count()                       # se calcula en los executors; llega 1 número
print("Ventas:", total)

filas = ventas.take(2)                       # llegan 2 filas al driver
for fila in filas:
    print(fila["id_venta"], fila["fecha_venta"], fila["canal"], fila["valor_neto"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 · Agregar en el clúster antes de traer datos al driver
# MAGIC **Regla de oro:** si vas a llevar datos a Python (por ejemplo, para graficar con pandas), **resume primero** en Spark y trae solo el resultado.
# MAGIC - Opción mala: `ventas.toPandas()` trae las 2.238 filas (en un caso real podrían ser millones y el driver se queda sin memoria).
# MAGIC - Opción buena: `groupBy(...).agg(...)` en Spark y luego `toPandas()` de 3 filas.
# MAGIC - `F.sum("valor_neto").alias("ventas_netas")`: suma la columna y le da nombre al resultado.
# MAGIC
# MAGIC **Qué esperar:** un DataFrame de pandas de 3 filas (un canal por fila; aquí sumamos todas las ventas, sin filtrar por estado).

# COMMAND ----------

resumen = (ventas.groupBy("canal")                                  # se agrupa en los executors
                 .agg(F.sum("valor_neto").alias("ventas_netas")))
pdf = resumen.toPandas()                                            # llegan solo 3 filas al driver
print(type(pdf))
pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Lazy evaluation, DAG, plan lógico y plan físico
# MAGIC En Spark hay dos tipos de operaciones:
# MAGIC - **Transformaciones** (`select`, `filter`, `withColumn`, `groupBy`, `join`…): **no** se ejecutan; Spark solo las **anota** en un plan.
# MAGIC - **Acciones** (`count`, `show`, `display`, `collect`, `write`…): **disparan** la ejecución de todo el plan.
# MAGIC
# MAGIC Esto es la **evaluación perezosa** (*lazy evaluation*): Spark espera a la acción para ver el plan completo y optimizarlo.
# MAGIC
# MAGIC ### 5.1 · Las transformaciones no se ejecutan
# MAGIC - `time.perf_counter()` devuelve un reloj de alta precisión; la resta da los segundos transcurridos.
# MAGIC - La cadena de transformaciones se define **casi al instante** (milisegundos): todavía no se ha leído ni un dato.
# MAGIC - `count()` es la **acción**: ahí Spark lee, filtra y cuenta.
# MAGIC
# MAGIC **Qué esperar:** la definición tarda ~0,000 s; la acción, alrededor de medio segundo, y devuelve 1076 ventas.

# COMMAND ----------

inicio = time.perf_counter()
completadas_tienda = (ventas
                      .filter(F.col("estado") == "Completada")          # transformación
                      .filter(F.col("canal") == "Tienda física")        # transformación
                      .withColumn("anio", F.year("fecha_venta")))       # transformación
print(f"Definir las transformaciones: {time.perf_counter() - inicio:.3f} s")

inicio = time.perf_counter()
n = completadas_tienda.count()                                          # ACCIÓN: ahora sí se ejecuta
print(f"Ejecutar la acción count(): {time.perf_counter() - inicio:.3f} s  →  {n} ventas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 · Con Spark Connect, hasta los errores esperan a la acción
# MAGIC En serverless (Spark Connect) el plan se **analiza** en el servidor cuando hay una acción. Por eso una columna que no existe
# MAGIC **no** da error al escribir la transformación, sino al ejecutarla.
# MAGIC - La primera línea define una selección de una columna inexistente: no pasa nada.
# MAGIC - `try / except`: al pedir `count()`, el servidor analiza el plan y responde con el error `UNRESOLVED_COLUMN`.
# MAGIC
# MAGIC **Qué esperar:** «Definida sin error» y luego el mensaje de error.

# COMMAND ----------

df_malo = ventas.select("columna_que_no_existe")     # transformación: no se valida todavía
print("Definida sin error")

try:
    df_malo.count()                                  # acción: aquí se analiza el plan
except Exception as e:
    print("Error al ejecutar:", str(e).splitlines()[0][:160])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 · Transformaciones estrechas y anchas
# MAGIC - **Estrecha** (*narrow*): cada partición se procesa sola, sin mover datos (`filter`, `select`, `withColumn`). No genera `Exchange`.
# MAGIC - **Ancha** (*wide*): necesita juntar datos de varias particiones (`groupBy`, `join`, `orderBy`, `distinct`, `repartition`). Genera un **shuffle** (`Exchange`) y una **nueva etapa** en el DAG.
# MAGIC
# MAGIC `df.explain()` sin parámetros imprime solo el **plan físico**. Busca la palabra `Exchange` (en Photon: `ShuffleExchange`) en cada uno.
# MAGIC Usamos `ventas_4` (los datos repartidos en 4 particiones, sección 3.2), como pasaría con una tabla grande: así se ve qué operaciones
# MAGIC necesitan mover datos. (Con una tabla tan pequeña que cabe en **una** partición, Spark no necesita shuffle para agrupar.)
# MAGIC
# MAGIC **Qué esperar:**
# MAGIC - Plan 1 (solo `filter` + `select`): solo el `Exchange` del `repartition(4)`; el filtro y la selección se hacen dentro de cada partición.
# MAGIC - Plan 2 (con `groupBy`): **un `Exchange` más**, con `hashpartitioning(canal, …)`: las filas de cada canal viajan a la misma partición.

# COMMAND ----------

print("=== 1. Solo transformaciones estrechas (filter, select) ===")
ventas_4.filter(F.col("cantidad") > 5).select("id_venta", "cantidad").explain()

print("\n=== 2. Con una transformación ancha (groupBy) ===")
ventas_4.groupBy("canal").count().explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 · Del código al plan: `explain(mode="extended")`
# MAGIC Cuando llega la acción, el optimizador **Catalyst** convierte tu código en un plan, por etapas:
# MAGIC 1. **Plan lógico sin resolver** (*parsed*): lo que escribiste, tal cual.
# MAGIC 2. **Plan lógico analizado** (*analyzed*): con nombres y tipos comprobados contra el catálogo.
# MAGIC 3. **Plan lógico optimizado** (*optimized*): reescrito con reglas (por ejemplo, filtrar **antes** de unir).
# MAGIC 4. **Plan físico** (*physical*): **cómo** se ejecuta: qué lectura, qué tipo de join, dónde hay *shuffle* (`Exchange`).
# MAGIC
# MAGIC `df.explain(mode="extended")` imprime los cuatro. La consulta de ejemplo responde: *«ventas completadas de Tecnología por ciudad del cliente»*.
# MAGIC - `.join(otro_df, "columna")`: une dos DataFrames por una columna común.
# MAGIC - Fíjate en el plan optimizado: los `Filter` quedan **pegados a la lectura** de cada tabla, aunque en el código el filtro de categoría está después del join.

# COMMAND ----------

consulta = (ventas
            .join(productos, "id_producto")                       # unir con productos
            .join(clientes, "id_cliente")                         # unir con clientes
            .filter(F.col("estado") == "Completada")
            .filter(F.col("categoria") == "Tecnología")           # filtro escrito DESPUÉS de los join
            .groupBy("ciudad")
            .agg(F.sum("valor_neto").alias("ventas_netas")))

consulta.explain(mode="extended")       # parsed, analyzed, optimized y physical

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 · El plan físico, en formato legible
# MAGIC `explain(mode="formatted")` muestra el plan físico como un **esquema numerado** y luego el detalle de cada paso. Busca:
# MAGIC - `Scan` / `PhotonScan`: lectura de cada tabla (con `PushedFilters` o `DataFilters` si el filtro se empujó a la lectura).
# MAGIC - `Join` (por ejemplo `BroadcastHashJoin`): la tabla pequeña se **envía completa** a cada executor para evitar mover la grande.
# MAGIC - `Exchange`: un **shuffle** (los datos se redistribuyen entre executors). Cada shuffle separa **etapas** (*stages*) del DAG.
# MAGIC - `AdaptiveSparkPlan`: la **Adaptive Query Execution** (AQE) puede ajustar el plan mientras corre.

# COMMAND ----------

consulta.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.6 · El DAG y las etapas en Databricks
# MAGIC El **DAG** (*Directed Acyclic Graph*, grafo dirigido acíclico) es el mapa de pasos que Spark arma para un **job**: cada acción
# MAGIC crea un job, cada shuffle lo parte en **etapas**, y cada etapa se ejecuta como muchas **tareas** (una por partición).
# MAGIC - En **serverless** no hay Spark UI: debajo de cada celda ejecutada aparece **«See performance»** → abre el **query profile**,
# MAGIC   que dibuja el plan como un grafo con el tiempo y las filas de cada paso.
# MAGIC - En **cómputo clásico**, la **Spark UI** muestra jobs, stages y el DAG de cada stage.
# MAGIC
# MAGIC Ejecuta la celda siguiente y abre su query profile: verás las lecturas, el join, el agregado y el shuffle.

# COMMAND ----------

display(consulta.orderBy(F.desc("ventas_netas")))      # display = acción → abre "See performance"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Lectura y escritura de datos con PySpark
# MAGIC Hasta ahora partimos de las **tablas** de la Clase 2. Para el laboratorio integrador partiremos de los **archivos crudos** del volumen:
# MAGIC en la Clase 2 los leíste con `read_files` (SQL); aquí, con `spark.read`.
# MAGIC
# MAGIC - **Leer:** `spark.read.format("csv" | "json" | "parquet").option(...).load(ruta)`, o los atajos `spark.read.csv(...)`, `.json(...)`, `.parquet(...)`.
# MAGIC - **Escribir:** `df.write.format(...).mode(...).save(ruta)` para **archivos**, o `df.write.saveAsTable("cat.schema.tabla")` para una **tabla** (Delta por defecto).
# MAGIC - **Modos de escritura** (`mode`): `"overwrite"` (reemplaza), `"append"` (agrega), `"error"` (falla si ya existe; es el valor por defecto), `"ignore"` (no hace nada si ya existe).
# MAGIC
# MAGIC Usamos los mismos 3 archivos del volumen de la Clase 2.
# MAGIC
# MAGIC ### 6.1 · CSV sin opciones: todo es texto
# MAGIC - `spark.read.option("header", True)`: la primera fila trae los nombres de columna.
# MAGIC - Sin `inferSchema`, Spark **no** adivina tipos: todas las columnas quedan como `string` (por defecto `inferSchema` es `false`).
# MAGIC
# MAGIC **Qué esperar:** las 7 columnas de `productos.csv` como `string`.

# COMMAND ----------

productos_csv_txt = (spark.read
                     .option("header", True)                     # 1.ª fila = nombres
                     .csv(f"{ARCHIVOS}/productos.csv"))
productos_csv_txt.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 · CSV con `inferSchema`
# MAGIC - `.option("inferSchema", True)`: Spark recorre el archivo **una vez más** para adivinar los tipos.
# MAGIC - Es cómodo para explorar, pero puede equivocarse y cuesta una pasada extra.
# MAGIC
# MAGIC **Qué esperar:** `id_producto`, `precio_lista` y `costo_unitario` como `integer`; `activo` sigue siendo `string`.

# COMMAND ----------

productos_csv_inf = (spark.read
                     .option("header", True)
                     .option("inferSchema", True)                # adivinar tipos (lectura extra)
                     .csv(f"{ARCHIVOS}/productos.csv"))
productos_csv_inf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 · CSV con esquema explícito (buena práctica)
# MAGIC - El esquema se puede escribir como **texto DDL**: `"nombre TIPO, nombre TIPO, ..."`, igual que en SQL.
# MAGIC - `.schema(esquema)`: Spark usa esos tipos y **no** necesita la pasada extra de `inferSchema`.
# MAGIC - En CSV el esquema se aplica **por posición**: el orden debe coincidir con el de las columnas del archivo.
# MAGIC - `.option("mode", "FAILFAST")`: si una fila no encaja con el esquema, la lectura **falla** (en lugar de dejar `NULL`, que es el modo por defecto `PERMISSIVE`).
# MAGIC
# MAGIC **Qué esperar:** tipos exactos (`decimal(12,2)` para el dinero) y 24 filas.

# COMMAND ----------

esquema_productos = """
    id_producto INT, nombre_producto STRING, categoria STRING, marca STRING,
    precio_lista DECIMAL(12,2), costo_unitario DECIMAL(12,2), activo STRING
"""
productos_csv = (spark.read
                 .option("header", True)
                 .option("mode", "FAILFAST")                     # error si una fila no encaja
                 .schema(esquema_productos)                      # tipos definidos por nosotros
                 .csv(f"{ARCHIVOS}/productos.csv"))
productos_csv.printSchema()
print(productos_csv.count(), "productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 · JSON: objetos anidados
# MAGIC - `spark.read.json(ruta)`: lee **JSON Lines** (un objeto por línea). Para un JSON «bonito» de varias líneas: `.option("multiLine", True)`.
# MAGIC - El objeto `pago` se convierte en una columna `struct`. `F.col("pago.metodo")` lee un campo del struct con un **punto**.
# MAGIC - `.show(3, truncate=False)`: acción que imprime 3 filas sin recortar el texto.
# MAGIC
# MAGIC **Qué esperar:** `fecha_venta` como `string`, `pago` como `struct<cuotas, metodo>` y 3 filas de muestra.

# COMMAND ----------

ventas_json = spark.read.json(f"{ARCHIVOS}/ventas_2025.json")
ventas_json.printSchema()

(ventas_json
 .select("id_venta", "fecha_venta", "pago", F.col("pago.metodo").alias("metodo_pago"))
 .show(3, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 · Parquet: el esquema viene en el archivo
# MAGIC - `spark.read.parquet(ruta)`: no hace falta `header`, `inferSchema` ni esquema: el archivo **guarda sus tipos**.
# MAGIC
# MAGIC **Qué esperar:** `fecha_ingreso` como `date` y `meta_mensual` como `decimal(14,2)`.

# COMMAND ----------

vendedores_parquet = spark.read.parquet(f"{ARCHIVOS}/vendedores.parquet")
vendedores_parquet.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 · Escribir archivos en el volumen
# MAGIC Escribimos las **ventas** (la tabla Delta) en tres formatos para compararlos.
# MAGIC - `ventas.write.mode("overwrite").parquet(ruta)`: escribe una **carpeta** con uno o varios archivos `part-….parquet` (uno por partición).
# MAGIC - `.option("header", True).csv(ruta)`: CSV con encabezado.
# MAGIC - `.json(ruta)`: JSON Lines.
# MAGIC - `.partitionBy("canal")`: crea **una subcarpeta por valor** (`canal=En línea/`, `canal=Tienda física/`…). Al leer filtrando por canal, Spark solo abre esa carpeta.
# MAGIC
# MAGIC **Qué esperar:** las carpetas quedan en el volumen `/Volumes/comercializadora/analisis_pyspark/salidas/`.

# COMMAND ----------

ventas.write.mode("overwrite").parquet(f"{SALIDAS}/ventas_parquet")                         # Parquet
ventas.write.mode("overwrite").option("header", True).csv(f"{SALIDAS}/ventas_csv")          # CSV con encabezado
ventas.write.mode("overwrite").json(f"{SALIDAS}/ventas_json")                               # JSON Lines
ventas.write.mode("overwrite").partitionBy("canal").parquet(f"{SALIDAS}/ventas_por_canal")  # una carpeta por canal
print("Archivos escritos en", SALIDAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.7 · ¿Qué se escribió y cuánto pesa?
# MAGIC Los volúmenes tienen rutas tipo **POSIX** (como las de un disco), así que se pueden recorrer con el módulo `os` de Python:
# MAGIC - `os.walk(ruta)`: recorre la carpeta **y todas sus subcarpetas**; en cada una devuelve la lista de archivos.
# MAGIC - `os.path.getsize(archivo)`: tamaño del archivo en bytes. La función `tamano` los suma.
# MAGIC - `sorted(os.listdir(ruta))`: lista (ordenada) lo que hay dentro de una carpeta.
# MAGIC
# MAGIC **Qué esperar:** JSON ≈ 0,75 MB · CSV ≈ 0,30 MB · Parquet ≈ 0,03 MB (comprimido y columnar). En `ventas_por_canal` hay una subcarpeta por canal y un archivo `_SUCCESS` (marca de que la escritura terminó bien).

# COMMAND ----------

import os

def tamano(ruta):
    """Suma el tamaño (en bytes) de todos los archivos de una carpeta y sus subcarpetas."""
    total = 0
    for carpeta, _, archivos in os.walk(ruta):              # recorre carpeta y subcarpetas
        for nombre in archivos:
            total += os.path.getsize(os.path.join(carpeta, nombre))
    return total                                            # bytes (abajo se dividen entre 1e6 para ver MB)

for carpeta in ["ventas_json", "ventas_csv", "ventas_parquet", "ventas_por_canal"]:
    print(f"{carpeta:18s} {tamano(f'{SALIDAS}/{carpeta}') / 1e6:6.2f} MB")

print("\nContenido de ventas_por_canal:", sorted(os.listdir(f"{SALIDAS}/ventas_por_canal")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.8 · Leer lo que escribimos
# MAGIC - Leemos la carpeta particionada: Spark **reconstruye** la columna `canal` a partir de los nombres de las subcarpetas.
# MAGIC - `.filter(F.col("canal") == "Teléfono")`: gracias a la partición, Spark solo lee la carpeta `canal=Teléfono` (*partition pruning*).
# MAGIC
# MAGIC **Qué esperar:** 2238 filas en total y 241 ventas por teléfono.

# COMMAND ----------

por_canal = spark.read.parquet(f"{SALIDAS}/ventas_por_canal")
print("Filas leídas:", por_canal.count())
print("Ventas por teléfono:", por_canal.filter(F.col("canal") == "Teléfono").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.9 · Escribir una tabla Delta con `saveAsTable`
# MAGIC - `df.write.mode("overwrite").saveAsTable("cat.schema.tabla")`: crea (o reemplaza) una **tabla gestionada** de Unity Catalog.
# MAGIC   El formato por defecto es **Delta**.
# MAGIC - Ejemplo: cargamos los **productos del CSV con esquema explícito** (6.3) y convertimos `activo` a booleano.
# MAGIC   - `.withColumn("activo", F.col("activo") == "Sí")`: reemplaza la columna por el resultado de la comparación (`True`/`False`).
# MAGIC - `spark.sql(...)` ejecuta SQL desde Python: aquí, el `ALTER TABLE … ALTER COLUMN … COMMENT` de la Clase 2 para documentar las columnas.
# MAGIC
# MAGIC **Qué esperar:** la tabla `analisis_pyspark.productos_pyspark` con 24 filas y sus descripciones.

# COMMAND ----------

destino = f"{CATALOGO}.{SALIDA}.productos_pyspark"

(productos_csv
 .withColumn("activo", F.col("activo") == "Sí")          # 'Sí'/'No' -> True/False
 .write.mode("overwrite")
 .saveAsTable(destino))                                   # tabla Delta gestionada

spark.sql(f"""
    ALTER TABLE {destino} ALTER COLUMN
      id_producto  COMMENT 'Identificador único del producto',
      precio_lista COMMENT 'Precio de venta sugerido, en COP',
      activo       COMMENT 'TRUE si el producto se sigue vendiendo'
""")
display(spark.table(destino).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Laboratorio integrador: de los archivos a una tabla oro
# MAGIC *«Gerencia quiere un reporte mensual de ventas por **ciudad del cliente** y **categoría**, construido directamente desde los
# MAGIC archivos de origen, guardado como tabla oro y con un gráfico de las categorías.»*
# MAGIC
# MAGIC Pasos (todo con PySpark):
# MAGIC 1. **Leer** los 3 archivos con esquema y limpiar tipos.
# MAGIC 2. **Unir** con la tabla `clientes` (Delta) y con productos.
# MAGIC 3. **Agregar** por mes, ciudad y categoría.
# MAGIC 4. **Escribir** la tabla oro con `saveAsTable` y documentarla.
# MAGIC 5. **Leer** la tabla y **graficar**.
# MAGIC
# MAGIC ### 7.1 · Leer y limpiar las ventas desde el JSON
# MAGIC - `F.to_date("fecha_venta")`: texto → fecha. `.cast("decimal(14,2)")`: cambia el tipo de una columna.
# MAGIC - `F.col("pago.metodo")`: aplanar el struct.
# MAGIC - Mismo cálculo de `valor_neto` que en la Clase 2.
# MAGIC
# MAGIC **Qué esperar:** 2238 ventas; luego 2001 completadas.

# COMMAND ----------

ventas_limpias = (spark.read.json(f"{ARCHIVOS}/ventas_2025.json")
    .select(
        F.col("id_venta").cast("int"),
        F.to_date("fecha_venta").alias("fecha_venta"),                    # texto -> fecha
        F.col("id_cliente").cast("int"),
        F.col("id_producto").cast("int"),
        F.col("cantidad").cast("int"),
        (F.col("cantidad") * F.col("precio_unitario") * (1 - F.coalesce("descuento_pct", F.lit(0))))
            .cast("decimal(14,2)").alias("valor_neto"),                     # columna calculada
        F.col("pago.metodo").alias("metodo_pago"),                          # aplanar el struct
        "canal", "estado"))
print("Ventas leídas:", ventas_limpias.count())
completadas = ventas_limpias.filter(F.col("estado") == "Completada")
print("Completadas  :", completadas.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 · Unir y agregar
# MAGIC - Productos desde el **CSV con esquema** (6.3) y clientes desde la **tabla Delta** de la Clase 2: PySpark mezcla fuentes sin problema.
# MAGIC - `F.countDistinct("id_cliente")`: cuántos clientes **distintos** compraron.
# MAGIC - `F.round(F.avg(...), 0)`: ticket promedio redondeado.
# MAGIC
# MAGIC **Qué esperar:** 238 filas (de un máximo de 12 meses × 5 ciudades × 4 categorías = 240: dos combinaciones no tuvieron ventas).

# COMMAND ----------

reporte = (completadas
           .join(productos_csv.select("id_producto", "categoria"), "id_producto")     # desde el CSV
           .join(clientes.select("id_cliente", "ciudad"), "id_cliente")               # desde la tabla Delta
           .groupBy(F.to_date(F.date_trunc("month", "fecha_venta")).alias("mes"),
                    "ciudad", "categoria")
           .agg(F.count("*").alias("ventas"),
                F.countDistinct("id_cliente").alias("clientes"),
                F.sum("valor_neto").alias("ventas_netas"),
                F.round(F.avg("valor_neto"), 0).alias("ticket_promedio")))
print(reporte.count(), "filas")
display(reporte.orderBy("mes", "ciudad", "categoria").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 · Guardar como tabla oro y documentarla
# MAGIC - `.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(...)`: crea o reemplaza la tabla Delta; `overwriteSchema`
# MAGIC   permite reemplazarla aunque cambie el esquema.
# MAGIC - Con `spark.sql` agregamos el comentario de la tabla y la descripción de cada columna (mismo patrón de la Clase 2).
# MAGIC
# MAGIC **Qué esperar:** sin errores; la tabla queda en `comercializadora.analisis_pyspark.ventas_ciudad_categoria_mes`.

# COMMAND ----------

tabla_oro = f"{CATALOGO}.{SALIDA}.ventas_ciudad_categoria_mes"

reporte.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(tabla_oro)

spark.sql(f"COMMENT ON TABLE {tabla_oro} IS 'Ventas completadas por mes, ciudad del cliente y categoría (capa oro, PySpark)'")
spark.sql(f"""
    ALTER TABLE {tabla_oro} ALTER COLUMN
      mes             COMMENT 'Primer día del mes',
      ciudad          COMMENT 'Ciudad del cliente',
      categoria       COMMENT 'Categoría del producto',
      ventas          COMMENT 'Número de ventas completadas',
      clientes        COMMENT 'Clientes distintos que compraron',
      ventas_netas    COMMENT 'Suma de valor_neto, en COP',
      ticket_promedio COMMENT 'Promedio de valor_neto por venta, en COP'
""")
print("Tabla oro lista:", tabla_oro)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 · Leer la tabla oro y graficar
# MAGIC - `spark.table(tabla_oro)`: leemos lo que guardamos (como haría un dashboard).
# MAGIC - `groupBy("categoria")` en Spark → `toPandas()` de 4 filas → `plot(kind="bar")`.
# MAGIC - `rot=0`: etiquetas horizontales.
# MAGIC
# MAGIC **Qué esperar:** Tecnología primero (595,7 M), luego Oficina, Hogar y Deportes.

# COMMAND ----------

oro = spark.table(tabla_oro)
pdf = (oro.groupBy("categoria")
          .agg(F.sum("ventas_netas").alias("ventas_netas"))
          .orderBy(F.desc("ventas_netas"))
          .toPandas())
pdf["millones"] = pdf["ventas_netas"].astype(float) / 1e6
pdf.plot(kind="bar", x="categoria", y="millones", legend=False, rot=0,
         title="Ventas netas 2025 por categoría (millones de COP) · desde la tabla oro")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.5 · Comprobar que coincide con la Clase 2
# MAGIC Una buena práctica es **validar** el resultado contra otra fuente. Comparamos el total de la tabla oro (construida desde los
# MAGIC archivos con PySpark) con el total de la tabla `ventas` (construida con SQL en la Clase 2).
# MAGIC - `.collect()[0][0]`: trae la única fila y su única columna al driver (seguro: es un solo número).
# MAGIC
# MAGIC **Qué esperar:** los dos totales iguales (1.270.326.100 COP).

# COMMAND ----------

total_oro    = oro.agg(F.sum("ventas_netas")).collect()[0][0]
total_clase2 = ventas.filter(F.col("estado") == "Completada").agg(F.sum("valor_neto")).collect()[0][0]
print("Tabla oro (PySpark):", total_oro)
print("Tabla ventas (SQL) :", total_clase2)
print("¿Coinciden?        :", total_oro == total_clase2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Retos y limpieza
# MAGIC **Retos** (resuélvelos con PySpark):
# MAGIC 1. Producto más vendido (en unidades) de cada categoría. *(pista: `Window.partitionBy("categoria")` + `F.row_number()`)*
# MAGIC 2. Ticket promedio por **método de pago** solo para ventas en línea; grafícalo con `pdf.plot(kind="bar", ...)`.
# MAGIC 3. Escribe `reporte` como **CSV** particionado por `ciudad` en `SALIDAS` y vuelve a leerlo filtrando una ciudad.
# MAGIC 4. Mira el `explain(mode="formatted")` de tu solución al reto 1: ¿cuántos `Exchange` (shuffles) tiene?
# MAGIC
# MAGIC **Limpieza (opcional):** borra lo creado **hoy** (las tablas de la Clase 2 se conservan). Quita el `#` de la línea y ejecuta.
# MAGIC - `DROP SCHEMA … CASCADE`: borra el schema `analisis_pyspark` con **todo** su contenido: las tablas y el volumen `salidas` con sus archivos.
# MAGIC   Como es un volumen **gestionado**, Unity Catalog borra también los archivos (con 7 días de retención). El volumen `archivos` de la Clase 2 no se toca.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOGO}.{SALIDA} CASCADE")   # tablas + volumen de salidas
print("Limpieza comentada: quita el # de la línea de arriba para borrar lo creado hoy")