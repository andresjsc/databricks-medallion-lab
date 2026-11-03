# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Laboratorio · Clase 2 — Procesamiento con Spark SQL
# MAGIC **Curso:** Ingeniería de Datos e IA con Databricks · **Caso:** sistema de ventas de la *Comercializadora Andina*
# MAGIC
# MAGIC La Comercializadora Andina vende productos de tecnología, hogar, oficina y deportes en cinco ciudades de Colombia.
# MAGIC En este notebook vas a construir su sistema de ventas en el lakehouse y a responder preguntas reales del negocio con SQL.
# MAGIC
# MAGIC | Sección | Qué haces | Dónde |
# MAGIC |---|---|---|
# MAGIC | 1 | Comprobar el motor Apache Spark | Notebook |
# MAGIC | 2 | Catálogo → schema → volumen (interfaz) y tabla con buenas prácticas (código) | Interfaz + notebook |
# MAGIC | 3 | Fundamentos de SQL sobre la tabla de práctica | Notebook |
# MAGIC | 4 | Tabla Delta `clientes` creada con código SQL | Notebook |
# MAGIC | 5 | Eliminar recursos: desde la interfaz y desde código | Interfaz + notebook |
# MAGIC | 6 | Leer los archivos CSV, JSON y Parquet que **subiste a mano** al volumen | Notebook |
# MAGIC | 7 | CTAS de `productos`, `vendedores` y `ventas` + `ALTER TABLE` con descripciones y restricciones | Notebook |
# MAGIC | 8 | CTE con las tablas del sistema | Notebook |
# MAGIC | 9 | Análisis por necesidades del negocio (tablas + gráficos con Python) | Notebook |
# MAGIC | 10 | Tablero final con Python | Notebook |
# MAGIC | 11 | Retos y limpieza | Notebook |
# MAGIC
# MAGIC **Antes de empezar**
# MAGIC 1. Descarga los 3 archivos de la carpeta `code/datos` del curso: `productos.csv`, `ventas_2025.json`, `vendedores.parquet`.
# MAGIC 2. Pulsa **Connect** (arriba a la derecha) y elige el cómputo **Serverless**.
# MAGIC 3. Ejecuta cada celda con **Shift + Enter**, en orden. Lee siempre la explicación que está **antes** de cada celda.
# MAGIC
# MAGIC **Cómo leer este notebook**
# MAGIC - Las celdas que empiezan por `%sql` son **SQL**. Las demás son **Python** (las usamos para los gráficos y para ver errores sin detener el notebook).
# MAGIC - En SQL, todo lo que va después de `--` es un **comentario**: Spark no lo ejecuta, es una nota para ti. En Python, el comentario empieza por `#`.
# MAGIC - Los nombres de tablas y columnas no llevan tildes ni ñ: así no hay que escribirlos entre comillas invertidas (`` `…` ``).
# MAGIC
# MAGIC Fuentes oficiales: [Unity Catalog](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/) ·
# MAGIC [Delta Lake](https://learn.microsoft.com/azure/databricks/delta/) ·
# MAGIC [read_files](https://learn.microsoft.com/azure/databricks/sql/language-manual/functions/read_files) ·
# MAGIC [CREATE TABLE (CTAS)](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using) ·
# MAGIC [ALTER COLUMN](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-column) ·
# MAGIC [CTE](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-cte) ·
# MAGIC [Window functions](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-window-functions) ·
# MAGIC [_sqldf y notebooks](https://learn.microsoft.com/azure/databricks/notebooks/notebooks-code) ·
# MAGIC [pandas DataFrame.plot](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.plot.html). Probado en Databricks Free Edition (serverless), noviembre 2026.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Apache Spark: el motor que ejecuta todo
# MAGIC Cada celda de este notebook la ejecuta **Apache Spark** en cómputo **serverless**: Databricks enciende las máquinas
# MAGIC por ti y Spark reparte el trabajo en **tareas** que corren en paralelo.
# MAGIC
# MAGIC ### 1.1 · ¿Qué versión de Spark estoy usando?
# MAGIC - `spark` es una variable que Databricks crea automáticamente en cada notebook: es la **sesión de Spark**, la puerta de entrada al motor.
# MAGIC - `spark.version` devuelve la versión del motor como texto.
# MAGIC - `print(…)` muestra en pantalla lo que le pases, separado por comas.
# MAGIC
# MAGIC **Qué esperar:** una línea como `Versión de Apache Spark: 4.2.0`.

# COMMAND ----------

# spark = sesión de Spark que Databricks crea por ti
# spark.version = texto con la versión del motor
print("Versión de Apache Spark:", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 · Una consulta que Spark reparte en paralelo
# MAGIC - `range(10000000)` es una **función de tabla**: genera una tabla con una columna `id` y los números del 0 al 9.999.999.
# MAGIC - `count(*)` cuenta las filas y `sum(id)` suma todos los valores. `AS filas` y `AS suma` dan nombre a las columnas del resultado.
# MAGIC - No tienes que decirle a Spark cómo repartir el trabajo: el optimizador (Catalyst) lo decide.
# MAGIC
# MAGIC **Qué esperar:** `filas = 10000000` y `suma = 49999995000000`, en un par de segundos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS filas,     -- cuántas filas hay
# MAGIC        sum(id)  AS suma       -- suma de la columna id
# MAGIC FROM range(10000000);         -- tabla generada: id de 0 a 9.999.999

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Catálogo, schema, volumen y tabla
# MAGIC Unity Catalog organiza todo en tres niveles: **catálogo → schema → objeto** (tabla, vista, volumen…).
# MAGIC El nombre completo de cualquier objeto es `catálogo.schema.objeto`. En Free Edition ya existe el catálogo **`workspace`**;
# MAGIC en esta sección lo usamos tal cual.
# MAGIC
# MAGIC **Paso 2.1 · Crear el schema (interfaz)** — barra lateral **Catalog** → catálogo **`workspace`** → **Create schema** → nombre `practica_ui` → **Create**.
# MAGIC
# MAGIC **Paso 2.2 · Crear el volumen (interfaz)** — selecciona el schema `practica_ui` → **Create › Volume** → nombre `archivos_ui` → tipo **Managed** → **Create**.
# MAGIC Un **volumen** es una carpeta gobernada por Unity Catalog para guardar **archivos** (no tablas).
# MAGIC
# MAGIC **Paso 2.3 · Subir un archivo (interfaz)** — abre el volumen `archivos_ui` → **Upload to this volume** → arrastra `productos.csv` → **Upload**.
# MAGIC
# MAGIC > ¿Prefieres crear tu propio catálogo? En **Catalog → Catalogs → Create catalog** (tipo *Standard*). Si lo haces, cambia `workspace` por su nombre en las secciones 2, 3 y 5.
# MAGIC
# MAGIC **Paso 2.4 · Crear la tabla con código.** La interfaz solo crea tablas **a partir de un archivo** y deja que Databricks
# MAGIC adivine los tipos. La buena práctica es **definir la tabla primero** (nombres, tipos, descripciones y restricciones)
# MAGIC y **después cargarla** con los datos del archivo que subiste en el paso 2.3.
# MAGIC
# MAGIC ### 2.4a · Definir la tabla
# MAGIC Partes de la sentencia:
# MAGIC - `CREATE OR REPLACE TABLE <nombre> (…)`: crea la tabla; si ya existe, la reemplaza (así puedes volver a ejecutar la celda sin error).
# MAGIC - Dentro del paréntesis va **una columna por línea**: `nombre  TIPO  [NOT NULL]  COMMENT 'descripción'`.
# MAGIC   - Tipos: `INT` (entero), `STRING` (texto), `DECIMAL(12,2)` (número exacto con 2 decimales, ideal para dinero), `BOOLEAN` (verdadero/falso).
# MAGIC   - `NOT NULL`: la columna **no** puede quedar vacía. Delta rechaza la fila si llega sin valor.
# MAGIC   - `COMMENT '…'`: la descripción de la columna. Se ve en Catalog Explorer.
# MAGIC - `CONSTRAINT pk_productos_ui PRIMARY KEY (id_producto)`: declara la **clave primaria** (el identificador único de cada fila). Es informativa: documenta, no valida.
# MAGIC - `CLUSTER BY AUTO`: Databricks organiza los datos automáticamente según cómo los consultes (*liquid clustering*).
# MAGIC - `COMMENT '…'` (después del paréntesis): la descripción de la **tabla**.
# MAGIC - `TBLPROPERTIES (…)`: metadatos propios en formato `'clave' = 'valor'` (capa, origen…).
# MAGIC
# MAGIC **Qué esperar:** la celda no devuelve filas; la tabla queda creada y vacía.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.practica_ui.productos_ui (
# MAGIC   -- columna        tipo          ¿obligatoria?  descripción
# MAGIC   id_producto     INT           NOT NULL COMMENT 'Identificador único del producto',
# MAGIC   nombre_producto STRING        NOT NULL COMMENT 'Nombre comercial del producto',
# MAGIC   categoria       STRING                 COMMENT 'Tecnología, Hogar, Oficina o Deportes',
# MAGIC   marca           STRING                 COMMENT 'Marca del producto',
# MAGIC   precio_lista    DECIMAL(12,2)          COMMENT 'Precio de venta sugerido, en COP',
# MAGIC   costo_unitario  DECIMAL(12,2)          COMMENT 'Costo de compra por unidad, en COP',
# MAGIC   activo          BOOLEAN                COMMENT 'TRUE si el producto se sigue vendiendo',
# MAGIC   CONSTRAINT pk_productos_ui PRIMARY KEY (id_producto)          -- clave primaria
# MAGIC )
# MAGIC CLUSTER BY AUTO                                                 -- organización automática de los datos
# MAGIC COMMENT 'Productos de práctica (sección 2). Origen: productos.csv' -- descripción de la tabla
# MAGIC TBLPROPERTIES ('capa' = 'practica', 'origen' = 'productos.csv');  -- metadatos propios

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4b · Cargar la tabla con el archivo del volumen
# MAGIC - `INSERT OVERWRITE <tabla>`: **reemplaza** el contenido de la tabla con el resultado del `SELECT` (si la ejecutas dos veces, no duplica filas).
# MAGIC - `BY NAME`: cada columna del `SELECT` se guarda en la columna de la tabla **que se llama igual** (no importa el orden).
# MAGIC - `read_files('<ruta>', format => 'csv', header => true)`: lee el archivo del volumen.
# MAGIC   - La ruta de un volumen siempre es `/Volumes/<catálogo>/<schema>/<volumen>/<archivo>`.
# MAGIC   - `header => true`: la primera fila del CSV trae los nombres de las columnas.
# MAGIC - `CAST(x AS tipo)`: convierte el valor al tipo de la tabla. El CSV no guarda tipos, por eso convertimos.
# MAGIC - `activo = 'Sí'`: es una **comparación**; devuelve `TRUE` si el texto es `'Sí'` y `FALSE` si no. Así el texto pasa a `BOOLEAN`.
# MAGIC
# MAGIC **Qué esperar:** `num_inserted_rows = 24`.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE workspace.practica_ui.productos_ui BY NAME   -- reemplaza el contenido, emparejando por nombre
# MAGIC SELECT
# MAGIC   CAST(id_producto AS INT)              AS id_producto,       -- texto/entero -> INT
# MAGIC   nombre_producto,                                            -- se copia tal cual
# MAGIC   categoria,
# MAGIC   marca,
# MAGIC   CAST(precio_lista   AS DECIMAL(12,2)) AS precio_lista,      -- -> número exacto con 2 decimales
# MAGIC   CAST(costo_unitario AS DECIMAL(12,2)) AS costo_unitario,
# MAGIC   activo = 'Sí'                         AS activo             -- 'Sí'/'No' -> TRUE/FALSE
# MAGIC FROM read_files('/Volumes/workspace/practica_ui/archivos_ui/productos.csv',
# MAGIC                 format => 'csv', header => true);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 · Comprobar que todo existe
# MAGIC Comandos `SHOW` y `LIST` para **ver** lo que hay en Unity Catalog (no modifican nada):
# MAGIC - `SHOW SCHEMAS IN workspace`: los schemas del catálogo `workspace`.
# MAGIC - `SHOW TABLES IN workspace.practica_ui`: las tablas y vistas del schema.
# MAGIC - `SHOW VOLUMES IN workspace.practica_ui`: los volúmenes del schema.
# MAGIC - `LIST '<ruta>'`: los archivos de una carpeta del volumen, con su tamaño.
# MAGIC
# MAGIC Después abre **Catalog Explorer → workspace → practica_ui → productos_ui**: verás la descripción de cada columna.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;      -- debe aparecer practica_ui

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.practica_ui;     -- debe aparecer productos_ui

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.practica_ui;    -- debe aparecer archivos_ui

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/workspace/practica_ui/archivos_ui/';   -- debe aparecer productos.csv (1458 bytes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Fundamentos de SQL
# MAGIC Una consulta responde una pregunta. Se escribe en este orden:
# MAGIC
# MAGIC | Cláusula | Pregunta que responde | ¿Obligatoria? |
# MAGIC |---|---|---|
# MAGIC | `SELECT` | ¿Qué columnas quiero ver? | Sí |
# MAGIC | `FROM` | ¿De qué tabla salen los datos? | Casi siempre |
# MAGIC | `WHERE` | ¿Qué filas me interesan? | No |
# MAGIC | `GROUP BY` | ¿Cómo agrupo las filas? | No |
# MAGIC | `HAVING` | ¿Qué grupos me interesan? | No |
# MAGIC | `ORDER BY` | ¿En qué orden las muestro? | No |
# MAGIC | `LIMIT` | ¿Cuántas filas como máximo? | No |
# MAGIC
# MAGIC Spark las **evalúa** en otro orden: `FROM` → `WHERE` → `GROUP BY` → `HAVING` → `SELECT` → `ORDER BY` → `LIMIT`.
# MAGIC Por eso un alias creado en el `SELECT` todavía no existe en el `WHERE`, pero sí en el `ORDER BY`.
# MAGIC
# MAGIC Trabajamos sobre la tabla `workspace.practica_ui.productos_ui` del paso 2.4.
# MAGIC
# MAGIC ### 3.1 · Necesidad: «quiero ver todo el catálogo de productos»
# MAGIC - `SELECT *`: el asterisco significa **todas las columnas**.
# MAGIC - `FROM <tabla>`: de dónde salen los datos (nombre completo de tres niveles).
# MAGIC
# MAGIC **Qué esperar:** las 24 filas con sus 7 columnas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *                                  -- todas las columnas
# MAGIC FROM workspace.practica_ui.productos_ui;  -- de esta tabla

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · Necesidad: «solo necesito el nombre y el precio»
# MAGIC - En lugar de `*`, se escriben las columnas que quieres, **separadas por comas**.
# MAGIC - `AS producto`: un **alias**, es decir, un nombre nuevo para la columna en el resultado. La tabla no cambia.
# MAGIC
# MAGIC **Qué esperar:** 24 filas, 2 columnas: `producto` y `precio`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_producto AS producto,   -- columna 1, renombrada
# MAGIC        precio_lista    AS precio      -- columna 2, renombrada
# MAGIC FROM workspace.practica_ui.productos_ui;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 · Necesidad: «¿qué productos de Tecnología cuestan menos de 300.000?»
# MAGIC - `WHERE <condición>`: deja solo las filas que cumplen la condición.
# MAGIC - `=` compara igualdad; el texto va entre comillas simples (`'Tecnología'`). `<` significa «menor que».
# MAGIC - `AND`: **las dos** condiciones deben cumplirse. (`OR` sería «al menos una»).
# MAGIC
# MAGIC **Qué esperar:** 4 productos: Mouse inalámbrico, Teclado mecánico, Audífonos Bluetooth y Cámara web HD.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_producto, precio_lista
# MAGIC FROM workspace.practica_ui.productos_ui
# MAGIC WHERE categoria = 'Tecnología'      -- condición 1: la categoría es Tecnología
# MAGIC   AND precio_lista < 300000;        -- condición 2: cuesta menos de 300.000

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 · Necesidad: «los 5 productos más caros»
# MAGIC - `ORDER BY precio_lista DESC`: ordena por precio de **mayor a menor** (`DESC`). Sin `DESC` sería de menor a mayor (`ASC`).
# MAGIC - `LIMIT 5`: devuelve como máximo 5 filas. Junto con `ORDER BY` da un «top 5».
# MAGIC
# MAGIC **Qué esperar:** Portátil, Bicicleta estática, Silla ergonómica, Monitor y Escritorio en L.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_producto, categoria, precio_lista
# MAGIC FROM workspace.practica_ui.productos_ui
# MAGIC ORDER BY precio_lista DESC     -- del más caro al más barato
# MAGIC LIMIT 5;                       -- solo los 5 primeros

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 · Necesidad: «¿qué categorías vendemos?»
# MAGIC - `DISTINCT`: elimina las filas repetidas. Cada categoría aparece **una sola vez**.
# MAGIC
# MAGIC **Qué esperar:** 4 filas: Deportes, Hogar, Oficina, Tecnología (en orden alfabético por el `ORDER BY`).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT categoria      -- cada categoría una sola vez
# MAGIC FROM workspace.practica_ui.productos_ui
# MAGIC ORDER BY categoria;            -- orden alfabético

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 · Necesidad: «¿cuánto ganamos por unidad en cada producto?»
# MAGIC - En el `SELECT` puedes **calcular**: `precio_lista - costo_unitario` resta dos columnas en cada fila.
# MAGIC - `margen_pct` = ganancia ÷ precio × 100. `round(x, 1)` redondea a 1 decimal.
# MAGIC - `ORDER BY margen_pct`: en el `ORDER BY` **sí** se puede usar el alias creado en el `SELECT`.
# MAGIC
# MAGIC **Qué esperar:** 24 filas, de mayor a menor margen (la Banda elástica primero, el Portátil al final).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_producto,
# MAGIC        precio_lista - costo_unitario                                  AS ganancia_unidad, -- resta por fila
# MAGIC        round((precio_lista - costo_unitario) / precio_lista * 100, 1) AS margen_pct       -- porcentaje
# MAGIC FROM workspace.practica_ui.productos_ui
# MAGIC ORDER BY margen_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 · Necesidad: «¿cuántos productos y qué precio promedio tiene cada categoría?»
# MAGIC - `GROUP BY categoria`: junta las filas que tienen la misma categoría; el resultado tiene **una fila por grupo**.
# MAGIC - **Funciones de agregado**: resumen muchas filas en un valor por grupo.
# MAGIC   `count(*)` cuenta, `avg(x)` promedia, `max(x)` toma el mayor (también existen `sum` y `min`).
# MAGIC - Regla: toda columna del `SELECT` debe estar en el `GROUP BY` **o** dentro de un agregado.
# MAGIC
# MAGIC **Qué esperar:** 4 filas. Tecnología tiene 8 productos y el precio promedio más alto.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT categoria,                                 -- columna de agrupación
# MAGIC        count(*)                 AS productos,     -- cuántos productos hay en el grupo
# MAGIC        round(avg(precio_lista)) AS precio_promedio,
# MAGIC        max(precio_lista)        AS precio_maximo
# MAGIC FROM workspace.practica_ui.productos_ui
# MAGIC GROUP BY categoria                                -- una fila por categoría
# MAGIC ORDER BY productos DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.8 · Necesidad: «solo las categorías con más de 5 productos»
# MAGIC - `HAVING`: filtra **grupos**, después de agrupar. Puede usar agregados como `count(*)`.
# MAGIC - Diferencia clave: `WHERE` filtra **filas** antes de agrupar; `HAVING` filtra **grupos** después de agrupar.
# MAGIC
# MAGIC **Qué esperar:** Tecnología (8) y Deportes (6).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT categoria, count(*) AS productos
# MAGIC FROM workspace.practica_ui.productos_ui
# MAGIC GROUP BY categoria
# MAGIC HAVING count(*) > 5;            -- solo los grupos con más de 5 filas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Tablas Delta: catálogo, schema y tabla con código SQL
# MAGIC Ahora creamos el sistema real **con código**: el catálogo `comercializadora`, el schema `sistema_ventas` y la tabla Delta `clientes`.
# MAGIC **Delta Lake** es el formato de todas las tablas en Databricks: guarda los datos en archivos Parquet y agrega un **registro de
# MAGIC transacciones** que permite cambios seguros (ACID), historial, *time travel* y `UPDATE`/`DELETE`/`MERGE`.
# MAGIC
# MAGIC > Si tu workspace no te permite crear catálogos (por ejemplo, en una empresa sin el permiso `CREATE CATALOG`), usa
# MAGIC > `workspace` en lugar de `comercializadora` en todo el notebook (Ctrl/Cmd + Shift + F para reemplazar).
# MAGIC
# MAGIC ### 4.1 · Crear el catálogo y el schema
# MAGIC - `CREATE CATALOG IF NOT EXISTS <nombre>`: crea el catálogo solo si no existe (si ya existe, no da error).
# MAGIC - `CREATE SCHEMA IF NOT EXISTS <catálogo>.<schema>`: crea el schema dentro del catálogo.
# MAGIC - `COMMENT '…'`: descripción del catálogo o del schema.
# MAGIC - Una celda `%sql` puede tener **varias sentencias**; cada una termina con `;`.
# MAGIC
# MAGIC **Qué esperar:** sin filas. Refresca **Catalog** en la barra lateral y verás el catálogo nuevo.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS comercializadora                  -- nivel 1: catálogo
# MAGIC   COMMENT 'Sistema de ventas de la Comercializadora Andina';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS comercializadora.sistema_ventas     -- nivel 2: schema dentro del catálogo
# MAGIC   COMMENT 'Tablas del sistema de ventas: clientes, productos, vendedores y ventas';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 · Fijar el catálogo y el schema de trabajo
# MAGIC - `USE CATALOG` y `USE SCHEMA` fijan dónde trabajas. Desde aquí basta con el **nombre corto** (`clientes` en vez de
# MAGIC   `comercializadora.sistema_ventas.clientes`).
# MAGIC - `current_catalog()` y `current_schema()` devuelven dónde estás.
# MAGIC
# MAGIC **Qué esperar:** `catalogo = comercializadora`, `esquema = sistema_ventas`.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG comercializadora;          -- catálogo por defecto
# MAGIC USE SCHEMA sistema_ventas;             -- schema por defecto
# MAGIC SELECT current_catalog() AS catalogo,  -- comprobación
# MAGIC        current_schema()  AS esquema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 · Crear la tabla Delta `clientes`
# MAGIC - `DROP TABLE IF EXISTS clientes`: borra la tabla si existía. Lo hacemos para que la demostración de historial (4.9)
# MAGIC   empiece siempre en la **versión 0**.
# MAGIC - `CREATE TABLE clientes (…)`: misma estructura que en el paso 2.4a: columna, tipo, `NOT NULL`, `COMMENT`, clave primaria,
# MAGIC   `CLUSTER BY AUTO`, comentario de tabla y `TBLPROPERTIES`.
# MAGIC - `DATE`: tipo fecha (año-mes-día).
# MAGIC
# MAGIC **Qué esperar:** sin filas; la tabla queda vacía.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS clientes;   -- empezar desde cero (versión 0 del historial)
# MAGIC
# MAGIC CREATE TABLE clientes (
# MAGIC   id_cliente      INT    NOT NULL COMMENT 'Identificador único del cliente',
# MAGIC   nombre_completo STRING NOT NULL COMMENT 'Nombre de la persona o razón social',
# MAGIC   email           STRING          COMMENT 'Correo de contacto',
# MAGIC   ciudad          STRING          COMMENT 'Ciudad principal del cliente',
# MAGIC   segmento        STRING          COMMENT 'Persona o Empresa',
# MAGIC   fecha_registro  DATE            COMMENT 'Fecha de alta en el CRM',
# MAGIC   CONSTRAINT pk_clientes PRIMARY KEY (id_cliente)
# MAGIC )archivos_ui
# MAGIC COMMENT 'Clientes de la Comercializadora Andina (datos de demostración)'
# MAGIC TBLPROPERTIES ('capa' = 'plata', 'origen' = 'CRM', 'responsable' = 'equipo-datos');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 · Llenar la tabla con datos de demostración
# MAGIC - `INSERT INTO <tabla> VALUES (…), (…)`: agrega filas. Cada paréntesis es **una fila**, con los valores **en el mismo orden**
# MAGIC   que las columnas de la tabla.
# MAGIC - `DATE'2023-02-14'`: un valor de tipo fecha escrito como texto con formato año-mes-día.
# MAGIC - La segunda sentencia (`SELECT … ORDER BY id_cliente`) muestra el resultado.
# MAGIC
# MAGIC **Qué esperar:** 15 clientes: 11 personas y 4 empresas en 5 ciudades.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO clientes VALUES
# MAGIC   -- (id, nombre, email, ciudad, segmento, fecha de registro)
# MAGIC   (1,  'Laura Gómez Restrepo',          'laura.gomez@correo.com',            'Bogotá',       'Persona', DATE'2023-02-14'),
# MAGIC   (2,  'Andrés Martínez Ruiz',          'andres.martinez@correo.com',        'Medellín',     'Persona', DATE'2023-03-02'),
# MAGIC   (3,  'Distribuciones El Faro S.A.S.', 'compras@elfaro.com.co',             'Barranquilla', 'Empresa', DATE'2023-04-18'),
# MAGIC   (4,  'Camila Torres Vargas',          'camila.torres@correo.com',          'Cali',         'Persona', DATE'2023-06-25'),
# MAGIC   (5,  'Jorge Ramírez Ospina',          'jorge.ramirez@correo.com',          'Bogotá',       'Persona', DATE'2023-08-09'),
# MAGIC   (6,  'Valentina Castro Mejía',        'valentina.castro@correo.com',       'Bucaramanga',  'Persona', DATE'2023-09-30'),
# MAGIC   (7,  'Soluciones Andinas Ltda.',      'gerencia@solucionesandinas.com.co', 'Bogotá',       'Empresa', DATE'2023-11-12'),
# MAGIC   (8,  'Sebastián López Herrera',       'sebastian.lopez@correo.com',        'Medellín',     'Persona', DATE'2024-01-20'),
# MAGIC   (9,  'Daniela Moreno Silva',          'daniela.moreno@correo.com',         'Cali',         'Persona', DATE'2024-02-28'),
# MAGIC   (10, 'Colegio San Martín',            'compras@colegiosanmartin.edu.co',   'Cali',         'Empresa', DATE'2024-04-05'),
# MAGIC   (11, 'Felipe Rojas Cárdenas',         'felipe.rojas@correo.com',           'Barranquilla', 'Persona', DATE'2024-05-17'),
# MAGIC   (12, 'Mariana Pérez Quintero',        'mariana.perez@correo.com',          'Bogotá',       'Persona', DATE'2024-07-03'),
# MAGIC   (13, 'Innovar Antioquia S.A.S.',      'compras@innovarantioquia.com.co',   'Medellín',     'Empresa', DATE'2024-08-22'),
# MAGIC   (14, 'Santiago Díaz Londoño',         'santiago.diaz@correo.com',          'Bucaramanga',  'Persona', DATE'2024-10-10'),
# MAGIC   (15, 'Isabella Hernández Ríos',       'isabella.hernandez@correo.com',     'Cali',         'Persona', DATE'2024-12-01');
# MAGIC
# MAGIC SELECT * FROM clientes ORDER BY id_cliente;   -- ver lo que quedó guardado

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 · ¿Qué hay por dentro de una tabla Delta?
# MAGIC `DESCRIBE DETAIL <tabla>` devuelve los metadatos de la tabla. Columnas para fijarte:
# MAGIC - `format`: `delta`. · `location`: dónde guarda Unity Catalog los archivos (tabla **gestionada**).
# MAGIC - `numFiles` y `sizeInBytes`: cuántos archivos Parquet tiene y cuánto pesan.
# MAGIC - `clusterByAuto`: `true` porque usamos `CLUSTER BY AUTO`. · `properties`: incluye nuestras `TBLPROPERTIES`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL clientes;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 · Delta protege la calidad de los datos
# MAGIC Delta valida las restricciones y los tipos **antes de escribir**. Si una fila no cumple, la operación completa se cancela y la tabla no cambia.
# MAGIC
# MAGIC Esta celda es **Python** para poder mostrar el error sin detener el notebook:
# MAGIC - `spark.sql("…")` ejecuta una sentencia SQL desde Python.
# MAGIC - `try:` intenta ejecutar el bloque; si ocurre un error, Python salta a `except Exception as e:` y guarda el error en `e`.
# MAGIC - `str(e).splitlines()[0]` toma solo la primera línea del mensaje de error, y `[:220]` sus primeros 220 caracteres.
# MAGIC
# MAGIC **Qué esperar:** dos mensajes de rechazo: `DELTA_NOT_NULL_CONSTRAINT_VIOLATED` (falta el id) y `CAST_INVALID_INPUT`
# MAGIC (`'no-es-fecha'` no es una fecha válida; el **modo ANSI** de Spark da error en lugar de guardar `NULL`).

# COMMAND ----------

# Intento 1: un cliente SIN id_cliente (la columna es NOT NULL)
try:
    spark.sql("INSERT INTO clientes VALUES (NULL, 'Sin identificador', NULL, 'Cali', 'Persona', current_date())")
except Exception as e:                       # si falla, guardamos el error en e
    print("Rechazado por la restricción NOT NULL:\n", str(e).splitlines()[0][:220])

# Intento 2: una fecha que no es fecha (la columna es de tipo DATE)
try:
    spark.sql("INSERT INTO clientes (id_cliente, nombre_completo, fecha_registro) VALUES (99, 'Fecha inválida', 'no-es-fecha')")
except Exception as e:
    print("\nRechazado por el tipo de dato (schema enforcement):\n", str(e).splitlines()[0][:220])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 · `UPDATE`: modificar filas existentes
# MAGIC Necesidad: «Laura cambió su correo».
# MAGIC - `UPDATE <tabla> SET columna = nuevo_valor WHERE <condición>`: cambia el valor **solo** en las filas que cumplen el `WHERE`.
# MAGIC - ⚠️ Sin `WHERE`, el `UPDATE` cambiaría **todas** las filas.
# MAGIC - Es una **transacción ACID**: se aplica completa o no se aplica.
# MAGIC
# MAGIC **Qué esperar:** `num_affected_rows = 1`.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE clientes
# MAGIC SET email = 'laura.gomez.r@correo.com'   -- nuevo valor
# MAGIC WHERE id_cliente = 1;                    -- solo la fila de Laura

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.8 · `DELETE`: un error humano a propósito
# MAGIC Simulamos que alguien borra a **todas las empresas** por equivocación.
# MAGIC - `DELETE FROM <tabla> WHERE <condición>`: elimina las filas que cumplen la condición.
# MAGIC - Luego contamos cuántos clientes quedan.
# MAGIC
# MAGIC **Qué esperar:** `clientes_que_quedan = 11` (antes eran 15). En 4.9–4.10 lo vamos a deshacer.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM clientes WHERE segmento = 'Empresa';    -- borra las 4 empresas
# MAGIC
# MAGIC SELECT count(*) AS clientes_que_quedan FROM clientes;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.9 · El historial: cada operación es una versión
# MAGIC `DESCRIBE HISTORY <tabla>` lista todas las operaciones sobre la tabla, de la más reciente a la más antigua.
# MAGIC Columnas clave: `version` (número de versión), `timestamp` (cuándo), `userName` (quién), `operation` (qué) y
# MAGIC `operationMetrics` (cuántas filas cambió).
# MAGIC
# MAGIC **Qué esperar:** versión 0 = `CREATE TABLE`, 1 = `WRITE` (el INSERT), 2 = `UPDATE`, 3 = `DELETE`.
# MAGIC Databricks puede agregar operaciones automáticas de mantenimiento (por ejemplo `OPTIMIZE`): si aparecen, los números se corren.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY clientes;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.10 · *Time travel* y `RESTORE`
# MAGIC - `FROM clientes VERSION AS OF 2`: consulta la tabla **como estaba en la versión 2** (antes del `DELETE`), sin modificarla.
# MAGIC - `RESTORE TABLE clientes TO VERSION AS OF 2`: **devuelve** la tabla a esa versión. El `RESTORE` también queda en el historial.
# MAGIC - Si en 4.9 viste un `OPTIMIZE` automático antes del `DELETE`, usa la versión inmediatamente anterior al `DELETE`.
# MAGIC
# MAGIC **Qué esperar:** primero `Persona 11 · Empresa 4`; después del `RESTORE`, `clientes_recuperados = 15`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT segmento, count(*) AS clientes
# MAGIC FROM clientes VERSION AS OF 2        -- la tabla tal como estaba en la versión 2
# MAGIC GROUP BY segmento;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE clientes TO VERSION AS OF 2;            -- deshacer el DELETE
# MAGIC
# MAGIC SELECT count(*) AS clientes_recuperados FROM clientes;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.11 · `MERGE`: actualizar e insertar en una sola operación (*upsert*)
# MAGIC Necesidad: «el CRM envía un cliente con el correo actualizado y dos clientes nuevos».
# MAGIC - `MERGE INTO destino USING origen ON <condición>`: compara cada fila del **origen** con el **destino** usando la condición (el id).
# MAGIC - `WHEN MATCHED THEN UPDATE SET *`: si el id ya existe, **actualiza** todas sus columnas con los valores del origen.
# MAGIC - `WHEN NOT MATCHED THEN INSERT *`: si el id no existe, **inserta** la fila nueva.
# MAGIC - `SELECT * FROM VALUES (…) AS crm(columnas…)`: crea una tabla temporal en línea con 3 filas y nombra sus columnas.
# MAGIC
# MAGIC **Qué esperar:** `num_updated_rows = 1` (cliente 6) y `num_inserted_rows = 2` (clientes 16 y 17). Total: 17 clientes.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes AS destino                          -- tabla que se actualiza
# MAGIC USING (
# MAGIC   SELECT * FROM VALUES                                  -- datos que llegan del CRM
# MAGIC     (6,  'Valentina Castro Mejía', 'valecastro@correo.com',             'Bucaramanga', 'Persona', DATE'2023-09-30'),
# MAGIC     (16, 'Ferretería La Esquina',  'ventas@ferreterialaesquina.com.co', 'Medellín',    'Empresa', DATE'2026-10-15'),
# MAGIC     (17, 'Natalia Suárez Pineda',  'natalia.suarez@correo.com',         'Bogotá',      'Persona', DATE'2026-10-20')
# MAGIC   AS crm(id_cliente, nombre_completo, email, ciudad, segmento, fecha_registro)
# MAGIC ) AS origen
# MAGIC ON destino.id_cliente = origen.id_cliente               -- cómo se emparejan las filas
# MAGIC WHEN MATCHED THEN UPDATE SET *                          -- existe -> actualizar
# MAGIC WHEN NOT MATCHED THEN INSERT *;                         -- no existe -> insertar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM clientes ORDER BY id_cliente;   -- 17 clientes; el 6 con correo nuevo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Eliminar recursos: desde la interfaz y desde código
# MAGIC **Desde la interfaz** (lo que creaste en la sección 2). Se borra **de adentro hacia afuera**:
# MAGIC 1. **Catalog** → `workspace` → `practica_ui` → tabla `productos_ui` → menú ⋮ (arriba a la derecha) → **Delete** → confirma.
# MAGIC 2. Volumen `archivos_ui` → menú ⋮ → **Delete** → confirma (los archivos del volumen gestionado se marcan para borrado).
# MAGIC 3. Schema `practica_ui` → menú ⋮ → **Delete** → confirma. Desde la interfaz, el schema debe estar vacío antes de borrarlo.
# MAGIC
# MAGIC ### 5.0 · Comprobar que se eliminó
# MAGIC `SHOW SCHEMAS IN workspace LIKE 'practica_ui'` busca un schema con ese nombre.
# MAGIC
# MAGIC **Qué esperar:** ninguna fila (si borraste el schema).

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace LIKE 'practica_ui';   -- vacío = eliminado

# COMMAND ----------

# MAGIC %md
# MAGIC **Desde código** practicamos en un schema de pruebas: lo creamos, lo llenamos y lo eliminamos.
# MAGIC
# MAGIC ### 5.1 · Crear recursos de prueba
# MAGIC - Un schema `zona_pruebas`, una tabla pequeña creada con CTAS (`CREATE TABLE … AS SELECT 1 AS id, …`) y un volumen.
# MAGIC
# MAGIC **Qué esperar:** `SHOW TABLES` muestra `tabla_prueba`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS comercializadora.zona_pruebas COMMENT 'Schema temporal para practicar borrados';
# MAGIC CREATE OR REPLACE TABLE comercializadora.zona_pruebas.tabla_prueba AS SELECT 1 AS id, 'prueba' AS texto;
# MAGIC CREATE VOLUME IF NOT EXISTS comercializadora.zona_pruebas.volumen_prueba;
# MAGIC SHOW TABLES IN comercializadora.zona_pruebas;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 · `DROP TABLE` y `UNDROP`
# MAGIC - `DROP TABLE IF EXISTS <tabla>`: borra la tabla (`IF EXISTS` evita el error si no existe).
# MAGIC - Una tabla **gestionada** se puede recuperar durante **7 días**. `SHOW TABLES DROPPED IN <schema>` lista las borradas que aún se pueden recuperar.
# MAGIC - `UNDROP TABLE <tabla>`: la recupera (la más reciente con ese nombre).
# MAGIC
# MAGIC **Qué esperar:** primero la lista de tablas borradas (`tabla_prueba`); después del `UNDROP`, la fila `1 | prueba`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS comercializadora.zona_pruebas.tabla_prueba;   -- borrar
# MAGIC SHOW TABLES DROPPED IN comercializadora.zona_pruebas;              -- ¿qué se puede recuperar?

# COMMAND ----------

# MAGIC %sql
# MAGIC UNDROP TABLE comercializadora.zona_pruebas.tabla_prueba;           -- recuperar
# MAGIC SELECT * FROM comercializadora.zona_pruebas.tabla_prueba;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 · Borrar objeto por objeto o en cascada
# MAGIC - Objeto por objeto: `DROP TABLE`, `DROP VOLUME`, `DROP SCHEMA` (de adentro hacia afuera).
# MAGIC - `DROP SCHEMA` sin más opciones es `RESTRICT`: solo borra si el schema está **vacío**.
# MAGIC - `CASCADE` borra el schema o el catálogo **con todo lo que contiene**. Las líneas con `--` están comentadas: no se ejecutan.
# MAGIC
# MAGIC **Qué esperar:** `SHOW SCHEMAS IN comercializadora` ya no muestra `zona_pruebas`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE  IF EXISTS comercializadora.zona_pruebas.tabla_prueba;     -- 1. la tabla
# MAGIC DROP VOLUME IF EXISTS comercializadora.zona_pruebas.volumen_prueba;   -- 2. el volumen
# MAGIC DROP SCHEMA IF EXISTS comercializadora.zona_pruebas;                  -- 3. el schema (ya vacío)
# MAGIC
# MAGIC -- Todo de una vez con CASCADE (borra el schema y lo que contiene):
# MAGIC -- DROP SCHEMA IF EXISTS workspace.practica_ui CASCADE;
# MAGIC -- DROP CATALOG IF EXISTS comercializadora CASCADE;               -- ¡no lo ejecutes todavía!
# MAGIC
# MAGIC SHOW SCHEMAS IN comercializadora;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Archivos CSV, JSON y Parquet en un volumen
# MAGIC Los datos de un sistema suelen llegar como **archivos**: un CSV exportado de Excel, un JSON de una aplicación web,
# MAGIC un Parquet de otro sistema. Los guardamos en un **volumen** y los leemos con la función `read_files`.
# MAGIC
# MAGIC ### 6.1 · Crear el volumen del sistema de ventas
# MAGIC `CREATE VOLUME IF NOT EXISTS <catálogo>.<schema>.<volumen>` crea un volumen **gestionado** (Databricks administra el almacenamiento).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS comercializadora.sistema_ventas.archivos
# MAGIC   COMMENT 'Archivos fuente del sistema de ventas (subidos a mano)';

# COMMAND ----------

# MAGIC %md
# MAGIC **Paso manual · Subir los 3 archivos**: **Catalog** → `comercializadora` → `sistema_ventas` → `archivos` → **Upload to this volume** →
# MAGIC arrastra `productos.csv`, `ventas_2025.json` y `vendedores.parquet` → **Upload**. Luego ejecuta la celda siguiente.
# MAGIC
# MAGIC **Qué esperar:** los 3 archivos con su tamaño en bytes (1458, 2536 y 606741).

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/comercializadora/sistema_ventas/archivos/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 · Leer el CSV
# MAGIC - `read_files('<ruta>', format => 'csv', header => true)` lee el archivo como si fuera una tabla.
# MAGIC - `format => 'csv'`: el formato. Los parámetros se escriben `nombre => valor`.
# MAGIC - `header => true`: la primera fila tiene los nombres de columna. Sin esta opción, las columnas se llamarían `_c0`, `_c1`…
# MAGIC - La columna extra `_rescued_data` guarda lo que no encaje en el esquema (aquí queda vacía: el archivo está limpio).
# MAGIC
# MAGIC **Qué esperar:** 24 filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/productos.csv',
# MAGIC                 format => 'csv',      -- tipo de archivo
# MAGIC                 header => true);      -- la 1.ª fila son los nombres

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 · ¿Qué tipos infirió Spark?
# MAGIC `DESCRIBE QUERY <consulta>` muestra las columnas y los tipos que tendría el resultado, **sin** devolver los datos.
# MAGIC
# MAGIC **Qué esperar:** `precio_lista` como `int` y `activo` como `string` (`'Sí'`/`'No'`): el CSV no guarda tipos, Spark los **adivina**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE QUERY
# MAGIC SELECT * FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/productos.csv',
# MAGIC                          format => 'csv', header => true);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 · Leer el JSON y su objeto anidado
# MAGIC - Cada línea del archivo es un objeto JSON (*JSON Lines*), que es el formato por defecto.
# MAGIC - `pago` es un objeto dentro del objeto: Spark lo lee como `STRUCT` (una columna con campos adentro).
# MAGIC - `pago.metodo`: con un **punto** se accede a un campo del `STRUCT`.
# MAGIC - `LIMIT 10`: solo para ver una muestra.
# MAGIC
# MAGIC **Qué esperar:** 10 ventas; la columna `pago` se ve como `{cuotas, metodo}`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_venta, fecha_venta, canal,
# MAGIC        pago,                          -- el objeto completo (STRUCT)
# MAGIC        pago.metodo AS metodo_pago,    -- un campo del objeto
# MAGIC        estado
# MAGIC FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/ventas_2025.json', format => 'json')
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 · Tipos inferidos del JSON
# MAGIC **Qué esperar:** `fecha_venta` llega como `string` (JSON no tiene tipo fecha), los números como `bigint`/`double`
# MAGIC y las columnas en **orden alfabético**. Por eso, al crear la tabla, convertimos con `CAST`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE QUERY
# MAGIC SELECT * FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/ventas_2025.json', format => 'json');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 · Leer el Parquet
# MAGIC - Sin `format`: `read_files` lo detecta por la extensión `.parquet`.
# MAGIC - Parquet es **binario y columnar**, y **guarda el esquema** dentro del archivo.
# MAGIC
# MAGIC **Qué esperar:** 8 vendedores; en `DESCRIBE QUERY`, `fecha_ingreso` ya es `date` y `meta_mensual` `decimal(14,2)`: no hay que convertir nada.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/vendedores.parquet');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE QUERY
# MAGIC SELECT * FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/vendedores.parquet');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.7 · Atajo: consultar un archivo por su ruta
# MAGIC `` json.`<ruta>` ``: forma corta de leer un archivo indicando el formato antes de la ruta (entre comillas invertidas).
# MAGIC Es rápida para mirar, pero **no admite opciones** (por ejemplo, `header` en un CSV). Para trabajar en serio, usa `read_files`.
# MAGIC
# MAGIC **Qué esperar:** `ventas = 2238`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS ventas
# MAGIC FROM json.`/Volumes/comercializadora/sistema_ventas/archivos/ventas_2025.json`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.8 · Metadatos del archivo de origen
# MAGIC `_metadata` es una columna oculta que `read_files` agrega con datos del archivo: `file_name`, `file_size`, `file_path`…
# MAGIC No aparece con `SELECT *`: hay que pedirla. `GROUP BY ALL` agrupa por todas las columnas que no son agregados.
# MAGIC
# MAGIC **Qué esperar:** `ventas_2025.json | 606741 | 2238`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name AS archivo,   -- nombre del archivo
# MAGIC        _metadata.file_size AS bytes,     -- tamaño
# MAGIC        count(*)            AS filas
# MAGIC FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/ventas_2025.json', format => 'json')
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · CTAS: de archivo a tabla Delta con buenas prácticas
# MAGIC `CREATE TABLE … AS SELECT` (**CTAS**) crea la tabla **y** la llena con el resultado de la consulta en un solo paso.
# MAGIC El esquema sale de la consulta, por eso **ajustamos tipos y nombres con `CAST … AS`** en el `SELECT`.
# MAGIC Lo que un CTAS **no** permite es declarar la lista de columnas (descripciones, `NOT NULL`, claves): eso se agrega
# MAGIC **justo después con `ALTER TABLE`**. Patrón para cada tabla:
# MAGIC
# MAGIC 1. **CTAS** con `CREATE OR REPLACE` (re-ejecutable) · `CLUSTER BY AUTO` · `COMMENT` de tabla · `TBLPROPERTIES` · columnas de linaje.
# MAGIC 2. **`ALTER TABLE … ALTER COLUMN`** con la descripción (`COMMENT`) de **cada** columna, en una sola sentencia
# MAGIC    (Databricks SQL y Databricks Runtime 16.3+, incluido serverless).
# MAGIC 3. **`ALTER TABLE`** para las restricciones: `NOT NULL`, `PRIMARY KEY`, `FOREIGN KEY` y `CHECK`.
# MAGIC
# MAGIC ### 7.1a · `productos`: CTAS desde el CSV
# MAGIC - `CREATE OR REPLACE TABLE productos … AS SELECT …`: crea la tabla con las columnas y los tipos que salen del `SELECT`.
# MAGIC - `trim(texto)`: quita espacios al inicio y al final.
# MAGIC - **Columnas de linaje** (para saber de dónde viene cada fila): `_metadata.file_name` (archivo) y `current_timestamp()` (fecha y hora de la carga).
# MAGIC
# MAGIC **Qué esperar:** `num_inserted_rows = 24`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE productos
# MAGIC CLUSTER BY AUTO
# MAGIC COMMENT 'Catálogo de productos. Origen: productos.csv'
# MAGIC TBLPROPERTIES ('capa' = 'plata', 'origen' = 'productos.csv', 'responsable' = 'equipo-datos')
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(id_producto AS INT)              AS id_producto,
# MAGIC   trim(nombre_producto)                 AS nombre_producto,  -- sin espacios sobrantes
# MAGIC   categoria,
# MAGIC   marca,
# MAGIC   CAST(precio_lista   AS DECIMAL(12,2)) AS precio_lista,     -- dinero: número exacto
# MAGIC   CAST(costo_unitario AS DECIMAL(12,2)) AS costo_unitario,
# MAGIC   activo = 'Sí'                         AS activo,           -- 'Sí'/'No' -> BOOLEAN
# MAGIC   _metadata.file_name                   AS archivo_origen,   -- linaje: de qué archivo
# MAGIC   current_timestamp()                   AS fecha_carga       -- linaje: cuándo se cargó
# MAGIC FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/productos.csv',
# MAGIC                 format => 'csv', header => true);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1b · `productos`: descripción de cada columna y restricciones
# MAGIC - `ALTER TABLE productos ALTER COLUMN col1 COMMENT '…', col2 COMMENT '…', …`: agrega la descripción de **varias columnas en una sola sentencia**, separadas por comas.
# MAGIC - `ALTER COLUMN id_producto SET NOT NULL`: desde ahora la columna no acepta vacíos (Delta revisa primero que ninguna fila actual esté vacía).
# MAGIC - `ADD CONSTRAINT pk_productos PRIMARY KEY (id_producto)`: clave primaria (informativa). Necesita que la columna sea `NOT NULL`.
# MAGIC - `ADD CONSTRAINT chk_precio_positivo CHECK (precio_lista > 0)`: regla que **toda** fila debe cumplir. Delta rechaza las que no.
# MAGIC
# MAGIC **Qué esperar:** sin filas. En Catalog Explorer, la tabla `productos` muestra la descripción de sus 9 columnas.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE productos ALTER COLUMN
# MAGIC   id_producto     COMMENT 'Identificador único del producto',
# MAGIC   nombre_producto COMMENT 'Nombre comercial del producto',
# MAGIC   categoria       COMMENT 'Tecnología, Hogar, Oficina o Deportes',
# MAGIC   marca           COMMENT 'Marca del producto',
# MAGIC   precio_lista    COMMENT 'Precio de venta sugerido, en COP',
# MAGIC   costo_unitario  COMMENT 'Costo de compra por unidad, en COP',
# MAGIC   activo          COMMENT 'TRUE si el producto se sigue vendiendo',
# MAGIC   archivo_origen  COMMENT 'Archivo del que se cargó la fila',
# MAGIC   fecha_carga     COMMENT 'Momento de la carga';
# MAGIC
# MAGIC ALTER TABLE productos ALTER COLUMN id_producto SET NOT NULL;                         -- obligatoria
# MAGIC ALTER TABLE productos ADD CONSTRAINT pk_productos PRIMARY KEY (id_producto);         -- clave primaria
# MAGIC ALTER TABLE productos ADD CONSTRAINT chk_precio_positivo CHECK (precio_lista > 0);   -- regla de calidad

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2a · `vendedores`: CTAS desde el Parquet
# MAGIC Como Parquet ya trae los tipos correctos (`DATE`, `DECIMAL`, `BOOLEAN`), **no hace falta `CAST`**: solo seleccionamos las
# MAGIC columnas y agregamos el linaje. Compara con el CSV: **el formato de origen decide cuánta limpieza necesitas**.
# MAGIC
# MAGIC **Qué esperar:** `num_inserted_rows = 8`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vendedores
# MAGIC CLUSTER BY AUTO
# MAGIC COMMENT 'Equipo comercial y su meta de ventas mensual. Origen: vendedores.parquet'
# MAGIC TBLPROPERTIES ('capa' = 'plata', 'origen' = 'vendedores.parquet', 'responsable' = 'equipo-datos')
# MAGIC AS
# MAGIC SELECT id_vendedor, nombre_vendedor, sede, ciudad, fecha_ingreso, meta_mensual, activo,  -- tipos ya correctos
# MAGIC        _metadata.file_name AS archivo_origen,
# MAGIC        current_timestamp() AS fecha_carga
# MAGIC FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/vendedores.parquet');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2b · `vendedores`: descripción de cada columna y restricciones
# MAGIC Mismo patrón que 7.1b: descripciones de todas las columnas, `NOT NULL` + clave primaria en el id y una regla `CHECK`
# MAGIC (la meta debe ser positiva).

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE vendedores ALTER COLUMN
# MAGIC   id_vendedor     COMMENT 'Identificador único del vendedor',
# MAGIC   nombre_vendedor COMMENT 'Nombre completo',
# MAGIC   sede            COMMENT 'Tienda donde trabaja',
# MAGIC   ciudad          COMMENT 'Ciudad de la sede',
# MAGIC   fecha_ingreso   COMMENT 'Fecha de ingreso a la empresa',
# MAGIC   meta_mensual    COMMENT 'Meta de ventas netas por mes, en COP',
# MAGIC   activo          COMMENT 'TRUE si sigue en la empresa',
# MAGIC   archivo_origen  COMMENT 'Archivo del que se cargó la fila',
# MAGIC   fecha_carga     COMMENT 'Momento de la carga';
# MAGIC
# MAGIC ALTER TABLE vendedores ALTER COLUMN id_vendedor SET NOT NULL;
# MAGIC ALTER TABLE vendedores ADD CONSTRAINT pk_vendedores PRIMARY KEY (id_vendedor);
# MAGIC ALTER TABLE vendedores ADD CONSTRAINT chk_meta_positiva CHECK (meta_mensual > 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3a · `ventas`: CTAS desde el JSON
# MAGIC Además de convertir tipos:
# MAGIC - **Columna calculada** `valor_neto` = `cantidad × precio_unitario × (1 − descuento)`.
# MAGIC   `coalesce(descuento_pct, 0)` usa 0 cuando el descuento viene vacío (`NULL`); si no, el resultado entero sería `NULL`.
# MAGIC - **Aplanar el `STRUCT`**: `pago.metodo` y `pago.cuotas` se vuelven columnas normales (`metodo_pago`, `cuotas`).
# MAGIC - `CAST(fecha_venta AS DATE)`: el texto `'2025-01-01'` pasa a tipo fecha.
# MAGIC
# MAGIC **Qué esperar:** `num_inserted_rows = 2238`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ventas
# MAGIC CLUSTER BY AUTO
# MAGIC COMMENT 'Ventas 2025, una fila por venta. Origen: ventas_2025.json'
# MAGIC TBLPROPERTIES ('capa' = 'plata', 'origen' = 'ventas_2025.json', 'responsable' = 'equipo-datos')
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(id_venta        AS INT)           AS id_venta,
# MAGIC   CAST(fecha_venta     AS DATE)          AS fecha_venta,      -- texto -> fecha
# MAGIC   CAST(id_cliente      AS INT)           AS id_cliente,
# MAGIC   CAST(id_producto     AS INT)           AS id_producto,
# MAGIC   CAST(id_vendedor     AS INT)           AS id_vendedor,      -- NULL en ventas en línea
# MAGIC   CAST(cantidad        AS INT)           AS cantidad,
# MAGIC   CAST(precio_unitario AS DECIMAL(12,2)) AS precio_unitario,
# MAGIC   CAST(descuento_pct   AS DECIMAL(4,2))  AS descuento_pct,    -- NULL = sin descuento registrado
# MAGIC   CAST(cantidad * precio_unitario * (1 - coalesce(descuento_pct, 0)) AS DECIMAL(14,2)) AS valor_neto,  -- calculada
# MAGIC   canal,
# MAGIC   pago.metodo                            AS metodo_pago,      -- aplanar el STRUCT
# MAGIC   CAST(pago.cuotas AS INT)               AS cuotas,
# MAGIC   estado,
# MAGIC   _metadata.file_name                    AS archivo_origen,
# MAGIC   current_timestamp()                    AS fecha_carga
# MAGIC FROM read_files('/Volumes/comercializadora/sistema_ventas/archivos/ventas_2025.json', format => 'json');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3b · `ventas`: descripción de cada columna
# MAGIC Las 15 columnas con su descripción, en una sola sentencia `ALTER TABLE … ALTER COLUMN`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ventas ALTER COLUMN
# MAGIC   id_venta        COMMENT 'Identificador único de la venta',
# MAGIC   fecha_venta     COMMENT 'Fecha de la venta',
# MAGIC   id_cliente      COMMENT 'Cliente que compró (clientes.id_cliente)',
# MAGIC   id_producto     COMMENT 'Producto vendido (productos.id_producto)',
# MAGIC   id_vendedor     COMMENT 'Vendedor (vendedores.id_vendedor). NULL en ventas en línea',
# MAGIC   cantidad        COMMENT 'Unidades vendidas',
# MAGIC   precio_unitario COMMENT 'Precio por unidad al momento de la venta, en COP',
# MAGIC   descuento_pct   COMMENT 'Descuento aplicado (0.05 = 5 %). NULL = sin dato',
# MAGIC   valor_neto      COMMENT 'cantidad x precio_unitario x (1 - descuento), en COP',
# MAGIC   canal           COMMENT 'Tienda física, En línea o Teléfono',
# MAGIC   metodo_pago     COMMENT 'Medio de pago',
# MAGIC   cuotas          COMMENT 'Número de cuotas (1 si no es a crédito)',
# MAGIC   estado          COMMENT 'Completada, Devuelta o Anulada',
# MAGIC   archivo_origen  COMMENT 'Archivo del que se cargó la fila',
# MAGIC   fecha_carga     COMMENT 'Momento de la carga';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3c · `ventas`: restricciones
# MAGIC - Clave primaria en `id_venta` (antes, `SET NOT NULL`).
# MAGIC - **Claves foráneas** (`FOREIGN KEY (col) REFERENCES otra_tabla`): documentan que `id_cliente`, `id_producto` e `id_vendedor`
# MAGIC   apuntan a la clave primaria de `clientes`, `productos` y `vendedores`. Son **informativas** (no se validan), pero Catalog
# MAGIC   Explorer las muestra como relaciones entre tablas.
# MAGIC - `CHECK (cantidad > 0)`: no se aceptan ventas con cantidad cero o negativa.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ventas ALTER COLUMN id_venta SET NOT NULL;
# MAGIC ALTER TABLE ventas ADD CONSTRAINT pk_ventas PRIMARY KEY (id_venta);
# MAGIC ALTER TABLE ventas ADD CONSTRAINT fk_ventas_clientes   FOREIGN KEY (id_cliente)  REFERENCES clientes;
# MAGIC ALTER TABLE ventas ADD CONSTRAINT fk_ventas_productos  FOREIGN KEY (id_producto) REFERENCES productos;
# MAGIC ALTER TABLE ventas ADD CONSTRAINT fk_ventas_vendedores FOREIGN KEY (id_vendedor) REFERENCES vendedores;
# MAGIC ALTER TABLE ventas ADD CONSTRAINT chk_cantidad_positiva CHECK (cantidad > 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 · Verificar el resultado
# MAGIC - `DESCRIBE TABLE EXTENDED ventas`: columnas, tipos y **comentarios**, más detalles de la tabla (clustering, ubicación, restricciones).
# MAGIC - `SHOW TBLPROPERTIES ventas`: todas las propiedades, incluidas las nuestras (`capa`, `origen`, `responsable`)
# MAGIC   y las restricciones `CHECK` (`delta.constraints.…`).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED ventas;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES ventas;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.5 · Conteo final de las cuatro tablas del sistema
# MAGIC `UNION ALL` **apila** los resultados de varias consultas que tienen las mismas columnas.
# MAGIC
# MAGIC **Qué esperar:** clientes 17 · productos 24 · vendedores 8 · ventas 2238.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'clientes' AS tabla, count(*) AS filas FROM clientes
# MAGIC UNION ALL SELECT 'productos',  count(*) FROM productos      -- se apila debajo
# MAGIC UNION ALL SELECT 'vendedores', count(*) FROM vendedores
# MAGIC UNION ALL SELECT 'ventas',     count(*) FROM ventas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · CTE: consultas en pasos con nombre
# MAGIC Una **CTE** (*Common Table Expression*) es un resultado temporal **con nombre** que solo existe dentro de esa consulta:
# MAGIC
# MAGIC ```sql
# MAGIC WITH paso_1 AS ( SELECT … ),          -- se define con WITH nombre AS ( consulta )
# MAGIC      paso_2 AS ( SELECT … FROM paso_1 ) -- cada paso puede usar los anteriores; se separan con coma
# MAGIC SELECT … FROM paso_2;                 -- la consulta final usa los pasos
# MAGIC ```
# MAGIC
# MAGIC Sirve para escribir una consulta compleja **de arriba abajo, paso a paso**, en lugar de meter subconsultas unas dentro de otras.
# MAGIC
# MAGIC ### 8.1 · Necesidad: «¿qué productos venden más que el producto promedio?»
# MAGIC **Primero sin CTE.** Una **subconsulta** es un `SELECT` entre paréntesis dentro de otra consulta. Aquí hay tres anidadas:
# MAGIC - la de adentro calcula las ventas por producto,
# MAGIC - `avg(…)` calcula el promedio de esas ventas,
# MAGIC - y el cálculo de ventas por producto **se repite dos veces**. Es difícil de leer y de mantener.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT id_producto, sum(valor_neto) AS ventas_netas          -- ventas por producto
# MAGIC   FROM ventas WHERE estado = 'Completada' GROUP BY id_producto) AS por_producto
# MAGIC WHERE ventas_netas > (
# MAGIC   SELECT avg(ventas_netas) FROM (                              -- promedio de...
# MAGIC     SELECT sum(valor_neto) AS ventas_netas                     -- ...las ventas por producto (¡repetido!)
# MAGIC     FROM ventas WHERE estado = 'Completada' GROUP BY id_producto))
# MAGIC ORDER BY ventas_netas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **Ahora con CTE.** Mismo resultado, en dos pasos con nombre:
# MAGIC 1. `por_producto`: ventas netas de cada producto (solo ventas completadas).
# MAGIC 2. `promedio`: el promedio de esas ventas, **reutilizando** `por_producto`.
# MAGIC 3. Consulta final: une con `productos` para mostrar el nombre (`JOIN … USING (id_producto)`) y filtra los que superan el promedio.
# MAGIC    `pp` y `p` son **alias de tabla**: nombres cortos para no repetir el nombre completo.
# MAGIC
# MAGIC **Qué esperar:** 6 productos, encabezados por el Portátil 14 pulgadas (289,4 millones).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_producto AS (                       -- paso 1
# MAGIC   SELECT id_producto, sum(valor_neto) AS ventas_netas
# MAGIC   FROM ventas
# MAGIC   WHERE estado = 'Completada'
# MAGIC   GROUP BY id_producto
# MAGIC ),
# MAGIC promedio AS (                                -- paso 2: usa el paso 1
# MAGIC   SELECT avg(ventas_netas) AS valor FROM por_producto
# MAGIC )
# MAGIC SELECT p.nombre_producto, pp.ventas_netas    -- consulta final
# MAGIC FROM por_producto pp
# MAGIC JOIN productos p USING (id_producto)         -- traer el nombre del producto
# MAGIC WHERE pp.ventas_netas > (SELECT valor FROM promedio)
# MAGIC ORDER BY pp.ventas_netas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 · Necesidad: «¿qué vendedores cumplieron su meta cada mes?»
# MAGIC Tres pasos encadenados:
# MAGIC 1. `ventas_ok`: solo ventas **completadas** que tienen vendedor (`id_vendedor IS NOT NULL`: las ventas en línea no tienen).
# MAGIC 2. `por_mes`: total de cada vendedor en cada mes. `date_trunc('MONTH', fecha)` lleva cada fecha al primer día de su mes,
# MAGIC    así todas las ventas de marzo quedan en `2025-03-01` y se pueden agrupar.
# MAGIC 3. `con_meta`: une con `vendedores` para traer el nombre y la meta, y calcula `cumplimiento_pct` = ventas ÷ meta × 100.
# MAGIC
# MAGIC En la consulta final, `cumplimiento_pct >= 100 AS cumplio_meta` es una comparación que devuelve `true`/`false`.
# MAGIC
# MAGIC **Qué esperar:** 96 filas (8 vendedores × 12 meses). En enero cumplieron Paula Salazar (106,3 %) y Carolina Méndez (102 %).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ventas_ok AS (                          -- paso 1: ventas completadas con vendedor
# MAGIC   SELECT * FROM ventas
# MAGIC   WHERE estado = 'Completada' AND id_vendedor IS NOT NULL
# MAGIC ),
# MAGIC por_mes AS (                                 -- paso 2: total por vendedor y mes
# MAGIC   SELECT id_vendedor,
# MAGIC          date_trunc('MONTH', fecha_venta) AS mes,
# MAGIC          sum(valor_neto)                  AS ventas_mes
# MAGIC   FROM ventas_ok
# MAGIC   GROUP BY id_vendedor, date_trunc('MONTH', fecha_venta)
# MAGIC ),
# MAGIC con_meta AS (                                -- paso 3: comparar con la meta
# MAGIC   SELECT v.nombre_vendedor, v.sede, pm.mes, pm.ventas_mes, v.meta_mensual,
# MAGIC          round(pm.ventas_mes / v.meta_mensual * 100, 1) AS cumplimiento_pct
# MAGIC   FROM por_mes pm
# MAGIC   JOIN vendedores v USING (id_vendedor)
# MAGIC )
# MAGIC SELECT *, cumplimiento_pct >= 100 AS cumplio_meta   -- true / false
# MAGIC FROM con_meta
# MAGIC ORDER BY mes, cumplimiento_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 · CTE + CTAS: guardar el resultado como tabla «oro»
# MAGIC - Un CTAS puede llevar una CTE: `CREATE OR REPLACE TABLE … AS WITH … SELECT …`.
# MAGIC - La tabla `cumplimiento_metas` es de capa **oro**: datos agregados, listos para reportes y tableros.
# MAGIC - `GROUP BY ALL`: agrupa por todas las columnas del `SELECT` que no son agregados (aquí, `id_vendedor` y `mes`).
# MAGIC - Después del CTAS, igual que en la sección 7, agregamos la descripción de cada columna con `ALTER TABLE … ALTER COLUMN`.
# MAGIC - Última consulta: `count_if(condición)` cuenta las filas que cumplen la condición (los meses con meta cumplida).
# MAGIC
# MAGIC **Qué esperar:** Carolina Méndez con 7 meses con meta; Ricardo Pabón con 3.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cumplimiento_metas
# MAGIC CLUSTER BY AUTO
# MAGIC COMMENT 'Cumplimiento mensual de meta por vendedor (capa oro)'
# MAGIC TBLPROPERTIES ('capa' = 'oro', 'responsable' = 'equipo-datos')
# MAGIC AS
# MAGIC WITH por_mes AS (                                         -- CTE dentro del CTAS
# MAGIC   SELECT id_vendedor, date_trunc('MONTH', fecha_venta) AS mes, sum(valor_neto) AS ventas_mes
# MAGIC   FROM ventas
# MAGIC   WHERE estado = 'Completada' AND id_vendedor IS NOT NULL
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT v.nombre_vendedor, v.sede, CAST(pm.mes AS DATE) AS mes, pm.ventas_mes, v.meta_mensual,
# MAGIC        round(pm.ventas_mes / v.meta_mensual * 100, 1) AS cumplimiento_pct
# MAGIC FROM por_mes pm
# MAGIC JOIN vendedores v USING (id_vendedor);
# MAGIC
# MAGIC ALTER TABLE cumplimiento_metas ALTER COLUMN                 -- descripciones
# MAGIC   nombre_vendedor  COMMENT 'Vendedor',
# MAGIC   sede             COMMENT 'Sede del vendedor',
# MAGIC   mes              COMMENT 'Primer día del mes',
# MAGIC   ventas_mes       COMMENT 'Ventas netas completadas del mes, en COP',
# MAGIC   meta_mensual     COMMENT 'Meta del mes, en COP',
# MAGIC   cumplimiento_pct COMMENT 'ventas_mes / meta_mensual x 100';
# MAGIC
# MAGIC SELECT nombre_vendedor,
# MAGIC        count_if(cumplimiento_pct >= 100) AS meses_con_meta,      -- cuántos meses cumplió
# MAGIC        round(avg(cumplimiento_pct), 1)   AS cumplimiento_promedio
# MAGIC FROM cumplimiento_metas
# MAGIC GROUP BY nombre_vendedor
# MAGIC ORDER BY meses_con_meta DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Análisis por necesidades del negocio
# MAGIC Cada ejercicio parte de una **pregunta real** de un área de la empresa. La celda SQL la responde (resultado en tabla)
# MAGIC y la celda Python siguiente dibuja el resultado.
# MAGIC
# MAGIC **Cómo pasa el resultado de SQL a Python**
# MAGIC - Databricks guarda el resultado de la **última** celda SQL ejecutada en la variable de Python **`_sqldf`** (un DataFrame de Spark).
# MAGIC - `_sqldf.toPandas()` lo convierte en un DataFrame de **pandas**, la librería de tablas de Python.
# MAGIC - `pdf.plot(kind=…, x=…, y=…)` dibuja el gráfico con una sola línea. `kind` es el tipo: `"bar"` (barras verticales),
# MAGIC   `"barh"` (barras horizontales), `"line"` (línea) o `"pie"` (torta).
# MAGIC - Las columnas de dinero llegan como `DECIMAL`; las dividimos entre 1.000.000 (`/ 1e6`) para graficarlas en **millones de pesos**.
# MAGIC
# MAGIC > Ejecuta siempre la celda Python **justo después** de su celda SQL: `_sqldf` cambia cada vez que corre una celda SQL.
# MAGIC
# MAGIC ### 9.0 · Preparar Python para graficar
# MAGIC - `import pandas as pd`: carga pandas con el nombre corto `pd`.
# MAGIC - `import matplotlib.pyplot as plt`: carga la librería de gráficos que usa pandas por debajo; `plt.show()` muestra el gráfico.
# MAGIC - `plt.rcParams["figure.figsize"] = (10, 4)`: tamaño por defecto de los gráficos (ancho, alto).

# COMMAND ----------

import pandas as pd                          # tablas en Python
import matplotlib.pyplot as plt              # gráficos (pandas la usa por debajo)

plt.rcParams["figure.figsize"] = (10, 4)     # tamaño por defecto de cada gráfico
print("Listo para graficar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 1 · Filtrar por fecha
# MAGIC *«Marketing quiere revisar las ventas completadas de la primera semana de diciembre de 2025.»*
# MAGIC - `BETWEEN '2025-12-01' AND '2025-12-07'`: fechas entre esos dos días, **incluidos** los extremos.
# MAGIC - `AND estado = 'Completada'`: además, solo ventas completadas.
# MAGIC
# MAGIC **Qué esperar:** 64 ventas, ordenadas por fecha.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_venta, fecha_venta, canal, cantidad, valor_neto
# MAGIC FROM ventas
# MAGIC WHERE fecha_venta BETWEEN '2025-12-01' AND '2025-12-07'     -- incluye el 1 y el 7
# MAGIC   AND estado = 'Completada'
# MAGIC ORDER BY fecha_venta, id_venta;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 2 · Varias condiciones a la vez
# MAGIC *«Finanzas necesita las ventas en línea pagadas con tarjeta de crédito a 12 cuotas o más, del segundo semestre.»*
# MAGIC Cuatro condiciones unidas con `AND`: todas deben cumplirse. `>=` significa «mayor o igual».
# MAGIC
# MAGIC **Qué esperar:** 107 ventas; la más grande, de 17,4 millones a 24 cuotas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_venta, fecha_venta, metodo_pago, cuotas, valor_neto
# MAGIC FROM ventas
# MAGIC WHERE canal = 'En línea'                   -- 1. canal
# MAGIC   AND metodo_pago = 'Tarjeta de crédito'   -- 2. medio de pago
# MAGIC   AND cuotas >= 12                         -- 3. 12 cuotas o más
# MAGIC   AND fecha_venta >= '2025-07-01'          -- 4. desde julio
# MAGIC ORDER BY valor_neto DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 3 · Buscar texto
# MAGIC *«Un cliente pregunta por todo lo que tengamos para el escritorio»* y *«comercial quiere los clientes con correo corporativo».*
# MAGIC - `LIKE 'patrón'`: busca texto con comodines. `%` = cualquier texto (incluso vacío).
# MAGIC - `ILIKE`: igual que `LIKE` pero **sin distinguir** mayúsculas de minúsculas («Escritorio» y «escritorio» coinciden).
# MAGIC - `NOT LIKE '%@correo.com'`: los correos que **no** terminan en `@correo.com` (el dominio personal genérico).
# MAGIC
# MAGIC **Qué esperar:** 2 productos (Escritorio en L y Lámpara LED de escritorio) y 5 empresas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_producto, categoria, precio_lista
# MAGIC FROM productos
# MAGIC WHERE nombre_producto ILIKE '%escritorio%';   -- contiene "escritorio", sin importar mayúsculas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_completo, email, segmento
# MAGIC FROM clientes
# MAGIC WHERE email NOT LIKE '%@correo.com';          -- no termina en @correo.com

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 4 · Datos faltantes (`NULL`)
# MAGIC *«¿Cuántas ventas no tienen el descuento registrado? ¿En qué canal pasa más?»*
# MAGIC - `NULL` = **sin dato**. Se pregunta con `IS NULL` (nunca con `= NULL`, que no funciona).
# MAGIC - `count_if(descuento_pct IS NULL)`: cuenta las filas sin descuento.
# MAGIC - `coalesce(descuento_pct, 0)`: si el descuento es `NULL`, usa 0. Así el promedio no ignora esas ventas.
# MAGIC
# MAGIC **Qué esperar:** Tienda física 135 de 1197 (11,3 %), En línea 69 de 800, Teléfono 26 de 241.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT canal,
# MAGIC        count(*)                                                   AS ventas,
# MAGIC        count_if(descuento_pct IS NULL)                            AS sin_descuento_registrado,
# MAGIC        round(count_if(descuento_pct IS NULL) / count(*) * 100, 1) AS pct_sin_dato,
# MAGIC        round(avg(coalesce(descuento_pct, 0)) * 100, 2)            AS descuento_promedio_pct
# MAGIC FROM ventas
# MAGIC GROUP BY canal
# MAGIC ORDER BY ventas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 5 · Condicionales (`CASE`)
# MAGIC *«Queremos segmentar las ventas por tamaño del ticket para diseñar promociones.»*
# MAGIC - `CASE WHEN condición THEN valor … ELSE valor END`: devuelve un valor distinto según la condición. Se evalúa **en orden**
# MAGIC   y se queda con la **primera** que se cumple; `ELSE` cubre todo lo demás.
# MAGIC - Los prefijos `1 ·`, `2 ·`, `3 ·` sirven para que el `ORDER BY` ponga los tamaños en orden lógico.
# MAGIC
# MAGIC **Qué esperar:** 112 ventas grandes (6 %) suman 512 millones: **el 40 % del dinero**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE
# MAGIC          WHEN valor_neto >= 2000000 THEN '3 · Grande (2 M o más)'          -- primera condición
# MAGIC          WHEN valor_neto >=  500000 THEN '2 · Mediana (500 mil a 2 M)'     -- si no, la segunda
# MAGIC          ELSE                            '1 · Pequeña (menos de 500 mil)'  -- todo lo demás
# MAGIC        END                AS tamano_ticket,
# MAGIC        count(*)           AS ventas,
# MAGIC        sum(valor_neto)    AS ventas_netas
# MAGIC FROM ventas
# MAGIC WHERE estado = 'Completada'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY tamano_ticket;

# COMMAND ----------

# MAGIC %md
# MAGIC **Gráfico de barras.** `kind="bar"` dibuja una barra por fila: `x` = la etiqueta de cada barra, `y` = la altura.
# MAGIC `legend=False` quita la leyenda (solo hay una serie) y `rot=0` deja las etiquetas horizontales.

# COMMAND ----------

pdf = _sqldf.toPandas()                                        # resultado SQL -> pandas
pdf["ventas_millones"] = pdf["ventas_netas"].astype(float) / 1e6   # DECIMAL -> número, en millones

pdf.plot(kind="bar", x="tamano_ticket", y="ventas_millones",
         title="Ventas netas por tamaño de ticket (millones de COP)", legend=False, rot=0)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 6 · Agrupar y resumir
# MAGIC *«¿Qué canal de venta nos deja más dinero y cuál tiene el ticket promedio más alto?»*
# MAGIC Varias agregaciones por canal en una sola consulta: número de ventas, unidades, dinero y ticket promedio (`avg`).
# MAGIC
# MAGIC **Qué esperar:** Tienda física 703,4 M · En línea 460,4 M · Teléfono 106,6 M.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT canal,
# MAGIC        count(*)               AS ventas,
# MAGIC        sum(cantidad)          AS unidades,
# MAGIC        sum(valor_neto)        AS ventas_netas,
# MAGIC        round(avg(valor_neto)) AS ticket_promedio
# MAGIC FROM ventas
# MAGIC WHERE estado = 'Completada'
# MAGIC GROUP BY canal
# MAGIC ORDER BY ventas_netas DESC;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["ventas_millones"] = pdf["ventas_netas"].astype(float) / 1e6

pdf.plot(kind="bar", x="canal", y="ventas_millones",
         title="Ventas netas por canal (millones de COP)", legend=False, rot=0)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 7 · Trabajar con fechas
# MAGIC *«Gerencia quiere ver cómo se comportaron las ventas mes a mes durante 2025.»*
# MAGIC - `date_trunc('MONTH', fecha_venta)`: lleva cada fecha al primer día de su mes (2025-03-17 → 2025-03-01) para agrupar por mes.
# MAGIC
# MAGIC **Gráfico de línea.** Para ver una **evolución en el tiempo** se usa `kind="line"`; `marker="o"` pone un punto en cada mes.
# MAGIC
# MAGIC **Qué esperar:** febrero es el mes más bajo (64,5 M) y diciembre el más alto (186,0 M).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc('MONTH', fecha_venta) AS mes,      -- primer día del mes
# MAGIC        count(*)                         AS ventas,
# MAGIC        sum(valor_neto)                  AS ventas_netas
# MAGIC FROM ventas
# MAGIC WHERE estado = 'Completada'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY mes;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["ventas_millones"] = pdf["ventas_netas"].astype(float) / 1e6

pdf.plot(kind="line", x="mes", y="ventas_millones", marker="o",
         title="Ventas netas por mes, 2025 (millones de COP)", legend=False)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 8 · Unir tablas (`JOIN`)
# MAGIC *«Servicio al cliente necesita ver una venta con nombres, no con códigos: quién compró, qué compró y quién la vendió.»*
# MAGIC - `FROM ventas v`: `v` es el **alias** de la tabla; `v.id_venta` = columna `id_venta` de `ventas`.
# MAGIC - `JOIN clientes c ON v.id_cliente = c.id_cliente`: trae la fila de `clientes` cuyo id coincide. Solo quedan las filas **con pareja**.
# MAGIC - `LEFT JOIN vendedores ve …`: conserva **todas** las ventas aunque no tengan vendedor (las ventas en línea); en esas, las columnas de `vendedores` quedan en `NULL`.
# MAGIC - `coalesce(ve.nombre_vendedor, 'Venta en línea')`: si no hay vendedor, muestra ese texto.
# MAGIC
# MAGIC **Qué esperar:** 5 ventas con nombres; la 100004 aparece como «Venta en línea».

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.id_venta, v.fecha_venta,
# MAGIC        c.nombre_completo AS cliente,
# MAGIC        p.nombre_producto AS producto,
# MAGIC        coalesce(ve.nombre_vendedor, 'Venta en línea') AS vendedor,
# MAGIC        v.canal, v.valor_neto
# MAGIC FROM ventas v
# MAGIC JOIN      clientes   c  ON v.id_cliente  = c.id_cliente     -- siempre hay cliente
# MAGIC JOIN      productos  p  ON v.id_producto = p.id_producto    -- siempre hay producto
# MAGIC LEFT JOIN vendedores ve ON v.id_vendedor = ve.id_vendedor   -- puede no haber vendedor
# MAGIC WHERE v.id_venta IN (100001, 100002, 100003, 100004, 100005)
# MAGIC ORDER BY v.id_venta;

# COMMAND ----------

# MAGIC %md
# MAGIC **Guardar la unión como vista.** Como esta unión la vamos a usar en muchas consultas, la guardamos como **vista**:
# MAGIC - `CREATE OR REPLACE VIEW <nombre> AS SELECT …`: guarda **la consulta**, no los datos. Cada vez que la consultas, se ejecuta sobre los datos actuales.
# MAGIC - `USING (id_cliente)`: atajo de `ON v.id_cliente = c.id_cliente` cuando la columna se llama igual en las dos tablas.
# MAGIC - `v.*`: todas las columnas de `ventas`.
# MAGIC - `utilidad` = valor neto − (cantidad × costo unitario).
# MAGIC
# MAGIC **Qué esperar:** `filas = 2238` (no se perdió ninguna venta gracias al `LEFT JOIN`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ventas_detalle
# MAGIC COMMENT 'Ventas con los datos de cliente, producto y vendedor'
# MAGIC AS
# MAGIC SELECT v.*,                                                     -- todas las columnas de ventas
# MAGIC        c.nombre_completo AS cliente, c.ciudad AS ciudad_cliente, c.segmento,
# MAGIC        p.nombre_producto, p.categoria, p.costo_unitario,
# MAGIC        v.valor_neto - v.cantidad * p.costo_unitario AS utilidad,  -- columna calculada
# MAGIC        ve.nombre_vendedor, ve.sede
# MAGIC FROM ventas v
# MAGIC JOIN      clientes   c  USING (id_cliente)
# MAGIC JOIN      productos  p  USING (id_producto)
# MAGIC LEFT JOIN vendedores ve USING (id_vendedor);
# MAGIC
# MAGIC SELECT count(*) AS filas FROM ventas_detalle;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 9 · `JOIN` + agrupación
# MAGIC *«¿Qué categoría vende más y cuál deja más utilidad?»*
# MAGIC Usamos la vista `ventas_detalle`, que ya trae `categoria` y `utilidad`. `margen_pct` = utilidad ÷ ventas × 100.
# MAGIC
# MAGIC **Gráfico con dos series.** Si `y` recibe una **lista** de columnas (`y=["…", "…"]`), pandas dibuja dos barras por categoría y agrega la leyenda.
# MAGIC
# MAGIC **Qué esperar:** Tecnología vende más (595,7 M), pero Hogar tiene el mejor margen (37,5 %).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT categoria,
# MAGIC        sum(cantidad)                                   AS unidades,
# MAGIC        sum(valor_neto)                                 AS ventas_netas,
# MAGIC        sum(utilidad)                                   AS utilidad,
# MAGIC        round(sum(utilidad) / sum(valor_neto) * 100, 1) AS margen_pct
# MAGIC FROM ventas_detalle
# MAGIC WHERE estado = 'Completada'
# MAGIC GROUP BY categoria
# MAGIC ORDER BY ventas_netas DESC;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["Ventas netas"] = pdf["ventas_netas"].astype(float) / 1e6   # nombres con espacio = nombres de la leyenda
pdf["Utilidad"]     = pdf["utilidad"].astype(float) / 1e6

pdf.plot(kind="bar", x="categoria", y=["Ventas netas", "Utilidad"],   # lista = dos series
         title="Ventas netas y utilidad por categoría (millones de COP)", rot=0)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 10 · Filtrar grupos (`HAVING`)
# MAGIC *«Queremos un programa de fidelización para los clientes que compraron más de 35 millones en 2025.»*
# MAGIC - `WHERE` filtra **filas** antes de agrupar (solo ventas completadas).
# MAGIC - `HAVING sum(valor_neto) > 35000000` filtra **grupos** después de agrupar (clientes con más de 35 millones).
# MAGIC
# MAGIC **Gráfico de barras horizontales.** `kind="barh"` es mejor cuando las etiquetas son nombres largos.
# MAGIC `pdf.iloc[::-1]` invierte el orden de las filas para que el mayor quede **arriba**.
# MAGIC
# MAGIC **Qué esperar:** 9 clientes: 4 empresas (203 a 257 M) y 5 personas (36 a 43 M).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cliente, segmento,
# MAGIC        count(*)        AS compras,
# MAGIC        sum(valor_neto) AS total_comprado
# MAGIC FROM ventas_detalle
# MAGIC WHERE estado = 'Completada'            -- filtra FILAS (antes de agrupar)
# MAGIC GROUP BY cliente, segmento
# MAGIC HAVING sum(valor_neto) > 35000000      -- filtra GRUPOS (después de agrupar)
# MAGIC ORDER BY total_comprado DESC;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["total_millones"] = pdf["total_comprado"].astype(float) / 1e6

pdf.iloc[::-1].plot(kind="barh", x="cliente", y="total_millones",      # iloc[::-1] = el mayor arriba
                    title="Clientes con compras de más de 35 millones (millones de COP)", legend=False)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 11 · Lo que NO está (`LEFT ANTI JOIN`)
# MAGIC *«Inventario pregunta qué productos nunca se han vendido»* y *«comercial quiere saber qué clientes aún no compran».*
# MAGIC - `LEFT ANTI JOIN`: devuelve las filas de la tabla de la **izquierda** que **no tienen pareja** en la de la derecha.
# MAGIC
# MAGIC **Qué esperar:** productos 23 (Tableta gráfica) y 24 (Banda elástica); clientes 16 y 17, los que llegaron con el `MERGE` de 4.11.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.id_producto, p.nombre_producto, p.categoria, p.activo
# MAGIC FROM productos p
# MAGIC LEFT ANTI JOIN ventas v ON p.id_producto = v.id_producto;   -- productos SIN ventas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.id_cliente, c.nombre_completo, c.fecha_registro
# MAGIC FROM clientes c
# MAGIC LEFT ANTI JOIN ventas v ON c.id_cliente = v.id_cliente;     -- clientes SIN compras

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 12 · Ranking (función de ventana)
# MAGIC *«¿Cuál es el ranking de vendedores del año, y cuánto aporta cada uno al total?»*
# MAGIC
# MAGIC Una **función de ventana** calcula sobre un grupo de filas **sin colapsarlas**: cada fila conserva su detalle y gana una columna nueva.
# MAGIC Se reconoce por `OVER (…)`.
# MAGIC - `rank() OVER (ORDER BY sum(valor_neto) DESC)`: numera a los vendedores de mayor a menor venta (1, 2, 3…; los empates comparten puesto).
# MAGIC - `sum(sum(valor_neto)) OVER ()`: `OVER ()` vacío = **todas** las filas; da el total general para calcular el porcentaje de cada vendedor.
# MAGIC
# MAGIC **Qué esperar:** 1.º Carolina Méndez (125,9 M, 15,5 %) … 8.º Ricardo Pabón (49,4 M, 6,1 %).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_vendedor, sede,
# MAGIC        sum(valor_neto)                                                AS ventas_netas,
# MAGIC        rank() OVER (ORDER BY sum(valor_neto) DESC)                    AS puesto,           -- ranking
# MAGIC        round(sum(valor_neto) / sum(sum(valor_neto)) OVER () * 100, 1) AS participacion_pct -- % del total
# MAGIC FROM ventas_detalle
# MAGIC WHERE estado = 'Completada' AND nombre_vendedor IS NOT NULL
# MAGIC GROUP BY nombre_vendedor, sede
# MAGIC ORDER BY puesto;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["ventas_millones"] = pdf["ventas_netas"].astype(float) / 1e6

pdf.iloc[::-1].plot(kind="barh", x="nombre_vendedor", y="ventas_millones",
                    title="Ranking de vendedores 2025 (millones de COP)", legend=False)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 13 · El mejor de cada grupo (`QUALIFY`)
# MAGIC *«Recursos humanos quiere premiar al mejor vendedor de cada mes.»*
# MAGIC - `row_number() OVER (PARTITION BY mes ORDER BY ventas_mes DESC)`: numera 1, 2, 3… **dentro de cada mes**
# MAGIC   (`PARTITION BY` reinicia la numeración en cada mes), del que más vendió al que menos.
# MAGIC - `QUALIFY … = 1`: deja solo la fila número 1 de cada mes. `QUALIFY` filtra por una función de ventana, como `HAVING` filtra por un agregado.
# MAGIC
# MAGIC **Qué esperar:** 12 filas, una por mes. Diciembre: Esteban Muñoz (397,9 % de su meta).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT mes, nombre_vendedor, sede, ventas_mes, cumplimiento_pct
# MAGIC FROM cumplimiento_metas
# MAGIC QUALIFY row_number() OVER (PARTITION BY mes ORDER BY ventas_mes DESC) = 1   -- el n.º 1 de cada mes
# MAGIC ORDER BY mes;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 14 · Comparar con el periodo anterior (`LAG`)
# MAGIC *«¿Cuánto crecieron o cayeron las ventas frente al mes anterior?»*
# MAGIC - La CTE `mensual` calcula las ventas de cada mes.
# MAGIC - `lag(ventas_netas) OVER (ORDER BY mes)`: trae el valor de la **fila anterior** (el mes anterior). En enero no hay anterior: `NULL`.
# MAGIC - `variacion_pct` = (mes actual ÷ mes anterior − 1) × 100. Positivo = creció; negativo = cayó.
# MAGIC
# MAGIC **Qué esperar:** mayor alza en marzo (+56,5 %), mayor caída en junio (−32,8 %).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mensual AS (
# MAGIC   SELECT date_trunc('MONTH', fecha_venta) AS mes, sum(valor_neto) AS ventas_netas
# MAGIC   FROM ventas WHERE estado = 'Completada'
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT mes, ventas_netas,
# MAGIC        lag(ventas_netas) OVER (ORDER BY mes)                                        AS mes_anterior,  -- fila anterior
# MAGIC        round((ventas_netas / lag(ventas_netas) OVER (ORDER BY mes) - 1) * 100, 1)  AS variacion_pct
# MAGIC FROM mensual
# MAGIC ORDER BY mes;

# COMMAND ----------

# MAGIC %md
# MAGIC **Gráfico.** `dropna()` quita las filas con `NULL` (enero no tiene variación). Las barras hacia abajo son caídas.

# COMMAND ----------

pdf = _sqldf.toPandas().dropna()                                # quitar enero (sin mes anterior)
pdf["variacion_pct"] = pdf["variacion_pct"].astype(float)
pdf["mes"] = pd.to_datetime(pdf["mes"]).dt.strftime("%Y-%m")    # etiqueta corta: 2025-02, 2025-03…

pdf.plot(kind="bar", x="mes", y="variacion_pct",
         title="Variación de ventas frente al mes anterior (%)", legend=False, rot=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 15 · Acumulado del año (`SUM OVER`)
# MAGIC *«¿Vamos a llegar a la meta anual de 1.000 millones? ¿En qué mes la superamos?»*
# MAGIC - `sum(ventas_netas) OVER (ORDER BY mes ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`: suma desde la **primera fila**
# MAGIC   hasta la **fila actual** = total **acumulado** mes a mes.
# MAGIC - `1000000000 AS meta_anual`: un valor fijo en todas las filas, para dibujar la meta como una línea.
# MAGIC
# MAGIC **Qué esperar:** el acumulado supera la meta en **noviembre** (1.084 M) y cierra el año en 1.270 M.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mensual AS (
# MAGIC   SELECT date_trunc('MONTH', fecha_venta) AS mes, sum(valor_neto) AS ventas_netas
# MAGIC   FROM ventas WHERE estado = 'Completada'
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT mes, ventas_netas,
# MAGIC        sum(ventas_netas) OVER (ORDER BY mes
# MAGIC                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado,  -- total acumulado
# MAGIC        1000000000 AS meta_anual
# MAGIC FROM mensual
# MAGIC ORDER BY mes;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["Acumulado"]  = pdf["acumulado"].astype(float) / 1e6
pdf["Meta anual"] = pdf["meta_anual"].astype(float) / 1e6

pdf.plot(kind="line", x="mes", y=["Acumulado", "Meta anual"], marker="o",   # dos líneas
         title="Ventas acumuladas 2025 frente a la meta anual (millones de COP)")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 16 · Indicadores de calidad: devoluciones
# MAGIC *«Calidad quiere saber qué categoría tiene la mayor tasa de devoluciones.»*
# MAGIC `count_if(estado = 'Devuelta') / count(*) × 100` = porcentaje de ventas devueltas en cada categoría (aquí contamos **todas** las ventas, no solo las completadas).
# MAGIC
# MAGIC **Qué esperar:** Hogar 7,2 % · Oficina 6,9 % · Deportes 6,1 % · Tecnología 5,7 %.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT categoria,
# MAGIC        count(*)                                                 AS ventas,
# MAGIC        count_if(estado = 'Devuelta')                            AS devueltas,
# MAGIC        round(count_if(estado = 'Devuelta') / count(*) * 100, 1) AS tasa_devolucion_pct
# MAGIC FROM ventas_detalle
# MAGIC GROUP BY categoria
# MAGIC ORDER BY tasa_devolucion_pct DESC;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["tasa_devolucion_pct"] = pdf["tasa_devolucion_pct"].astype(float)

pdf.plot(kind="bar", x="categoria", y="tasa_devolucion_pct",
         title="Tasa de devolución por categoría (%)", legend=False, rot=0)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 17 · Participación (gráfico de torta)
# MAGIC *«¿Cómo pagan nuestros clientes?»*
# MAGIC
# MAGIC **Gráfico de torta.** `kind="pie"` muestra cómo se reparte un total:
# MAGIC - `y` = la columna con los valores; `labels` = los nombres de cada porción.
# MAGIC - `autopct="%1.1f%%"` escribe el porcentaje de cada porción con un decimal.
# MAGIC - `ylabel=""` quita el texto del eje vertical, que en una torta no aporta.
# MAGIC
# MAGIC **Qué esperar:** Tarjeta de crédito ≈ 49 % del dinero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT metodo_pago, count(*) AS ventas, sum(valor_neto) AS ventas_netas
# MAGIC FROM ventas
# MAGIC WHERE estado = 'Completada'
# MAGIC GROUP BY metodo_pago
# MAGIC ORDER BY ventas_netas DESC;

# COMMAND ----------

pdf = _sqldf.toPandas()
pdf["ventas_millones"] = pdf["ventas_netas"].astype(float) / 1e6

pdf.plot(kind="pie", y="ventas_millones", labels=pdf["metodo_pago"], autopct="%1.1f%%",
         title="Participación por método de pago", legend=False, ylabel="", figsize=(6, 6))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Necesidad 18 · Patrones por día de la semana
# MAGIC *«¿Qué días de la semana conviene reforzar el personal en tienda?»*
# MAGIC - `dayofweek(fecha)`: número del día (1 = domingo … 7 = sábado). Lo usamos para **ordenar**.
# MAGIC - `element_at(array('Dom', 'Lun', …), n)`: toma el elemento número `n` de la lista. Así el número del día pasa a su nombre en español.
# MAGIC - `GROUP BY ALL`: agrupa por el número y el nombre del día.
# MAGIC
# MAGIC **Qué esperar:** 7 filas, de domingo a sábado, con las ventas en tienda física de todo el año.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dayofweek(fecha_venta)                                                             AS num_dia,  -- 1 = domingo
# MAGIC        element_at(array('Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb'), dayofweek(fecha_venta)) AS dia,
# MAGIC        count(*)                                                                           AS ventas
# MAGIC FROM ventas
# MAGIC WHERE canal = 'Tienda física' AND estado = 'Completada'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY num_dia;

# COMMAND ----------

pdf = _sqldf.toPandas()

pdf.plot(kind="bar", x="dia", y="ventas",
         title="Ventas en tienda física por día de la semana (2025)", legend=False, rot=0)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Tablero final con Python
# MAGIC Combinamos cuatro gráficos en una sola figura:
# MAGIC - `spark.sql("""…""")` ejecuta una consulta desde Python y devuelve un DataFrame de Spark; `.toPandas()` lo pasa a pandas.
# MAGIC   Las comillas triples `"""` permiten escribir la consulta en varias líneas.
# MAGIC - `fig, ejes = plt.subplots(2, 2, figsize=(14, 8))`: crea una figura con una cuadrícula de **2 × 2** gráficos. `ejes[fila, columna]` es cada espacio.
# MAGIC - `pdf.plot(…, ax=ejes[0, 0])`: el parámetro `ax` dice **en qué espacio** se dibuja cada gráfico.
# MAGIC - `fig.suptitle(…)`: título general · `plt.tight_layout()`: acomoda los espacios para que no se encimen.
# MAGIC
# MAGIC También puedes usar las visualizaciones integradas, sin código: en cualquier resultado SQL pulsa **+ › Visualization** o **+ › Data Profile**.

# COMMAND ----------

# 1. Una consulta por gráfico (los montos se dividen entre 1e6 directamente en SQL)
mensual = spark.sql("""
    SELECT date_trunc('MONTH', fecha_venta) AS mes, sum(valor_neto) / 1e6 AS millones
    FROM comercializadora.sistema_ventas.ventas WHERE estado = 'Completada'
    GROUP BY ALL ORDER BY mes""").toPandas()
categorias = spark.sql("""
    SELECT categoria, sum(utilidad) / 1e6 AS millones
    FROM comercializadora.sistema_ventas.ventas_detalle WHERE estado = 'Completada'
    GROUP BY ALL ORDER BY millones DESC""").toPandas()
ciudades = spark.sql("""
    SELECT ciudad_cliente, sum(valor_neto) / 1e6 AS millones
    FROM comercializadora.sistema_ventas.ventas_detalle WHERE estado = 'Completada'
    GROUP BY ALL ORDER BY millones""").toPandas()
metas = spark.sql("""
    SELECT nombre_vendedor, count_if(cumplimiento_pct >= 100) AS meses
    FROM comercializadora.sistema_ventas.cumplimiento_metas
    GROUP BY ALL ORDER BY meses""").toPandas()

# 2. DECIMAL -> número
for tabla in (mensual, categorias, ciudades):
    tabla["millones"] = tabla["millones"].astype(float)

# 3. Figura con 2 x 2 espacios; cada gráfico va en su espacio con ax=
fig, ejes = plt.subplots(2, 2, figsize=(14, 8))
mensual.plot(kind="line", x="mes", y="millones", marker="o", legend=False, ax=ejes[0, 0], title="Ventas netas por mes (M COP)")
categorias.plot(kind="bar", x="categoria", y="millones", legend=False, rot=0, ax=ejes[0, 1], title="Utilidad por categoría (M COP)")
ciudades.plot(kind="barh", x="ciudad_cliente", y="millones", legend=False, ax=ejes[1, 0], title="Ventas por ciudad del cliente (M COP)")
metas.plot(kind="barh", x="nombre_vendedor", y="meses", legend=False, ax=ejes[1, 1], title="Meses con meta cumplida")

fig.suptitle("Comercializadora Andina · Tablero de ventas 2025", fontsize=16, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · Retos y limpieza
# MAGIC **Retos** (resuélvelos tú, usando lo visto):
# MAGIC 1. ¿Cuál es el producto más vendido (en unidades) de cada categoría? *(pista: `QUALIFY` + `row_number`)*
# MAGIC 2. ¿Qué ciudad tiene el ticket promedio más alto en ventas **en línea**? Grafícalo con `pdf.plot(kind="bar", …)`.
# MAGIC 3. Crea con CTAS la tabla oro `ventas_por_ciudad_mes` (`CLUSTER BY AUTO`, comentario, `TBLPROPERTIES`) y luego agrega con `ALTER TABLE` la descripción de cada columna.
# MAGIC 4. Haz un `UPDATE` para marcar como `'Devuelta'` la venta `100010` y revisa el historial de la tabla `ventas`.
# MAGIC
# MAGIC **Limpieza (opcional):** si quieres borrar todo lo de esta clase, quita los `--` de las dos primeras líneas de la celda siguiente y ejecútala.
# MAGIC `CASCADE` borra el catálogo o el schema con todo su contenido. Las tablas gestionadas se pueden recuperar durante 7 días con `UNDROP`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP CATALOG IF EXISTS comercializadora CASCADE;
# MAGIC -- DROP SCHEMA  IF EXISTS workspace.practica_ui CASCADE;
# MAGIC SELECT 'Limpieza comentada: quita los -- de las líneas de arriba para borrar todo' AS aviso;