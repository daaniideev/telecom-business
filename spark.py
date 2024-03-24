# encoding: utf-8 

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark_IMF_Big_Data').getOrCreate()

# CASO 1:
df_clientes = spark.read.table("sector_telecomunicaciones.df_clientes")
df_facturas_mes_ant = spark.read.table("sector_telecomunicaciones.df_facturas_mes_ant")
df_num_contr_por_cli = df_facturas_mes_ant.groupBy("id_cliente").agg(count("*").alias('num_contratos')).filter(col("num_contratos") > 1)
df_nombre_clientes = df_num_contr_por_cli.join(df_clientes, "id_cliente").select("nombre")
df_nombre_clientes.show()

# CASO 2:
df_facturas_mes_actual = spark.read.table("df_facturas_mes_actual")
df_facturas_mes_ant = spark.read.table("df_facturas_mes_ant")
# Primero, obtengo un DF exactamente igual que 'df_facturas_mes_actual' pero con una columna extra 'flag_match' que tendrá dos posibles valores:
# - '1' si el contrato de ese cliente en ese registro ya existía el mes pasado.
# - 'null' si el contrato de ese cliente en ese registro es nuevo de este mes.
df_facturas_dos_meses = df_facturas_mes_actual.join(df_facturas_mes_ant.withColumn("flag_match", lit(1)).select("id_cliente", "id_oferta", "importe", "flag_match"), ["id_cliente", "id_oferta", "importe"], "left")
# Finalmente, al DF anterior le creo una nueva columna 'importe_dto' que contenga un 7% menos del valor de la columna 'importe' si la columna 'flag_match' es '1'. Si es 'null', le doy el mismo valor que el de la columna 'importe':
df_facturas_dos_meses = df_facturas_dos_meses.withColumn("importe_dto", when(col("flag_match") == 1, (col("importe") * 0.93).cast(DecimalType(17,2))).otherwise(col("importe"))).select("id_cliente", "id_oferta", "importe", "importe_dto","fecha").orderBy(asc("id_cliente"), desc("importe"))
df_facturas_dos_meses.show(n=40)

# CASO 3:
df_ofertas = spark.read.table("df_ofertas");
df_facturas_mes_actual = spark.read.table("df_facturas_mes_actual");
# Primero, filtro el DF 'df_ofertas' para seleccionar aquellos cuyo campo 'descripcion' contenga la cadena 'datos ilimitados':
df_ofertas_filter = df_ofertas.filter(col("descripcion").contains("datos ilimitados"))
# Segundo, obtengo un DF exactamente igual que 'df_facturas_dos_meses' (obtenido en el ejercicio anterior) pero con una columna extra 'flag_match' que tendrá dos posibles valores:
# - '1' si el campo 'descripcion' del contrato de ese cliente contiene el string 'datos ilimitados'.
# - 'null' si el campo 'descripcion' del contrato de ese cliente NO contiene el string 'datos ilimitados'.
df_fact_mes_act_lim = df_facturas_dos_meses.join(df_ofertas_filter.withColumn("match_flag", lit(1)).select("id_oferta", "match_flag"), ["id_oferta"], "left")
# Finalmente, aplico un 15% de aumento a los valores de la columna 'importe_dto' y 'importe' si la columna 'flag_match' es '1'. Si es 'null', no aplico ningun cambio:
df_fact_mes_act_lim = df_fact_mes_act_lim.withColumn("importe_dto", when(col("match_flag") == 1, (col("importe_dto")*1.15).cast(DecimalType(17,2))).otherwise(col("importe_dto"))) \
    .withColumn("importe", when(col("match_flag") == 1, (col("importe")*1.15).cast(DecimalType(17,2))).otherwise(col("importe"))) \
    .select("id_cliente", "id_oferta", "importe", "importe_dto", "fecha").orderBy("id_cliente");
df_fact_mes_act_lim.show(n=40)

# CASO 4:
df_clientes = spark.read.table("df_clientes");
df_consumos_diarios = spark.read.table("df_consumos_diarios");
# Creo el DF 'df_clientes_agr' que tiene a los clientes agrupados por 'grupo_edad':
df_clientes_agr = df_clientes.withColumn("grupo_edad", when(col("edad") < 26, lit(1))
                                         .when(col("edad").between(26,40),lit(2))
                                         .when(col("edad").between(41,65),lit(3))
                                         .otherwise(lit(4))).select("id_cliente", "nombre", "grupo_edad")
# Calculo la media de los consumos diarios:
df_cons_diar_agr = df_consumos_diarios.join(df_clientes_agr, ["id_cliente"]).select("grupo_edad", "consumo_datos_MB", "sms_enviados", "minutos_llamadas_movil", "minutos_llamadas_fijo").groupBy("grupo_edad").agg(
    avg("consumo_datos_MB").cast(DecimalType(17,2)).alias("media consumo de datos"),
    avg("sms_enviados").cast(DecimalType(17,2)).alias("media SMS enviados"),
    avg("minutos_llamadas_movil").cast(DecimalType(17,2)).alias("media minutos movil"),
    avg("minutos_llamadas_fijo").cast(DecimalType(17,2)).alias("media llamadas fijos")).orderBy("grupo_edad");
df_cons_diar_agr.show()

# CASO 5:
df_consumos_diarios = spark.read.table("df_consumos_diarios")
# Creo el DF 'df_consumos_diarios_aux', que es identico al DF 'df_consumos_diarios' pero añadiendo la columna 'sexo'
df_consumos_diarios_aux = df_consumos_diarios.join(df_clientes.select("sexo", "id_cliente"), ["id_cliente"]) 

# Al DF 'df_consumos_diarios_aux', le añado la columna 'dia', que contiene el dia de la semana (1 lunes, ..., 7 domingo). Filtro el DF por los dias 5, 6 i 7.
df_consumos_diarios_aux = df_consumos_diarios_aux.withColumn("dia", when(dayofweek(col("fecha")) - 1 == 0, 7).otherwise(dayofweek(col("fecha")) - 1)).select("sexo", "consumo_datos_MB", "dia", "fecha", "minutos_llamadas_movil").filter(col("dia").isin([5, 6, 7]))

# Agrupo el DF por 'sexo' i calculo el total de consumo de datos i minuto de llamadas a movil:
df_consumos_diarios_aux = df_consumos_diarios_aux.groupBy("sexo").agg(sum("consumo_datos_MB").alias("total_datos_moviles_finde"), sum("minutos_llamadas_movil").alias("total_mins_movil_finde")).select("sexo", "total_datos_moviles_finde","total_mins_movil_finde")
df_consumos_diarios_aux.show()

# CASO 6:
df_consumos_diarios = spark.read.table("df_consumos_diarios")
df_clientes = spark.read.table("df_clientes")
window_grupo_edad = Window.partitionBy("grupo_edad")
# Filtro el DF 'df_consumos_diarios' con los 15 primeros dias del mes, solo con las columnas "id_cliente", "consumo_datos_MB", "sms_enviados".
df_consumos_diarios_aux = df_consumos_diarios.select("id_cliente", "consumo_datos_MB", "sms_enviados").filter(col("fecha").substr(9,2) <= 15)
# Hago join con el DF 'df_clientes' para obtener el nombre y la edad del cliente:
df_consumos_diarios_aux = df_consumos_diarios_aux.join(df_clientes.select("id_cliente", "edad", "nombre"), "id_cliente")
# Agrupo el DF anterior por 'id_cliente' y calculo el total de consumo de datos y el maximo número de sms enviados en un día:
df_consumos_diarios_aux = df_consumos_diarios_aux.groupBy("id_cliente").agg(sum("consumo_datos_MB").alias("datos_moviles_total_15"), max("nombre").alias("nombre"), max("sms_enviados").alias("max_sms_enviados_15"), max("edad").alias("edad")).orderBy("id_cliente")
# Agrupo el DF anterior por 'grupo_edad':
df_consumos_diarios_aux = df_consumos_diarios_aux.withColumn("grupo_edad", when(col("edad") < 26, lit(1))
                                         .when(col("edad").between(26,40),lit(2))
                                         .when(col("edad").between(41,65),lit(3))
                                         .otherwise(lit(4)))
df_consumos_diarios_aux = df_consumos_diarios_aux.withColumn("datos_moviles_max", max("datos_moviles_total_15").
                                                                        over(window_grupo_edad)).filter(col("datos_moviles_total_15")n == col("datos_moviles_max")).select("nombre", "edad", "grupo_edad", "datos_moviles_total_15", "max_sms_enviados_15").orderBy("grupo_edad")
df_consumos_diarios_aux.show()


# CASO 7:

df_facturas_mes_ant = spark.read.table("df_facturas_mes_ant")
df_facturas_mes_actual = spark.read.table("df_facturas_mes_actual")
df_consumos_diarios = spark.read.table("df_consumos_diarios")
df_clientes = spark.read.table("df_clientes")

# Obtengo un DF igual que 'df_facturas_mes_actual' pero solo con los clientes que son nuevos: 
df_facturas_mes_actual_nuev = df_facturas_mes_actual.join(df_facturas_mes_ant, ["id_cliente"], "left_anti")

# Al DF anterior, le añado la columna del nombre y de la edad del cliente: 
df_facturas_mes_actual_nuev = df_facturas_mes_actual_nuev.join(df_clientes.select("id_cliente", "nombre", "edad"), "id_cliente")

# Al DF anterior, le elimino la columna 'id_oferta' y a la columna 'importe' obtengo la suma (para los clientes con dos ofertas contratadas:
df_facturas_mes_actual_nuev = df_facturas_mes_actual_nuev.groupBy("id_cliente").agg(sum("importe").alias("importe"), max("fecha").alias("fecha"), max("nombre").alias("nombre"), max("edad").alias("edad"))

# Al DF anterior, le añado la información de consumos diarios: 
df_facturas_mes_actual_nuev = df_facturas_mes_actual_nuev.join(df_consumos_diarios, "id_cliente")

# Agrupo el DF anterior por 'id_cliente' y obtengo el total de llamadas que hace cada uno de los clientes nuevos: 
df_facturas_mes_actual_nuev = df_facturas_mes_actual_nuev.groupBy("id_cliente") \
    .agg((sum("minutos_llamadas_movil") + sum("minutos_llamadas_fijo")).alias("total_minutos"),
         max("importe").alias("importe_total_mes_actual"),
         max("nombre").alias("nombre_cliente_nuevo"),
         max("edad").alias("edad")) \
    .orderBy(asc("total_minutos")) \
    .select("nombre_cliente_nuevo", "importe_total_mes_actual", "edad", "total_minutos")
df_facturas_mes_actual_nuev.show()

# CASO 8:

df_facturas_mes_ant = spark.read.table("df_facturas_mes_ant")
df_facturas_mes_actual = spark.read.table("df_facturas_mes_actual")
df_consumos_diarios = spark.read.table("df_consumos_diarios")
df_clientes = spark.read.table("df_clientes")

# Obtengo un DF igual que 'df_facturas_mes_actual' pero solo con los clientes que no son nuevos:
df_facturas_mes_actual_cons = (
    df_facturas_mes_actual.join(df_facturas_mes_ant, ["id_cliente"])
    .select("id_cliente")
    .dropDuplicates()
)
df_facturas_mes_actual_cons.show()

# Obtengo el DF 'df_consumos_diarios' pero solo con los clientes que no son nuevos, solo muestro las columnas "id_cliente", "sms_enviados", "fecha":
df_consumos_diarios_sms = df_consumos_diarios.select(
    "id_cliente", "sms_enviados", "fecha"
).join(df_facturas_mes_actual_cons, "id_cliente")

# Ahora, del DF anterior, selecciono solo los que tengan el valor de 'sms_enviados' en 0:
df_consumos_diarios_sms = df_consumos_diarios_sms.filter(col("sms_enviados") == 0)

# Agrupo por cliente y cuento en cuantos registros aparece cada ic_cliente (es decir, cuantos dias ha enviado 0 sms cada cliente):
df_consumos_diarios_sms = (
    df_consumos_diarios_sms.groupBy("id_cliente")
    .agg(count("id_cliente").alias("dias_sin_sms"))
    .join(df_clientes.select("id_cliente", "edad"), "id_cliente")
    .orderBy("id_cliente")
)

# Finalmente, muestro la columna 'nombre' y elimino la columna 'id_cliente':
df_consumos_diarios_sms = df_consumos_diarios_sms.join(
    df_clientes.select("id_cliente", "nombre"), "id_cliente"
).select("nombre", "edad", "dias_sin_sms")

df_consumos_diarios_sms.show(n=30)

# CASO 9:

df_facturas_mes_ant = spark.read.table("df_facturas_mes_ant")
df_facturas_mes_actual = spark.read.table("df_facturas_mes_actual")
df_consumos_diarios = spark.read.table("df_consumos_diarios")
df_clientes = spark.read.table("df_clientes")

# Obtenemos los clientes que tienen solo una oferta contratada en la compañía el mes de agosto:
df_facturas_mes_actual_aux = df_facturas_mes_actual.groupBy("id_cliente").agg(count("id_oferta").alias("num_contratos")).filter(col("num_contratos")==1).select("id_cliente").orderBy("id_cliente")
window = Window.partitionBy("group_by_col")

# 1. Obtenemos la suma de todos los dias de cada uno de los 4 consumos para cada uno de los clientes obtenidos en en el DF anterior:
df_consumos_diarios_aux = df_consumos_diarios.join(df_facturas_mes_actual_aux, "id_cliente") \
    .groupBy("id_cliente") \
    .agg( \
        sum("consumo_datos_MB").alias("total_datos"), \
        sum("sms_enviados").alias("total_sms"), \
        sum("minutos_llamadas_movil").alias("total_llamadas_movil"), \
        sum("minutos_llamadas_fijo").alias("total_llamadas_fijo")).orderBy("id_cliente")
# 2. Al DF anterior, creo las nuevas columnas 'datos_moviles_0_1', 'sms_0_1', 'total_llamadas_movil' y 'llamadas_fijo_0_1'
# Creo una columna 'dummy' con valor '1' que utilizaré para agrupar por esa misma columna y poder calcular el valor máximo de los 4 indicadores:
df_consumos_diarios_aux = df_consumos_diarios_aux.withColumn("group_by_col", lit(1))

# Creo 4 columnas que contengan el valor máximo de cada uno de los 4 indicadores:
df_consumos_diarios_aux = df_consumos_diarios_aux \
.withColumn("datos_max", max("total_datos").over(window)) \
.withColumn("sms_max", max("total_sms").over(window)) \
.withColumn("movil_max", max("total_llamadas_movil").over(window)) \
.withColumn("fijo_max", max("total_llamadas_fijo").over(window))

# Creo las 4 columnas 'datos_moviles_0_1', 'sms_0_1', 'llamadas_movil_0_1' y 'llamadas_fijo_0_1' y doy valor '1' y calculo el tanto por 1:
df_consumos_diarios_aux = df_consumos_diarios_aux \
.withColumn("datos_moviles_0_1", (col("total_datos")/col("datos_max"))) \
.withColumn("sms_0_1", (col("total_sms")/col("sms_max"))) \
.withColumn("llamadas_movil_0_1", (col("total_llamadas_movil")/col("movil_max"))) \
.withColumn("llamadas_fijo_0_1", (col("total_llamadas_fijo")/col("fijo_max"))) 

# 3. Multiplico por la ponderación correspondiente: 
df_consumos_diarios_aux = df_consumos_diarios_aux \
.withColumn("datos_moviles_0_1", col("datos_moviles_0_1")*.3 ) \
.withColumn("sms_0_1", col("sms_0_1")*.1 ) \
.withColumn("llamadas_movil_0_1", col("llamadas_movil_0_1")*.4 ) \
.withColumn("llamadas_fijo_0_1", col("llamadas_fijo_0_1")*.2 ) \

# 4. Obtengo la suma de los tres coeficientes: 
df_consumos_diarios_aux = df_consumos_diarios_aux \
.withColumn("coeficiente_cliente", (col("datos_moviles_0_1") + col("sms_0_1") + col("llamadas_movil_0_1") + col("llamadas_fijo_0_1")).cast(DecimalType(17,2))) 

# 5. Muestro el DF:
# Obtengo los nombres y edades y me quedo solo con las tres columnas que me piden:
df_consumos_diarios_aux = df_consumos_diarios_aux.join(df_clientes, "id_cliente").select("nombre", "edad", "coeficiente_cliente").orderBy(desc("coeficiente_cliente"))
df_consumos_diarios_aux.show()

# CASO 10:

df_facturas_mes_ant = spark.read.table("df_facturas_mes_ant")
df_facturas_mes_actual = spark.read.table("df_facturas_mes_actual")
df_consumos_diarios = spark.read.table("df_consumos_diarios")
df_clientes = spark.read.table("df_clientes")

window_grupo_edad = Window.partitionBy("grupo_edad").orderBy(asc("fecha"), desc("consumo_datos_MB"))

# Hacemos que el DF df_clientes tenga la columna 'grupo_edad'
df_clientes = df_clientes.withColumn("grupo_edad", when(col("edad") < 26, lit(1))
                                         .when(col("edad").between(26,40),lit(2))
                                         .when(col("edad").between(41,65),lit(3))
                                         .otherwise(lit(4))).select("id_cliente", "nombre", "grupo_edad")
# Creo DF que contenga el ID y el grupo edad de los 3 clientes que mas datos moviles consumen de cada grupo edad:
df_cli_que_mas_consumen = df_consumos_diarios.select("id_cliente", "consumo_datos_MB", "fecha").join(df_clientes, "id_cliente").orderBy("id_cliente", "grupo_edad") \
    .groupBy("id_cliente") \
    .agg(sum("consumo_datos_MB").alias("total_datos"), \
        max("grupo_edad").alias("grupo_edad")).orderBy("id_cliente")
# Creo DF con los tres clientes que mas datos gastan del grupo edad 1
df_cli_que_mas_consumen_1 = df_cli_que_mas_consumen.filter(col("grupo_edad") == 1).orderBy(desc("total_datos")).limit(3)
# Creo DF con los tres clientes que mas datos gastan del grupo edad 2
df_cli_que_mas_consumen_2 = df_cli_que_mas_consumen.filter(col("grupo_edad") == 2).orderBy(desc("total_datos")).limit(3)
# Creo DF con los tres clientes que mas datos gastan del grupo edad 3
df_cli_que_mas_consumen_3 = df_cli_que_mas_consumen.filter(col("grupo_edad") == 3).orderBy(desc("total_datos")).limit(3)
# Creo DF con los tres clientes que mas datos gastan del grupo edad 4
df_cli_que_mas_consumen_4 = df_cli_que_mas_consumen.filter(col("grupo_edad") == 4).orderBy(desc("total_datos")).limit(3)
# Creo la columna que tiene la suma de los datos acomulados
df_cli_que_mas_consumen_grp = df_cli_que_mas_consumen_1.union(df_cli_que_mas_consumen_2).union(df_cli_que_mas_consumen_3).union(df_cli_que_mas_consumen_4).select("id_cliente", "grupo_edad")

df_res_1 = df_consumos_diarios.join(df_cli_que_mas_consumen_1, "id_cliente") \
    .withColumn("MB_ACUMULADAS", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
    .withColumn("MAX_MB", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
        .select("id_cliente", "consumo_datos_MB", "MB_ACUMULADAS", "fecha", "grupo_edad", "MAX_MB") \
    .orderBy("grupo_edad", "fecha") \
    .withColumn("alcanza_20", when(col("MB_ACUMULADAS") >= 1024 * 20, lit(1)).otherwise(lit(0))) \
    .filter(col("alcanza_20") == 1) \
    .limit(1)

df_res_2 = df_consumos_diarios.join(df_cli_que_mas_consumen_2, "id_cliente") \
    .withColumn("MB_ACUMULADAS", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
    .withColumn("MAX_MB", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
        .select("id_cliente", "consumo_datos_MB", "MB_ACUMULADAS", "fecha", "grupo_edad", "MAX_MB") \
    .orderBy("grupo_edad", "fecha") \
    .withColumn("alcanza_20", when(col("MB_ACUMULADAS") >= 1024 * 20, lit(1)).otherwise(lit(0))) \
    .filter(col("alcanza_20") == 1) \
    .limit(1)

df_res_3 = df_consumos_diarios.join(df_cli_que_mas_consumen_3, "id_cliente") \
    .withColumn("MB_ACUMULADAS", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
    .withColumn("MAX_MB", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
        .select("id_cliente", "consumo_datos_MB", "MB_ACUMULADAS", "fecha", "grupo_edad", "MAX_MB") \
    .orderBy("grupo_edad", "fecha") \
    .withColumn("alcanza_20", when(col("MB_ACUMULADAS") >= 1024 * 20, lit(1)).otherwise(lit(0))) \
    .filter((col("alcanza_20") == 1) | (col("MAX_MB") <1024 * 20 )) \
    .limit(1)

df_res_4 = df_consumos_diarios.join(df_cli_que_mas_consumen_4, "id_cliente") \
    .withColumn("MB_ACUMULADAS", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
    .withColumn("MAX_MB", sum("consumo_datos_MB").over(window_grupo_edad.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
        .select("id_cliente", "consumo_datos_MB", "MB_ACUMULADAS", "fecha", "grupo_edad", "MAX_MB") \
    .orderBy("grupo_edad", "fecha") \
    .withColumn("alcanza_20", when(col("MB_ACUMULADAS") >= 1024 * 20, lit(1)).otherwise(lit(0))) \
    .filter((col("alcanza_20") == 1) | (col("MAX_MB") <1024 * 20 )) \
    .limit(1)
    
df_res = df_res_1.union(df_res_2).union(df_res_3).union(df_res_4) \
    .select("grupo_edad", "fecha", "MAX_MB", "alcanza_20") \
    .withColumnRenamed("fecha", "fecha_20_GB") \
    .withColumnRenamed("MAX_MB", "datos_moviles_total_grupo_3_clientes") \
    .withColumn("fecha_20_GB", when(col("alcanza_20") == lit(0), lit(None)).otherwise(col("fecha_20_GB"))) \
    .select("grupo_edad", "fecha_20_GB", "datos_moviles_total_grupo_3_clientes") 

df_res.show()