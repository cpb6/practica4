""" python/python3 practica4-cristina.py """
import json
import os

from pyspark import SparkContext


def mapper_movements(line):
    """
    Maps the fields of a JSON record representing a bike usage to desired fields.
    Args:
        line: JSON record representing a bike usage

    Returns:
        Tuple of user type, user day code, start station, end station, travel time, date, hour, and age range.
    """
    data = json.loads(line)
    user = data['user_type']
    user_day = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    date = data['unplug_hourTime']['$date'][0:10]
    hour = data['unplug_hourTime']['$date'][11:19]
    time = data['travel_time']
    age = data['ageRange']
    return user, user_day, start, end, time, date, hour, age


def get_spark_rdd_objects_from_dir(directory: str, spark_context):
    """
    Loads Spark RDD objects from JSON files in a directory using predefined mappers.

    Args:
        directory: Directory path containing JSON files

    Returns:
        Dictionary mapping filenames to corresponding RDD objects.
    """
    rdds_objects = {}
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            name = filename
            rdd_dataset_keys = list(RDD_DATASET_MAPPERS.keys())
            for k in rdd_dataset_keys:
                if k in filename:
                    rdds_objects[name] = spark_context.textFile(os.path.join(directory, filename)).map(
                        RDD_DATASET_MAPPERS[k])
            print(name)
    return rdds_objects


def mapper_stations(line):
    """
    Maps the fields of a JSON record representing a bike station to desired fields.
    Args:
        line: JSON record representing a bike station

    Returns:
        Dictionary containing day, hour, and station information.
    """
    data = json.loads(line)
    day = data['_id'][0:10]
    hour = data['_id'][11:27]
    station = data['stations']
    return {"day": day, "hour": hour, "station": station}


def datos_estaciones(lista):
    datos = {}
    for station in lista:
        datos[station['number']] = {}
        datos[station['number']]['id'] = station['id']
        datos[station['number']]['latitude'] = station['latitude']
        datos[station['number']]['longitude'] = station['longitude']
        datos[station['number']]['name'] = station['name']
        datos[station['number']]['total_bases'] = station['total_bases']
    return datos


# De todas las estaciones que hay en todos los datos vamos a intentar quedarnos
# tan solo con los que son de la estacion que queremos
def estaciones(lista):
    filtro = []
    # = json.loads(lista)
    for estacion in lista:
        if estacion['number'] == origen:
            filtro.append(estacion)
    return filtro


if __name__ == '__main__':
    sc = SparkContext()

    # constants
    DATASET_DIRECTORY = "public_data/bicimad"

    RDD_DATASET_MAPPERS = {
        "201901_movements.json": mapper_movements,
        "201902_movements.json": mapper_movements,
        "201906_movements.json": mapper_movements,
        "201901_stations.json": mapper_stations,
    }

    rdds_objects = get_spark_rdd_objects_from_dir(DATASET_DIRECTORY, sc)

    # INFORMATION
    print(f"All RDD objects: {rdds_objects}")

    rdd_movements = {key: value for key, value in rdds_objects.items() if "movements" in key}
    rdd_stations = {key: value for key, value in rdds_objects.items() if "stations" in key}

    print(f"All RDD objects - movements: {rdd_movements}")
    print(f"All RDD objects - stations: {rdd_stations}")

    print("=============== 201901_movements.json ==========")
    print(rdd_movements["201901_movements.json"].take(1))

    print("=============== 201902_movements.json ==========")
    print(rdd_movements["201902_movements.json"].take(1))

    print("=============== 201906_movements.json ==========")
    print(rdd_movements["201906_movements.json"].take(1))

    print("=============== 201901_stations.json ===========")
    print(rdd_stations["201901_stations.json"].take(1))

    # PROBLEMA 1
    print("====================================================================================================")
    print("PROBLEMA 1")
    problema1_doc = """Analizamos los tipos de usuarios. Queremos saber que tipo de usuarios utilizan más Bicimad. Para ello analizamos 
    la cantidad de usuarios de cada tipo. Utilizamos varios meses de 2019, para poder comprobar si es algo casual o no."""
    print(problema1_doc)

    print('rdd_movements["201901_movements.json"].countByKey()')
    print(rdd_movements["201901_movements.json"].countByKey())

    print('rdd_movements["201902_movements.json"].countByKey()')
    print(rdd_movements["201902_movements.json"].countByKey())

    print('rdd_movements["201906_movements.json"].countByKey()')
    print(rdd_movements["201906_movements.json"].countByKey())

    problema1_conclusiones = """
    Conclusiones
    ============ 
    Podemos ver como en todos los meses la mayor parte de los usuarios son del tipo 1. Los números nos 
    indican: 
    0: No se ha podido determinar el tipo de usuario 
    1: Usuario anual (poseedor de un pase anual) 
    2: Usuario ocasional 
    3: Trabajador de la empresa Por tanto podemos concluir que la mayor parte de los usuarios de Bicimad entre Enero de 2019 y Junio de 2019 son usuarios anuales."""
    print(problema1_conclusiones)

    estudio_caso_i = """
    Estudio de los usuarios tipo 1
    ==============================
    Una vez hemos concluido cuales son los usuarios habituates de Bicimad, vamos a estudiar únicamente estos:
    """
    print(estudio_caso_i)

    selected_type = 1
    rdd_users = rdd_movements["201901_movements.json"].filter(lambda x: x[0] == selected_type).map(
        lambda x: (x[1], tuple(x[2:])))
    print(rdd_users)

    print("rdd_users.count()")
    print(rdd_users.count())

    print("rdd_users.countByKey()")
    # print(rdd_users.countByKey())

    print("rdd_users.take(1)")
    print(rdd_users.take(1))

    # PROBLEMA 2 Usuarios por grupos de edades
    print("====================================================================================================")
    print("PROBLEMA 2")
    problema2_doc = """Queremos analizar el uso de bicimad en función de la edad. 
    0: No se ha podido determinar el rango de edad del usuario. 
    1: El usuario tiene entre 0 y 16 años. 
    2: El usuario tiene entre 17 y 18 años. 
    3: El usuario tiene entre 19 y 26 años. 
    4: El usuario tiene entre 27 y 40 años. 
    5: El usuario tiene entre 41 y 65 años. 
    6: El usuario tiene 66 años o más."""
    print(problema2_doc)

    # EDADES ENERO
    selected_age = 0
    rdd_ages = rdd_movements["201901_movements.json"].filter(lambda x: x[7] == selected_age).map(
        lambda x: (x[0], tuple(x[1:7])))
    print(rdd_ages.count())

    print("1: El usuario tiene entre 0 y 16 años.")
    selected_age = 1
    print(rdd_ages.count())

    print("2: El usuario tiene entre 17 y 18 años.")
    selected_age = 2
    print(rdd_ages.count())

    print("3: El usuario tiene entre 19 y 26 años.")
    selected_age = 3
    print(rdd_ages.count())

    print("4: El usuario tiene entre 27 y 40 años.")
    selected_age = 4
    print(rdd_ages.count())

    print("5: El usuario tiene entre 41 y 65 años.")
    selected_age = 5
    print(rdd_ages.count())

    print("6: El usuario tiene 66 años o más.")
    selected_age = 6
    print(rdd_ages.count())

    problema2_conclusiones_parciales = """
    Conclusiones Parciales
    ====================== 
    En enero de 2019 el grupo de edad con un rango determinado que más utilizó bicimad fue el grupo 4 (entre 27 y 40 años) 
    """
    print(problema2_conclusiones_parciales)

    # Edades resto de meses.
    print("""
    Ahora estudiamos la cantidad de usuario que usaron Bicimad el resto de meses y pertenecen a este grupo de edad.
    """)
    print("201902_movements")

    print("1: El usuario tiene entre 0 y 16 años.")
    selected_age = 1
    rdd_ages02 = rdd_movements["201902_movements.json"].filter(lambda x: x[7] == selected_age).map(
        lambda x: (x[0], tuple(x[1:7])))
    print(rdd_ages02.count())

    print("2: El usuario tiene entre 17 y 18 años.")
    selected_age = 2
    print(rdd_ages02.count())

    print("3: El usuario tiene entre 19 y 26 años.")
    selected_age = 3
    print(rdd_ages02.count())

    print("4: El usuario tiene entre 27 y 40 años.")
    selected_age = 4
    print(rdd_ages02.count())

    print("5: El usuario tiene entre 41 y 65 años.")
    selected_age = 5
    print(rdd_ages02.count())

    print("6: El usuario tiene 66 años o más.")
    selected_age = 6
    print(rdd_ages02.count())

    print("201906_movements")

    print("1: El usuario tiene entre 0 y 16 años.")
    selected_age = 1
    rdd_ages03 = rdd_movements["201906_movements.json"].filter(lambda x: x[7] == selected_age).map(
        lambda x: (x[0], tuple(x[1:7])))
    print(rdd_ages03.count())

    print("2: El usuario tiene entre 17 y 18 años.")
    selected_age = 2
    print(rdd_ages03.count())

    print("3: El usuario tiene entre 19 y 26 años.")
    selected_age = 3
    print(rdd_ages03.count())

    print("4: El usuario tiene entre 27 y 40 años.")
    selected_age = 4
    print(rdd_ages03.count())

    print("5: El usuario tiene entre 41 y 65 años.")
    selected_age = 5
    print(rdd_ages03.count())

    print("6: El usuario tiene 66 años o más.")
    selected_age = 6
    print(rdd_ages03.count())

    problema2_conclusiones = """
    Conclusiones
    ====================== 
    El tipo de usuario según la edad de Bicimad tiene en mayoría entre 27 y 40 años. 
    """
    print(problema2_conclusiones)

    # PROBLEMA 4 Encontrar una bici en una estación a cierta hora
    print("====================================================================================================")
    print("PROBLEMA 4")
    problema4_doc = """ Encontrar una bici en una estación a cierta hora """
    print(problema4_doc)

    listEstacion = (rdd_stations["201901_stations.json"]).map(lambda x: datos_estaciones(x['station'])).take(1)

    dicEstaciones = listEstacion[0]

    print("Ejemplo - Estacion 6 ")
    print(dicEstaciones['5'])

    # Definimos la hora y el origen
    print("Definimos la hora y el origen")
    origen = '57'
    hora = '03:00:00'

    rddE_Sit_fil = rdd_stations["201901_stations.json"].map(
        lambda x: {"day": x["day"], "hour": x["hour"], "stations": estaciones(x["station"])})
    print(rddE_Sit_fil.take(3))

    problema4_conclusiones_parciales = """
    Conclusiones Parciales 
    ====================== 
    Nos fijamos que a las 2:50 había 
    12 bicis ancladas en la estación deseada ('dock_bikes'). Por tanto, es muy probable que haya bicis disponibles para 
    realizar nuestro viaje."""
    print(problema4_conclusiones_parciales)

    print("====== Análisis del tiempo de los viajes ======== ")

    # Nos quedamos con los tiempos de cada viaje.
    rdd_time = rdd_users.map(lambda x: (x[1][2]))

    # Queremos saber los viajes que han durado más de media hora.
    rdd_time.filter(lambda x: x > 1800).count()

    # Viajes que han durado más de una hora.
    rdd_time.filter(lambda x: x > 3600).count()
    rdd_3600 = rdd_users.map(lambda x: (x[1][0], x[1][1], x[1][2])).filter(lambda x: x[2] > 3600)
    print(rdd_3600.take(10))
    print(rdd_3600.count())
