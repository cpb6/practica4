""" python/python3 practica4-cristina.py """
import json
import os

from pyspark import SparkContext


def movements_union_all(movements_rdds_values):
    # Creates Empty RDD
    result = sc.emptyRDD()

    for i in movements_rdds_values:
        result = result.union(i)

    return result


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
def estacion_origen(sc, rdd_users):
    
    rdd_origen= rdd_users.map(lambda x: (x[1][0],1))
    print(rdd_origen.take(1))
    cout=rdd_origen.countByKey()
    rdd_origenl= sc.parallelize(list(cout.items()))
    rdd_origen_ord=rdd_origenl.sortBy(lambda x: -x[1])
    return rdd_origen_ord


if __name__ == '__main__':
    sc = SparkContext()

    # constants
    DATASET_DIRECTORY = "public/bicimad"
    # DATASET_DIRECTORY = "hdfs://public_data/bicimad" 
    # DATASET_DIRECTORY = "hffs://public_data/bicimad"

    RDD_DATASET_MAPPERS = {
        "201704_movements.json": mapper_movements,
        "201705_movements.json": mapper_movements,
        "201706_movements.json": mapper_movements
    }

    rdds_objects = get_spark_rdd_objects_from_dir(DATASET_DIRECTORY, sc)
    print(rdds_objects)
    # INFORMATION
    print(f"All RDD objects: {rdds_objects}")

    rdd_movements_dict = {key: value for key, value in rdds_objects.items() if "movements" in key}

    print(f"All RDD objects - movements: {rdd_movements_dict}")

    print("=============== 201704_movements.json ==========")
    print(rdd_movements_dict["201704_movements.json"].take(1))

    print("=============== 201705_movements.json ==========")
    print(rdd_movements_dict["201705_movements.json"].take(1))

    print("=============== 201706_movements.json ==========")
    print(rdd_movements_dict["201706_movements.json"].take(1))

    all_movements_rdds = movements_union_all(movements_rdds_values=rdd_movements_dict.values())

    print("=============== ALL_movements.json ==========")
    print(all_movements_rdds.take(1))
    print(all_movements_rdds.count())

    # PROBLEMA 1
    print("====================================================================================================")
    print("PROBLEMA 1")
    problema1_doc = """Analizamos los tipos de usuarios. Queremos saber que tipo de usuarios utilizan más Bicimad. Para ello analizamos 
    la cantidad de usuarios de cada tipo. Utilizamos varios meses de 2019, para poder comprobar si es algo casual o no."""
    print(problema1_doc)


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
    rdd_users = all_movements_rdds.filter(lambda x: x[0] == selected_type).map(lambda x: (x[1], tuple(x[2:])))
    print(rdd_users)

    print("rdd_users.count(): Seleccionando Usuarios de Tipo 1 ")
    print(rdd_users.count())


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
    rdd_ages = all_movements_rdds.filter(lambda x: x[7] == selected_age).map(lambda x: (x[0], tuple(x[1:7])))

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

    problema2_conclusiones_parciales = f"""
    Conclusiones
    ====================== 
    Para las fechas registradas {rdd_movements_dict.keys()} el grupo de edad con un rango determinado que más utilizó 
    bicimad fue el grupo 4 (entre 27 y 40 años)"""
    print(problema2_conclusiones_parciales)
    print("==================================================================")
    print("                         PROBLEMA3                   ")
    print("                       ============                  ")
    print("Estudiamos las estaciones más utilizadas como origen")
    RDD = estacion_origen(sc, rdd_users)
    print(RDD.take(3))
    print("===============CONCLUSIONES=========")
    print("Obtenemos: [(57, 12014), (43, 11774), (163, 11275)]\n por tanto la estación más utilizada como origen en los meses escogidos es 57")
    