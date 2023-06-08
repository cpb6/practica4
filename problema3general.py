import datetime
import json
import os
import sys
from pprint import pprint

from pyspark import SparkConf, SparkContext

"""ESTUDIO DEL PROBLEMA 3 PARA EL AÑO Y MESES DESEADOS

Uso:
python/python3 problema3general.py "2017" "4 5 6" /public/bicimad

"""
def initSC(cores, memory):
    conf = SparkConf()\
        .setAppName(f"Bicimad most used origin stations {cores} cores {memory} mem")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    #sc.addPyFile("py.zip")
    return sc

def get_trip(line):
    data = json.loads(line)
    s_date = data["unplug_hourTime"]
    if isinstance(s_date, str):
        s_date = s_date.replace("Z","+0000")
        data["unplug_hourTime"] = datetime.datetime.strptime(s_date, "%Y-%m-%dT%H:%M:%S%z")
    else:
        s_date = s_date['$date']
        data["unplug_hourTime"] = datetime.datetime.strptime(s_date, "%Y-%m-%dT%H:%M:%S.%f%z")
    return data

def get_station_trips(sc,rdd):
    trips = rdd.map(get_trip)\
        .filter(lambda x: x["user_type"]==1)\
        .map(lambda x: (x["idunplug_station"], 1))\
        .countByKey()
    trips1= sc.parallelize(list(trips.items()))
    trips_ord=trips1.sortBy(lambda x: -x[1])
    return trips_ord




def main(sc, years, months, datadir):
    rdd = sc.parallelize([])
    for y in years:
        for m in months:
            filename = f"{datadir}/{y}{m:02}_movements.json"
            print(f"Adding {filename}")
            rdd = rdd.union(sc.textFile(filename))

    trips = get_station_trips(sc,rdd)
    print("............Starting computations.............")
    print(f"Las estaciones más utilizadas como origen son:")
    for t in trips.take(3):
        print(t)



if __name__=="__main__":
    if len(sys.argv)<2:
        years = [2017]
    else:
        years = list(map(int, sys.argv[1].split()))

    if len(sys.argv)<3:
        months = [4]
    else:
        months = list(map(int, sys.argv[2].split()))

    if len(sys.argv)<4:
        datadir = "."
    else:
        datadir = sys.argv[3]

    print(f"years: {years}")
    print(f"months: {months}")
    print(f"datadir: {datadir}")

    with initSC(0,0) as sc:
        main(sc, years, months, datadir)