import json

from pyspark.python.pyspark.shell import sc

from aviation_mapping import airport_database
import os, sys
from amadeus import Client, ResponseError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode,
    col,
    map_values,
    map_filter,
    split,
    expr,
    regexp_replace,
)
from pyspark.sql.types import StringType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class amadeus_client:
    def __init__(self, departure, arrival, date, nb_passengers, escale):
        self.departure = departure
        self.arrival = arrival
        self.date = date
        self.nb_passengers = nb_passengers
        self.escale = escale
        self.amadeus = Client(
            

        )

    # def __iter__(self):
    #     try:
    #         response = self.amadeus.shopping.flight_offers_search.get(
    #             originLocationCode=self.departure,
    #             destinationLocationCode=self.arrival,
    #             departureDate=self.date,
    #             adults=self.nb_passengers,
    #         )
    #         return iter(response.data)
    #     except ResponseError as error:
    #         print(error)

    def create_dataframe_from_API(self):
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.json("sample1.json").schema()
        return df


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # # spark
    # print(f"Spark version = {spark.version}")
    #
    # # hadoop
    # print(f"Hadoop version = {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")


    tmp = amadeus_client("CDG", "MAD", "2022-11-01", 2, "true")
    tmp_2 = tmp.create_dataframe_from_API()
    tmp_2.show(truncate=False)
    tmp_2.printSchema()


