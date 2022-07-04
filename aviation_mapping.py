
from pyspark.sql import SparkSession
import os, sys

from pyspark.sql.functions import  col
from pyspark.sql.types import StringType, StructField, FloatType, IntegerType, TimestampType, StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



class airport_database():
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        # self.init_data_airport().show(truncate=False)
        # self.init_data_airline().show(truncate=False)

    def init_data_airport(self):
        table_schema = StructType([StructField('ID', StringType(), False),
                                   StructField('Airport_name', StringType(), True),
                                   StructField('City', StringType(), True),
                                   StructField('Country', StringType(), True),
                                   StructField('IATA', StringType(), True),
                                   StructField('ICAO', StringType(), False),
                                   StructField('Latitude', FloatType(), False),
                                   StructField('Longitude', FloatType(), False),
                                   StructField('Altitude', StringType(), False),
                                   StructField('Timezone_hours', IntegerType(), False),
                                   StructField('Daylight_savings_time', StringType(), False),
                                   StructField('Timezone_area', StringType(), False),
                                   StructField('Type_airport', TimestampType(), True)])
        return self.spark.read \
            .option("delimiter", ",") \
            .schema(table_schema) \
            .csv("airports-extended.csv") \
            .select(col('City'), col('IATA'))

    def init_data_airline(self):
        table_schema = StructType([StructField('ID', StringType(), False),
                                   StructField('Airline_name', StringType(), True),
                                   StructField('Alias', StringType(), True),
                                   StructField('IATA', StringType(), True),
                                   StructField('ICAO', StringType(), False),
                                   StructField('CallSign', FloatType(), False),
                                   StructField('Country', FloatType(), False),
                                   StructField('Active', StringType(), False)])
        return self.spark.read \
            .option("delimiter", ",") \
            .schema(table_schema) \
            .csv("airlines.csv").select(col('Airline_name'), col('IATA')).na.drop("any")

# if __name__ == '__main__':
#     airport_database()