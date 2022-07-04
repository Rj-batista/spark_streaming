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
            client_id="NiItSOIbJgLxpiduy7sTS2pcGED0vtMV",
            client_secret="HOPtnAaOdNmMA3kf",

        )

    def __iter__(self):
        try:
            response = self.amadeus.shopping.flight_offers_search.get(
                originLocationCode=self.departure,
                destinationLocationCode=self.arrival,
                departureDate=self.date,
                adults=self.nb_passengers,
            )
            return iter(response.data)
        except ResponseError as error:
            print(error)

    def create_dataframe_from_API(self):
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(data=amadeus_client.__iter__(self))
        return df.select(
            df.id, df.lastTicketingDate, df.itineraries, df.price, df.travelerPricings
        ).filter(df.numberOfBookableSeats >= self.nb_passengers)

    def get_escale_or_not(self):
        df_clean = self.create_dataframe_from_API()
        df_tmp = df_clean.select(
            df_clean.id,
            df_clean.lastTicketingDate,
            df_clean.itineraries,
            df_clean.price,
            df_clean.travelerPricings,
            explode(df_clean.itineraries),
        )

        df_tmp2 = df_tmp.select(
            df_tmp.id,
            df_tmp.lastTicketingDate,
            df_tmp.price,
            df_tmp.travelerPricings,
            map_filter("col", lambda v, _: v == "segments").alias("data_filtered"),
        ).select(
            df_tmp.id,
            df_tmp.lastTicketingDate,
            df_tmp.price,
            df_tmp.travelerPricings,
            map_values("data_filtered").alias("values"),
        )

        df_final_escale = (
            df_tmp2.select(
                df_tmp2.id,
                df_tmp2.lastTicketingDate,
                df_tmp2.price,
                df_tmp2.travelerPricings,
                explode(df_tmp2.values),
            )
            .withColumnRenamed("col", "information")
            .withColumn(
                "escale", col("information").rlike("\\bdeparture\\b.*\\bdeparture\\b")
            )
            .withColumn(
                "information", expr("substring(information, 2, length(information)-2)")
            )
            .withColumn("information", regexp_replace("information", "=", ":"))
        )

        if self.escale == "false":
            return df_final_escale
        elif self.escale == "true":
            return df_final_escale

    def get_itineraries(self):
        df_tmp2 = self.get_escale_or_not()

        if self.escale == "false":

            df_tmpdirect = df_tmp2.filter(df_tmp2.escale == "false")
            df_tmpdirect = (
                df_tmpdirect.withColumn(
                    "arrival", split(df_tmpdirect["information"], ", a").getItem(1)
                )
                .withColumn(
                    "departure", split(df_tmpdirect["information"], ", d").getItem(1)
                )
                .withColumn(
                    "carrierCode", split(df_tmpdirect["information"], ", ca").getItem(1)
                )
                .drop("information")
            )
            return df_tmpdirect.withColumn(
                "carrierCodes", split(df_tmpdirect["carrierCode"], ",").getItem(0)
            ).drop("carrierCode")

        elif self.escale == "true":

            df_tmpescale = df_tmp2.filter(df_tmp2.escale == "true")
            df_tmpescale = (
                df_tmpescale.withColumn(
                    "arrival_1", split(df_tmpescale["information"], ", a").getItem(1)
                )
                .withColumn(
                    "arrival_2", split(df_tmpescale["information"], ", a").getItem(3)
                )
                .withColumn(
                    "col_{}".format(2),
                    split(df_tmpescale["information"], "}, d").getItem(1),
                )
            )
            df_tmpescale = df_tmpescale.withColumn(
                "departure_1", split(df_tmpescale["col_2"], ", \{d").getItem(0)
            )
            df_tmpescale = df_tmpescale.withColumn(
                "departure_2",
                split(df_tmpescale["information"], "departure").getItem(2),
            ).withColumn(
                "carrierCode",
                split(df_tmpescale["information"], "operating").getItem(1),
            )
            return df_tmpescale.withColumn(
                "carrierCodes", split(df_tmpescale["carrierCode"], ",").getItem(0)
            ).drop("information", "col_2", "carrierCode")

    def get_price_and_cabin_clean(self):
        df_path = self.get_itineraries()
        if self.escale == "false":
            df_tmp_price = (
                df_path.select(
                    df_path.id,
                    df_path.lastTicketingDate,
                    df_path.escale,
                    df_path.arrival,
                    df_path.departure,
                    df_path.carrierCodes,
                    df_path.price,
                    df_path.travelerPricings,
                    map_filter("price", lambda v, _: v == "grandTotal").alias(
                        "data_filtered"
                    ),
                    map_filter("price", lambda v, _: v == "currency").alias(
                        "currency_filtered"
                    ),
                )
                .select(
                    df_path.id,
                    df_path.lastTicketingDate,
                    df_path.escale,
                    df_path.arrival,
                    df_path.departure,
                    df_path.carrierCodes,
                    df_path.price,
                    df_path.travelerPricings,
                    map_values("data_filtered").alias("prices"),
                    map_values("currency_filtered").alias("currency"),
                )
                .drop("price")
            )

            df_tmp_price = df_tmp_price.select(
                df_tmp_price.id,
                df_tmp_price.lastTicketingDate,
                df_tmp_price.escale,
                df_tmp_price.arrival,
                df_tmp_price.departure,
                df_tmp_price.carrierCodes,
                df_tmp_price.currency,
                df_tmp_price.prices,
                explode("travelerPricings").alias("Class"),
            )

            df_tmp_price2 = df_tmp_price.select(
                df_tmp_price.id,
                df_tmp_price.lastTicketingDate,
                df_tmp_price.escale,
                df_tmp_price.arrival,
                df_tmp_price.departure,
                df_tmp_price.carrierCodes,
                df_tmp_price.currency,
                df_tmp_price.prices,
                map_values("Class").alias("Classes"),
            )

            df_tmp_price3 = df_tmp_price2.withColumn(
                "Classes", col("Classes").cast(StringType())
            ).withColumn("prices", col("prices").cast(StringType()))
            return (
                df_tmp_price3.withColumn(
                    "Cabin", split(df_tmp_price3["Classes"], ",").getItem(3)
                )
                .withColumn("currency", col("currency").cast(StringType()))
                .drop("Classes")
                .drop_duplicates(subset=["id"])
            )

        elif self.escale == "true":
            df_tmp_price = (
                df_path.select(
                    df_path.id,
                    df_path.lastTicketingDate,
                    df_path.escale,
                    df_path.arrival_1,
                    df_path.arrival_2,
                    df_path.departure_1,
                    df_path.departure_2,
                    df_path.carrierCodes,
                    df_path.price,
                    df_path.travelerPricings,
                    map_filter("price", lambda v, _: v == "grandTotal").alias(
                        "data_filtered"
                    ),
                    map_filter("price", lambda v, _: v == "currency").alias(
                        "currency_filtered"
                    ),
                )
                .select(
                    df_path.id,
                    df_path.lastTicketingDate,
                    df_path.escale,
                    df_path.arrival_1,
                    df_path.arrival_2,
                    df_path.departure_1,
                    df_path.departure_2,
                    df_path.carrierCodes,
                    df_path.price,
                    df_path.travelerPricings,
                    map_values("data_filtered").alias("prices"),
                    map_values("currency_filtered").alias("currency"),
                )
                .drop("price")
            )

            df_tmp_price = df_tmp_price.select(
                df_tmp_price.id,
                df_tmp_price.lastTicketingDate,
                df_tmp_price.escale,
                df_tmp_price.arrival_1,
                df_tmp_price.arrival_2,
                df_tmp_price.departure_1,
                df_tmp_price.departure_2,
                df_tmp_price.carrierCodes,
                df_tmp_price.currency,
                df_tmp_price.prices,
                explode("travelerPricings").alias("Class"),
            )

            df_tmp_price2 = df_tmp_price.select(
                df_tmp_price.id,
                df_tmp_price.lastTicketingDate,
                df_tmp_price.escale,
                df_tmp_price.arrival_1,
                df_tmp_price.arrival_2,
                df_tmp_price.departure_1,
                df_tmp_price.departure_2,
                df_tmp_price.carrierCodes,
                df_tmp_price.currency,
                df_tmp_price.prices,
                map_values("Class").alias("Classes"),
            )

            df_tmp_price3 = df_tmp_price2.withColumn(
                "Classes", col("Classes").cast(StringType())
            ).withColumn("prices", col("prices").cast(StringType()))
            return (
                df_tmp_price3.withColumn(
                    "Cabin", split(df_tmp_price3["Classes"], ",").getItem(0)
                )
                .withColumn("currency", col("currency").cast(StringType()))
                .drop("Classes")
                .drop_duplicates(subset=["id"])
            )

    def silver_table(self):
        df_tmp = self.get_price_and_cabin_clean()
        if self.escale == "false":

            return (
                df_tmp.withColumn(
                    "carrierCodes",
                    expr("substring(carrierCodes, 11, length(carrierCodes)-1)"),
                )
                .withColumn("Cabin", expr("substring(Cabin, 8, length(Cabin)-1)"))
                .withColumn("arrival", expr("substring(arrival, 12,length(arrival)-1)"))
                .withColumn(
                    "departure", expr("substring(departure, 14, length(departure)-3)")
                )
                .withColumn("arrival", regexp_replace("arrival", "}", ""))
                .withColumn("departure", regexp_replace("departure", "}}", ""))
            )

        elif self.escale == "true":

            return (
                df_tmp.withColumn(
                    "carrierCodes",
                    expr("substring(carrierCodes, 15, length(carrierCodes)-1)"),
                )
                .withColumn("Cabin", expr("substring(Cabin, 10, length(Cabin)-1)"))
                .withColumn("carrierCodes", regexp_replace("carrierCodes", "}", ""))
                .withColumn("Cabin", regexp_replace("Cabin", "}", ""))
                .withColumn(
                    "arrival_1", expr("substring(arrival_1, 12,length(arrival_1)-1)")
                )
                .withColumn(
                    "departure_1",
                    expr("substring(departure_1, 14, length(departure_1)-3)"),
                )
                .withColumn(
                    "arrival_2", expr("substring(arrival_2, 12,length(arrival_2)-1)")
                )
                .withColumn(
                    "departure_2",
                    expr("substring(departure_2, 14, length(departure_2)-3)"),
                )
                .withColumn("arrival_1", regexp_replace("arrival_1", "}", ""))
                .withColumn("departure_1", regexp_replace("departure_1", "}}", ""))
                .withColumn("arrival_2", regexp_replace("arrival_2", "}", ""))
                .withColumn("departure_2", regexp_replace("departure_2", "}}", ""))
            )

    def plane_ticket(self):
        if self.escale == "false":
            df_airport = airport_database().init_data_airport()
            df_airline = airport_database().init_data_airline()
            df_tmp = self.silver_table()

            df_tmp2 = (
                df_tmp.withColumn(
                    "date_darrivee", split(df_tmp["arrival"], ",").getItem(0)
                )
                .withColumn("ville_arrive", split(df_tmp["arrival"], ",").getItem(1))
                .withColumn(
                    "date_de_depart", split(df_tmp["departure"], ",").getItem(0)
                )
                .withColumn(
                    "ville_de_depart", split(df_tmp["departure"], ",").getItem(1)
                )
                .withColumn(
                    "ville_arrive",
                    expr("substring(ville_arrive, 11, length(ville_arrive)-1)"),
                )
                .withColumn(
                    "ville_de_depart",
                    expr("substring(ville_de_depart, 11, length(ville_de_depart)-1)"),
                )
                .drop("arrival", "departure")
            )

            df_tmp3 = (
                df_tmp2.join(
                    df_airline, df_tmp2.carrierCodes == df_airline.IATA, "inner"
                )
                .join(df_airport, df_tmp2.ville_arrive == df_airport.IATA, "inner")
                .select(
                    "Airline_name",
                    "id",
                    "date_de_depart",
                    "currency",
                    "prices",
                    "ville_de_depart",
                    "Cabin",
                    "lastTicketingDate",
                    "escale",
                    "date_darrivee",
                    "City",
                    "ville_arrive",
                )
                .withColumnRenamed("Airline_name", "Company")
                .withColumnRenamed("City", "Ville_darrivee")
                .withColumnRenamed("ville_arrive", "Aeroport_darrive")
            )

            return (
                df_tmp3.join(
                    df_airport, df_tmp2.ville_de_depart == df_airport.IATA, "inner"
                )
                .select(
                    "id",
                    "ville_de_depart",
                    "City",
                    "date_de_depart",
                    "Aeroport_darrive",
                    "Ville_darrivee",
                    "date_darrivee",
                    "escale",
                    "currency",
                    "prices",
                    "Cabin",
                    "Company",
                    "lastTicketingDate",
                )
                .withColumnRenamed("ville_de_depart", "Aeroport_de_depart")
                .withColumnRenamed("City", "ville_de_depart")
            )

        elif self.escale == "true":
            df_airport = airport_database().init_data_airport()
            df_airline = airport_database().init_data_airline()
            df_tmp = self.silver_table()

        df_tmp2 = (
            df_tmp.withColumn(
                "date_de_depart", split(df_tmp["departure_1"], ",").getItem(0)
            )
            .withColumn(
                "Aeroport_de_depart", split(df_tmp["departure_1"], ",").getItem(1)
            )
            .withColumn(
                "Aeroport_de_depart",
                expr("substring(Aeroport_de_depart, 11, length(Aeroport_de_depart)-1)"),
            )
            .withColumn(
                "date_darrivee_escale", split(df_tmp["arrival_1"], ",").getItem(0)
            )
            .withColumn("aeroport_descale", split(df_tmp["arrival_1"], ",").getItem(1))
            .withColumn(
                "date_depart_escale", split(df_tmp["departure_2"], ",").getItem(0)
            )
            .withColumn(
                "aeroport_descale",
                expr("substring(aeroport_descale, 11, length(aeroport_descale)-1)"),
            )
            .withColumn("date_darrivee", split(df_tmp["arrival_2"], ",").getItem(0))
            .withColumn("aeroport_darrivee", split(df_tmp["arrival_2"], ",").getItem(1))
            .withColumn(
                "aeroport_darrivee",
                expr("substring(aeroport_darrivee, 11, length(aeroport_darrivee)-1)"),
            )
            .drop("arrival_1", "departure_1", "arrival_2", "departure_2")
        )

        df_tmp3 = (
            df_tmp2.join(df_airline, df_tmp2.carrierCodes == df_airline.IATA, "inner")
            .join(df_airport, df_tmp2.aeroport_descale == df_airport.IATA, "inner")
            .select(
                "id",
                "Aeroport_de_depart",
                "date_de_depart",
                "City",
                "aeroport_descale",
                "date_darrivee_escale",
                "date_depart_escale",
                "aeroport_darrivee",
                "date_darrivee",
                "escale",
                "currency",
                "prices",
                "Cabin",
                "Airline_name",
                "lastTicketingDate",
            )
            .withColumnRenamed("Airline_name", "Company")
            .withColumnRenamed("City", "ville_descale")
        )

        df_tmp4 = (
            df_tmp3.join(
                df_airport, df_tmp2.Aeroport_de_depart == df_airport.IATA, "inner"
            )
            .select(
                "id",
                "Aeroport_de_depart",
                "City",
                "date_de_depart",
                "ville_descale",
                "aeroport_descale",
                "date_darrivee_escale",
                "date_depart_escale",
                "aeroport_darrivee",
                "date_darrivee",
                "escale",
                "currency",
                "prices",
                "Cabin",
                "Company",
                "lastTicketingDate",
            )
            .withColumnRenamed("City", "ville_de_depart")
        )

        return (
            df_tmp4.join(
                df_airport, df_tmp2.aeroport_darrivee == df_airport.IATA, "inner"
            )
            .select(
                "id",
                "Aeroport_de_depart",
                "ville_de_depart",
                "date_de_depart",
                "ville_descale",
                "aeroport_descale",
                "date_darrivee_escale",
                "date_depart_escale",
                "aeroport_darrivee",
                "City",
                "date_darrivee",
                "escale",
                "currency",
                "prices",
                "Cabin",
                "Company",
                "lastTicketingDate",
            )
            .withColumnRenamed("City", "ville_darrivee")
        )


if __name__ == "__main__":
    """
    -------------------------------------------------------------------------------------------------------------------
    Première partie du programme on prend en entrée les informations de voyage et on récupère la table avec toutes
    les informations dessus
    -------------------------------------------------------------------------------------------------------------------
    """
    # "CDG", "MAD", "2022-11-01", 2
    # des_all, des_ret,date,nb_passager = input("Entrez vos informations de voyages: ")
    spark = SparkSession.builder.getOrCreate()
    tmp = amadeus_client("CDG", "LIS", "2022-11-01", 2, "true")
    tmp.plane_ticket().show(truncate=False)


    """
   -------------------------------------------------------------------------------------------------------------------
   Zone de test sur le dataframe (grosChat)
   -------------------------------------------------------------------------------------------------------------------
   """
