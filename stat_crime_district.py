import sys
in_csv_file_crime = sys.argv[1]
in_csv_file_offense_codes = sys.argv[2]
out_parquet_file_result = sys.argv[3]


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Row, Window
import pyspark.sql.functions as F
def main():
        spark = SparkSession.builder.appName("crimes_stat").getOrCreate()
        crimes = spark.read.option("header", "true"
                                  ).option("delimetr", ","
                                  ).option("quote", "\""
                                  ).option("escape", "\""
                                  ).csv(in_csv_file_crime)
                                  # ).csv("spark/crime/crime.csv")
        crimes = crimes.withColumn("Lat", crimes["Lat"].cast('float')).withColumn(
                  "Long", crimes["Long"].cast('float'))

        offense_codes = spark.read.option("header", "true"
                                         ).option("delimetr", ","
                                         ).option("quote", "\""
                                         ).option("escape", "\""
                                         ).csv(in_csv_file_offense_codes)
                                        #  ).csv("spark/crime/offense_codes.csv")
        offense_codes = offense_codes.select(F.lpad(offense_codes['CODE'], 5, '0'
                        ).alias('CODE'), 'NAME'
                        ).dropDuplicates(['CODE'])
        split_col = F.split(offense_codes['NAME'], '-')
        offense_codes = offense_codes.select("CODE", F.rtrim(split_col.getItem(0).alias('NAME')).alias('NAME'))

        # Подсчет общего числа преступлений по районам 
        count_crimes_dist = crimes.groupBy('DISTRICT').count().withColumnRenamed('count', 'COUNT_CRIMES')

        #Подсчет медианы количества преступлений совершенных за месяц в каждом районе
        function_median = F.expr('percentile_approx(count, 0.5)')
        median_count_crimes_dist_month = crimes.groupBy("DISTRICT","YEAR","MONTH").count().groupBy("DISTRICT"
                      ).agg(function_median.alias('MEDIAN_CRIMES_MONTH'))
        
        
        # Расчет трех самых распостраненных преступлений в каждом районе
        windowSpec  = Window.partitionBy('DISTRICT').orderBy(F.desc('COUNT_CRIMES_TYPE'))
        crimes_count_dist_crime_type = crimes.groupBy('DISTRICT','OFFENSE_CODE').count().withColumnRenamed('count', 'COUNT_CRIMES_TYPE'
                      ).withColumn("ROW_NUMBER_COUNT_CRIMES_TYPE",F.row_number().over(windowSpec)
                      ).filter((F.col("ROW_NUMBER_COUNT_CRIMES_TYPE") == "1"
                      ) | (F.col("ROW_NUMBER_COUNT_CRIMES_TYPE") == "2"
                      ) | (F.col("ROW_NUMBER_COUNT_CRIMES_TYPE") == "3"))
        crimes_count_dist_crime_type_1 = crimes_count_dist_crime_type.join(
                       offense_codes, crimes_count_dist_crime_type.OFFENSE_CODE == offense_codes.CODE,"inner").select(
                       "DISTRICT","ROW_NUMBER_COUNT_CRIMES_TYPE","NAME")
        crimes_count_dist_crime_type_1_groupBy = crimes_count_dist_crime_type_1.groupby("DISTRICT").agg(
                                                 F.collect_list('NAME').alias("NAME")
                                                 )
        crimes_3max_count_dist_crime_type = crimes_count_dist_crime_type_1_groupBy.withColumn(
                                         "NAME", F.concat_ws(", ", "NAME"))

        #Расчет средней долгоды и широты преступления, совершенных в каждом районе
        crimes_avg_lat_long_dist = crimes.groupBy('DISTRICT').agg(F.avg("Lat").alias("Lat"),
                                       F.avg("Long").alias("Long")
                                       )

        # Сборка результирующей витрины и вывод в файл
        view_result = count_crimes_dist.join(median_count_crimes_dist_month,["DISTRICT"]
                                            ).join(crimes_3max_count_dist_crime_type,["DISTRICT"]
                                            ).join(crimes_avg_lat_long_dist,["DISTRICT"])
        view_result.write.mode('overwrite').parquet(out_parquet_file_result)
        # view_result.write.mode('overwrite').parquet("path/to/output_folder")

if __name__ == "__main__":
     main()

