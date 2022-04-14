from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import pyspark.sql.functions as f
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
stream_df = (spark.readStream.format('socket')
                             .option('host', '127.0.0.1')
                             .option('port', 22250)
                             .load())

json_df = stream_df.selectExpr("CAST(value AS STRING) AS payload")

writer = (
    json_df.writeStream
           .queryName('issloc')
           .format('memory')
           .outputMode('append')
)
import time

for _ in range(5):
    df = spark.sql("""
    SELECT get_json_object(payload, '$.timestamp') AS time,
           CAST(get_json_object(payload, '$.iss_position') as string) AS position
    FROM issloc
    """)

    df.show(10)

    print(df)
    time.sleep(5)

streamer.awaitTermination(timeout=3600)
print('streaming done!')
extract_df = df.withColumn('time', df['time']) \
       .withColumn('Latitude', f.split(df['position'], '"').getItem(3)) \
       .withColumn('Longitude', f.split(df['position'], '"').getItem(7))
plot_df = extract_df.toPandas()
plot_df['time'] = pd.to_datetime(plot_df['time'],unit='s')
gdf = geopandas.GeoDataFrame(
    plot_df, geometry=geopandas.points_from_xy(plot_df.Longitude, plot_df.Latitude))
worldmap = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
fig, ax = plt.subplots(figsize=(12, 6))
worldmap.plot(color="lightgrey", ax=ax)
x = plot_df['Longitude']
y = plot_df['Latitude']
plt.scatter(x, y,alpha=0.6,
            cmap='autumn')


# Creating axis limits and title
plt.xlim([-180, 180])
plt.ylim([-90, 90])

first_year = plot_df["time"].min().strftime("%Y/%d/%m, %H:%M:%S")
last_year = plot_df["time"].max().strftime("%Y/%d/%m, %H:%M:%S")
plt.title("ISS location " +
          str(first_year) + " - " + str(last_year))
plt.xlabel("Longitude")
plt.ylabel("Latitude")
frame1 = plt.gca()
frame1.axes.get_xaxis().set_ticks([])
frame1.axes.get_yaxis().set_ticks([])
plt.show()