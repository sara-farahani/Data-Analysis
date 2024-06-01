from pyspark.sql.functions import date_format, when, col, min, max
from pyspark.sql.window import Window
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
import io
import folium
from textwrap import wrap


def assign_rfm_scores(df, metric_col, score_col, reverse=False):
        # Compute the minimum and maximum values of the metric column
        min_max_vals = df.select(min(metric_col).alias("min_val"), max(metric_col).alias("max_val")).collect()
        min_val = min_max_vals[0]["min_val"]
        max_val = min_max_vals[0]["max_val"]

        # Normalize the column
        if reverse:
            df = df.withColumn(score_col, 200*(1- ((col(metric_col) - min_val) / (max_val - min_val))))
        else:
            df = df.withColumn(score_col, 100*(col(metric_col) - min_val) / (max_val - min_val))
        return df


def assign_total_rfm_score(rfm_df, rfm_score_col_header):
    # Calculate RFM score by concatenating the individual scores
    rfm_score_df = rfm_df.withColumn(rfm_score_col_header, 0.05*(0.45*col("recency_score")+(0.3*col("frequency_score"))+\
                                                    (0.25*col("monetary_score"))))
    rfm_score_df = rfm_score_df.withColumn("rfm_final_score", 
        when(col(rfm_score_col_header) <= 1.2, 1)
        .when((col(rfm_score_col_header) > 1.2) & (col(rfm_score_col_header) <= 2.5), 2)
        .when((col(rfm_score_col_header) > 2.5) & (col(rfm_score_col_header) <= 3.5), 3)
        .when((col(rfm_score_col_header) > 3.5) & (col(rfm_score_col_header) <= 4.2), 4)
        .otherwise(5)
    )

    return rfm_score_df


def plot_geo_states(dataframe, selected_column1_header, selected_column2_header,\
                     geo_states, save_file_path):
    # Create a folium map centered around Brazil (Based on dataset)
    geo_map = folium.Map(location=[-15.7833, -47.8667], zoom_start=5)

    # Add circles to the map
    for row in dataframe.collect():
        state = row[selected_column1_header]
        counts = row[selected_column2_header]
        # Get coordinates of the state
        geo_state = geo_states.filter(geo_states.geolocation_state == state).collect()[0]
        coordinates = [geo_state["geolocation_lat"], geo_state["geolocation_lng"]]
        if coordinates is not None:
            folium.Circle(
                location=coordinates,
                radius=counts*10,
                popup=f"{state}: {counts} customers",
                color="blue",
                fill=True,
                fill_color="blue"
            ).add_to(geo_map)
            folium.Marker(
                location=coordinates,
                icon=folium.DivIcon(html=f"""<div style="font-size: 16pt; color: red">{counts}</div>""")
            ).add_to(geo_map)

    img_data = geo_map._to_png(5)
    img = Image.open(io.BytesIO(img_data))
    img.save(save_file_path)


def convert_datetime_to_day_month(dataframe, column_header):
    return dataframe.withColumn("day", date_format(column_header, "EEEE")) \
                      .withColumn("month", date_format(column_header, "MMMM"))


def plot_line_bar(data_frame, title, x_label, y_label):
    plt.figure(figsize=(12, 6))
    plt.plot(data_frame[:, 0], data_frame[:, 1].astype(np.float), marker='o', color='b')
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.tight_layout()
    plt.show()


def plot_bar_chart(data_frame, title, x_label, y_label, wrapped_labels=False):
    plt.figure(figsize=(12, 6))
    num_products = len(np.unique(data_frame[:, 0]))
    colors = plt.cm.tab20(np.arange(num_products))
    if wrapped_labels:
        wrapped_labels = [ '\n'.join(wrap(label, 10)) for label in data_frame[:, 0]]
        data_frame[:, 0] = wrapped_labels

    plt.bar(data_frame[:, 0], data_frame[:, 1].astype(np.float), color=colors)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=10)
    plt.tight_layout()
    plt.show()


def plot_pie_chart(data_frame, labels):
    plt.pie(data_frame[:, 1], autopct='%.1f%%')
    plt.legend(labels, title="Categories", loc="upper left", bbox_to_anchor=(-0.4, 1.1))
    plt.show()