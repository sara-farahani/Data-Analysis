import numpy as np 


def load_dataset(spark_session, file_path):
    # Load the dataset into a DataFrame
    data_frame = spark_session.read.csv(file_path, header=True, inferSchema=True)
    
    # Remove rows with missing values
    data_frame.dropna()

    return data_frame