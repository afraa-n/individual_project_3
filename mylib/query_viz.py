"""
query and viz file
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# sample query 
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = (
        "SELECT species,"
        "AVG(petal_length * petal_width) AS avg_petal_area"
        "FROM default.iris"
        "GROUP BY species;"
    )
    query_result = spark.sql(query)
    return query_result

# sample viz for project
def viz():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    df = query.toPandas()

     # define species names and corresponding colors
    species_colors = {
        'Setosa': 'red',
        'Versicolor': 'green',
        'Virginica': 'blue'
    }
    # create a scatter plot for each species
    plt.figure(figsize=(10, 8)) 

    for variety, color in species_colors.items():
        subset = df[df['species'] == variety]
        plt.scatter(subset['sepal_length'], subset['sepal_width'], label=variety, c=color)

    plt.xlabel('Sepal Length (cm)')
    plt.ylabel('Sepal Width (cm)')
    plt.title('Scatter Plot: Sepal Length vs. Sepal Width')
    plt.legend(title='Species')
    plt.show()

if __name__ == "__main__":
    query_transform()
    viz()