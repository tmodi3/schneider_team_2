import pandas as pd  # Import pandas for DataFrame conversion
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from flask import Flask, request, render_template
import time
import logging
from datetime import datetime
from pyspark import SparkConf

# Create Spark configuration
spark_conf = SparkConf()
spark_conf.set("spark.hadoop.security.authentication", "simple")
spark_conf.set("spark.hadoop.security.authorization", "false")

# Initialize Spark session in local mode
spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .master("local[*]") \
    .appName("FreightSearchApp") \
    .getOrCreate()

# Initialize the Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(filename='app.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

# Load CSV files using Spark
load_posting_path = '/Users/tanmaymodi/Schneider/load_posting.csv'
load_stop_path = '/Users/tanmaymodi/Schneider/load_stop.csv'

try:
    logging.info("Attempting to load CSV files...")
    load_posting = spark.read.csv(load_posting_path, header=True, inferSchema=True)
    load_stop = spark.read.csv(load_stop_path, header=True, inferSchema=True)
    logging.info("CSV files loaded successfully using Spark.")
except Exception as e:
    logging.error(f"Error loading CSV files with Spark: {e}")
    raise

# Date validation function
def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        logging.error(f"Invalid date format: {date_str}")
        return False

def enhanced_search(trailer_type=None, origin_city=None, origin_state=None, destination_city=None, destination_state=None, pickup_date=None, delivery_date=None, is_hazardous=None, is_high_value=None):
    start_time = time.time()

    logging.info(f"Search initiated with filters - Trailer Type: {trailer_type}, Origin: {origin_city}, {origin_state}, Destination: {destination_city}, {destination_state}, Pickup Date: {pickup_date}, Delivery Date: {delivery_date}, Hazardous: {is_hazardous}, High Value: {is_high_value}")

    try:
        # Join the two Spark DataFrames on LOAD_ID
        merged_data = load_posting.join(load_stop, "LOAD_ID", "inner")
        logging.info("DataFrames merged successfully.")
    except Exception as e:
        logging.error(f"Error during merging DataFrames: {e}")
        raise

    # Filter by trailer type (cargo type)
    if trailer_type:
        try:
            merged_data = merged_data.filter(col('TRANSPORT_MODE').contains(trailer_type))
            logging.info(f"Filtered by trailer type '{trailer_type}', remaining records: {merged_data.count()}")
        except Exception as e:
            logging.error(f"Error filtering by trailer type: {e}")
            raise

    # Filter by hazardous status
    if is_hazardous is not None:
        try:
            merged_data = merged_data.filter(col('IS_HAZARDOUS') == lit(is_hazardous))
            logging.info(f"Filtered by hazardous status '{is_hazardous}', remaining records: {merged_data.count()}")
        except Exception as e:
            logging.error(f"Error filtering by hazardous status: {e}")
            raise

    # Filter by high value
    if is_high_value is not None:
        try:
            merged_data = merged_data.filter(col('IS_HIGH_VALUE') == lit(is_high_value))
            logging.info(f"Filtered by high value status '{is_high_value}', remaining records: {merged_data.count()}")
        except Exception as e:
            logging.error(f"Error filtering by high value status: {e}")
            raise

    # Initialize origin and destination filtered data
    origin_filtered = merged_data
    destination_filtered = merged_data

    # Filter by origin city/state (pickup: STOP_TYPE == 'P')
    if origin_city:
        try:
            origin_filtered = origin_filtered.filter((col('CITY').contains(origin_city)) & (col('STOP_SEQUENCE') == 1) & (col('STOP_TYPE') == 'P'))
            logging.info(f"Filtered by origin city '{origin_city}' for pickup, remaining records: {origin_filtered.count()}")
        except Exception as e:
            logging.error(f"Error filtering by origin city: {e}")
            raise
    if origin_state:
        try:
            origin_filtered = origin_filtered.filter((col('STATE').contains(origin_state)) & (col('STOP_TYPE') == 'P'))
            logging.info(f"Filtered by origin state '{origin_state}' for pickup, remaining records: {origin_filtered.count()}")
        except Exception as e:
            logging.error(f"Error filtering by origin state: {e}")
            raise

    # Filter by destination city/state (drop-off: STOP_TYPE == 'D')
    if destination_city:
        try:
            destination_filtered = destination_filtered.filter((col('CITY').contains(destination_city)) & (col('STOP_SEQUENCE') == destination_filtered.agg({'STOP_SEQUENCE': 'max'}).collect()[0][0]) & (col('STOP_TYPE') == 'D'))
            logging.info(f"Filtered by destination city '{destination_city}' for drop-off, remaining records: {destination_filtered.count()}")
        except Exception as e:
            logging.error(f"Error filtering by destination city: {e}")
            raise
    if destination_state:
        try:
            destination_filtered = destination_filtered.filter((col('STATE').contains(destination_state)) & (col('STOP_TYPE') == 'D'))
            logging.info(f"Filtered by destination state '{destination_state}' for drop-off, remaining records: {destination_filtered.count()}")
        except Exception as e:
            logging.error(f"Error filtering by destination state: {e}")
            raise

    # Join the filtered origin and destination data
    try:
        filtered_data = origin_filtered.join(destination_filtered, "LOAD_ID")
        logging.info(f"Filtered data joined successfully, total records: {filtered_data.count()}")
    except Exception as e:
        logging.error(f"Error joining filtered data: {e}")
        raise

    # Apply pickup and delivery date filters
    if pickup_date and is_valid_date(pickup_date):
        try:
            filtered_data = filtered_data.filter(col('APPOINTMENT_FROM').contains(pickup_date))
            logging.info(f"Filtered by pickup date '{pickup_date}', remaining records: {filtered_data.count()}")
        except Exception as e:
            logging.error(f"Error filtering by pickup date: {e}")
            raise
    if delivery_date and is_valid_date(delivery_date):
        try:
            filtered_data = filtered_data.filter(col('APPOINTMENT_TO').contains(delivery_date))
            logging.info(f"Filtered by delivery date '{delivery_date}', remaining records: {filtered_data.count()}")
        except Exception as e:
            logging.error(f"Error filtering by delivery date: {e}")
            raise

    search_time = time.time() - start_time
    logging.info(f"Search completed in {search_time} seconds, final result count: {filtered_data.count()}")

    try:
        # Convert the Spark DataFrame to Pandas for rendering
        results_df = filtered_data.toPandas() if not filtered_data.rdd.isEmpty() else pd.DataFrame()
        return results_df, search_time
    except Exception as e:
        logging.error(f"Error converting Spark DataFrame to Pandas: {e}")
        raise


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        trailer_type = request.form.get('trailer_type')
        origin_city = request.form.get('origin_city')
        origin_state = request.form.get('origin_state')
        destination_city = request.form.get('destination_city')
        destination_state = request.form.get('destination_state')
        pickup_date = request.form.get('pickup_date')
        delivery_date = request.form.get('delivery_date')
        is_hazardous = request.form.get('is_hazardous') == 'on'
        is_high_value = request.form.get('is_high_value') == 'on'

        # Perform the enhanced search
        try:
            results, search_time = enhanced_search(trailer_type, origin_city, origin_state, destination_city, destination_state, pickup_date, delivery_date, is_hazardous, is_high_value)
        except Exception as e:
            logging.error(f"Error during enhanced search: {e}")
            return f"<p>An error occurred: {e}</p>"

        # If no results, show a "No results found" message
        if results.empty:
            results_html = "<p>No results found.</p>"
        else:
            # Display all results
            results_html = results.to_html(index=False)

        # Render the template with the search results and search time
        return render_template('index.html', results=results_html, search_time=search_time)

    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
