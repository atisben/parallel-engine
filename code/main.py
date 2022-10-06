import pandas as pd
from datetime import datetime
import random
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import warnings
warnings.simplefilter("ignore")
from tools import Directory, Table, Dataframe
from google.cloud import bigquery

# Parse command line arguments
parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-p", "--project", type=str, help="id of the GCP project")
parser.add_argument("-d", "--dataset", type=str, help="name of the output BigQuery dataset")
parser.add_argument("-v", "--var", type=int, help="any variable to be reflected in the output")
args = vars(parser.parse_args())


# Create the output dataset
client = bigquery.Client(args["project"])
directory = Directory(client, args["project"], args["dataset"])
directory.create(location = "EU")

table = Table(client, directory, "runner_logs")

# Generate the DataFrame
df = pd.DataFrame()
df = df.append({"timestamp": datetime.now(), "id": random.randint(1,264), "var":args["var"]}, ignore_index=True)

dataframe = Dataframe(client, df)
dataframe.to_table(table, write_disposition="WRITE_APPEND")



