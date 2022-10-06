""" 
              __             __                __
  ____  _____/ /_____  _____/ /___  __  ______/ /
 / __ \/ ___/ __/ __ \/ ___/ / __ \/ / / / __  / 
/ /_/ / /__/ /_/ /_/ / /__/ / /_/ / /_/ / /_/ /  
\____/\___/\__/\____/\___/_/\____/\__,_/\__,_/  
"""                                           


#-------------------------------
#        libraries
#-------------------------------

#bigquery sdk
from google.cloud import bigquery, storage
from google.api_core.exceptions import BadRequest
from google.cloud.exceptions import NotFound

#formating related libraries
from pygments import highlight, lexers, formatters
from pygments_pprint_sql import SqlFilter
from colorama import Fore, Back, Style
from datetime import datetime, timedelta
from time import sleep

#-------------------------------
#        diretory
#-------------------------------

class Directory:
    '''
    Creates BigQuery directory
            Parameters:
                    project (str): Name of the project
                    dataset (str): Name of the dataset
    '''
    def __init__(self, client, project, dataset):
        self.project = project
        self.dataset = dataset
        self.client = client
        self.dataset_id = f'{self.project}.{self.dataset}'
    
    def check_if_exist(self):
        try:
            self.client.get_dataset(self.dataset)  # Make an API request.
            print("Dataset {} already exists".format(self.dataset))
            return(True)
        except NotFound:
            print("Dataset {} is not found".format(self.dataset))
            return(False)
    
    def create(self, location:str, exists_ok=True):
        '''
        Create a dataset in BigQuery 
        
                Parameters:
                        location (str): dataset location 
                        exists_ok (bool): default = True, ignore already exists errors when creating the dataset
                        
        '''
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(self.dataset_id)

        # TODO(developer): Specify the geographic location where the dataset should reside.
        dataset.location = location
        assert type(location)==str, 'location must be a string'

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = self.client.create_dataset(dataset, exists_ok)  # Make an API request.
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))

    def set_expiracy(self, num_days:int):
        '''
        Set expiracy of the refered BigQuery directory(dataset) object 
        
                Parameters:
                        num_days (int): default table expiration delay (days) at the dataset level for any of the created tables
                            ignored when defined at the table level
                        
        '''
        # dataset_id = 'your-project.your_dataset'
        dataset = self.client.get_dataset(self.dataset_id)  # Make an API request.
        dataset.default_table_expiration_ms = num_days * 24 * 3600 * 1000  # In milliseconds.

        dataset = self.client.update_dataset(
            dataset, ["default_table_expiration_ms"]
        )  # Make an API request.

        full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
        print(
            "Updated dataset {} with new expiration {} ms".format(
                full_dataset_id, dataset.default_table_expiration_ms
            )
        )

        
        
class Table():
    '''
    Creates BigQuery table path from a directory
            Parameters:
                    directory (object): directory
                    table (str): Name of the table
    '''
    
    def __init__(self, client, directory, table):
        self.project = directory.project
        self.dataset = directory.dataset
        self.table = table
        self.client = client
        
    def path(self, language, display=False, table_suffix=""):
        '''
        Returns the full path of a BigQuert directory object 
        
                Parameters:
                        language (str): legacy or standard
                        display* (boolean): prints the directory once the function is completez
                        table_suffix* (str): string to be added at the end of the table path, separator is already handled by the function           
        '''
        #checks
        if len(table_suffix)> 0 :#if there is a table suffix, adds a separator
            if table_suffix == "_":
                table_suffix = "_"
            else:
                table_suffix = "_" + str(table_suffix)
        if language == "standard":
            self.dir = "`{project}.{dataset}.{table}{suffix}`".format(project = self.project, dataset = self.dataset, table = self.table, suffix = table_suffix)              
        elif language == "legacy":
            self.dir = "{project}:{dataset}.{table}{suffix}".format(project = self.project, dataset = self.dataset, table = self.table, suffix = table_suffix)
        elif language == "directory":
            self.dir = "{project}.{dataset}.{table}{suffix}".format(project = self.project, dataset = self.dataset, table = self.table, suffix = table_suffix)
        if display:
            print("> directory = {}".format((self.dir)))
        return(self.dir)
    
    def check_if_exist(self):
        
        try:
            self.client.get_table(self.path("directory"))  # Make an API request.
            print("Table {} already exists.".format(self.path("directory")))
            return(True)
        except:
            print("Table {} is not found.".format(self.path("directory")))
            return(False)
        
    def set_expiracy(self, num_days:int):
        '''
        Set expiracy of the refered BigQuery talbe object 
        
                Parameters:
                        num_days (int): numbers of day to maintain the table (from now)
                        
        '''

        dataset_ref = bigquery.DatasetReference(self.project, self.dataset)
        table_ref = dataset_ref.table(self.table)
        table = self.client.get_table(table_ref)  # API request

        assert table.expires is None

        # set table to expire 5 days from now
        expiration = datetime.now(datetime.timezone.utc) + timedelta(days=num_days)
        table.expires = expiration
        table = self.client.update_table(table, ["expires"])  # API request

        # expiration is stored in milliseconds
        margin = datetime.timedelta(microseconds=1000)
        assert expiration - margin <= table.expires <= expiration + margin

    def to_storage(self, bucket_name: str, csv_file_name:str, location = "EU"):
        '''
        Send a copy of the BQ table as .csv file to a storage bucket located on the same project
        
                Parameters:
                        bucket_name (str): name of the GCS bucket (includes subfloder if required)
                        csv_file_name (str): name of the endpoint file
                        
        '''

        destination_uri = "gs://{}/{}".format(bucket_name, csv_file_name)
        dataset_ref = bigquery.DatasetReference(self.project, self.dataset)
        table_ref = dataset_ref.table(self.table)

        extract_job = self.client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location=location,
        )  # API request
        extract_job.result()  # Waits for job to complete.

        print(
            "Exported {}:{}.{} to {}".format(self.project, self.dataset, self.table, destination_uri)
        )
    
    

#-------------------------------
#          Query
#-------------------------------   

class Query:
    
    def __init__(self, client, query:str):
        self.client = client
        self.query = query
        
    def _check_query_job_state(self, query_job):
        # Check on the progress by getting the job's updated state. Once the state
        # is `DONE`, the results are ready.
        query_job = self.client.get_job(
            query_job.job_id, location=query_job.location
        )  # Make an API request.
        print(f"> Job {query_job.job_id} is currently {query_job.state} ", end='')
        while query_job.state in ['RUNNING', 'PENDING']:
            print(">", end='')
            query_job = self.client.get_job(
                query_job.job_id, location=query_job.location
                )  
            sleep(0.5)
        
        try:
            query_job.result()
            print(Fore.GREEN + f"\n> Query {query_job.state} (ಠ‿↼)") 
        except BadRequest as e:
            for e in query_job.errors:
                print(Fore.RED + f"\n> Query FAILED (ಠ_ಠ)")
                print('ERROR: {}'.format(e['message']))
        print(Style.RESET_ALL)
        return(query_job)
        
    def _retrieve_query_job_metadata(self, query_job):
        try:
            print(Fore.MAGENTA + f"> Email: {query_job.user_email}")
            print(f"> Job time: {query_job.created}")
            print(f"> Billed Bytes: {query_job.total_bytes_billed}")
            print(Style.RESET_ALL)
        except:
            print(Fore.RED + f"> Error printing metadata")
            print(Style.RESET_ALL)

    
    def display(self):
        '''
        Returns a beautyfied and readable version of the query
                        
        '''
        lexer = lexers.MySqlLexer()
        return(print(highlight(self.query, lexer, formatters.TerminalFormatter())))
            
                
    def execute(self, dry_run=False, sequence=True):
        '''
        Executes the query
        
            Parameters:
                dry_run: (bool) if set to True, runs an estimation of the costs
                        
        '''        
        # Set up query job configs
        job_config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=False)
        
        query_job=self.client.query(
            self.query,
            job_config=job_config
        )
        
        if dry_run:
            # A dry run query completes immediately.
            print(f"> This query will process {query_job.total_bytes_processed} bytes.")
        
        if sequence and not dry_run:
            self._check_query_job_state(query_job)
            self._retrieve_query_job_metadata(query_job)
            
        
        
    
    def to_table(
        self, 
        endpoint, 
        table_suffix: str="", 
        write_disposition: str="WRITE_TRUNCATE", 
        date_partitioning_field : str = None, 
        clustering_fields: list = None, 
        sequence: bool = True,
        dry_run: bool = False
        ):
        """
        Creates a new table from the results of a query
        
            Parameters:
                endpoint (obj:Table): output table path
                table_suffix (str): any variable suffix to add at the end of the table ("_" will be added automatically)
                write_disposition (str): default is WRITE_TRUNCATE and will replace the table, can be changed for WRITE_APPEND
                date_partitioning_field (str): 	If set, the table is partitioned by this field, the field must be a top-level TIMESTAMP, DATETIME, or DATE field
                clustering_fields (list[str]): Fields defining clustering for the table,  immutable after table creation
                sequence (bool): if set to False, doesn't wait for the execution of the job to start the next one in the loop (beta)         
                dry_run (bool): True if this query should be a dry run to estimate costs
                        
        """
        
        if not sequence:
            print(Fore.RED + f"\n> Sequencing is deactivated, job status wont be verified" + Style.RESET_ALL) 
        #if there is a table suffix, a sperator must be added
        if len(table_suffix) > 0 and '$' not in table_suffix :
            table_suffix = "_" + str(table_suffix)
        # Prepare a reference to a new dataset for storing the query results.   
        job_config = bigquery.QueryJobConfig()
        job_config.destination = f"{endpoint.project}.{endpoint.dataset}.{endpoint.table}{table_suffix}"
        job_config.write_disposition = write_disposition
        #work in progress
        job_config.clustering_fields = clustering_fields
        if date_partitioning_field:
            job_config.time_partitioning = bigquery.table.TimePartitioning(
                # Default partitioning type is day
                field = date_partitioning_field
            )
        job_config.dry_run = dry_run
        query = self.query
        # Run the query.
        print(f'> Exporting query results to table {job_config.destination}')
        query_job = self.client.query(query, job_config=job_config)
        
        if sequence:
            query_job = self.client.get_job(
                query_job.job_id, location=query_job.location
            )  # Make an API request.

            # Update the query job once done
            query_job = self._check_query_job_state(query_job)
            self._retrieve_query_job_metadata(query_job)
            return(query_job)

        
    
    def to_df(self):
        '''
        Transfers the results of the query to a dataframe using a storage bucket as intermediary storage
        Strongly recommended
        
                Parameters:
                        sequence: (bool) if set to True, wait for the execution of the job            
        ''' 
        dataframe = (
            self.client.query(self.query)
            .result()
            .to_dataframe(
                progress_bar_type='tqdm'
            )
        )
        
        return(dataframe)

#-------------------------------
#          Dataframe
#-------------------------------   

class Dataframe:
    def __init__(self, client, dataframe):
        self.client = client
        self.dataframe = dataframe
        
    def _check_job_state(self, query_job):
        # Check on the progress by getting the job's updated state. Once the state
        # is `DONE`, the results are ready.
        query_job = self.client.get_job(
            query_job.job_id, location=query_job.location
        )  # Make an API request.
        print(f"> Job {query_job.job_id} is currently {query_job.state} ", end='')
        while query_job.state=='RUNNING':
            print(">", end='')
            query_job = self.client.get_job(
                query_job.job_id, location=query_job.location
                )  
            sleep(0.5) 
        print(Fore.GREEN + f"\n> Query {query_job.state} (ಠ‿↼)") 
        print(Style.RESET_ALL)
        
    def _retrieve_job_metadata(self, query_job):
        print(Fore.MAGENTA + f"> Email: {query_job.user_email}")
        print(f"> Job time: {query_job.created}")
        print(Style.RESET_ALL)
    
    def to_table(self, endpoint, table_suffix="", write_disposition='WRITE_TRUNCATE', sequence=True):
        '''
        Transfers the results of the dataframe to a bigquery table
        
                Parameters:
                        endoint: (Talbe obj)
                        table_suffix: (str) 
                        write_disposition: (str) default is set to 'WRITE_TRUNCATE'
                        sequence: (bool) If set to False, the job won't wait for validation        
        ''' 
        if not sequence:
            print(Fore.RED + f"\n> Sequencing is deactivated, job status wont be verified" + Style.RESET_ALL) 
        #if there is a table suffix, a sperator must be added
        if len(table_suffix) > 0 and '$' not in table_suffix :
            table_suffix = "_" + str(table_suffix)
            
        table_id = f'{endpoint.project}.{endpoint.dataset}.{endpoint.table}{table_suffix}'
        
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
    #         schema=[
    #             # Specify the type of columns whose type cannot be auto-detected. For
    #             # example the "title" column uses pandas dtype "object", so its
    #             # data type is ambiguous.
    #             bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
    #             # Indexes are written if included in the schema by name.
    #             bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
    #         ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition=write_disposition,
        )
        print(f'> Exporting dataframe to table {table_id}')
        job = self.client.load_table_from_dataframe(
            self.dataframe, table_id, job_config=job_config
        )  # Make an API request.
        if sequence:
            
            job = self.client.get_job(
                job.job_id, location=job.location
            )  # Make an API request.
        
            self._check_job_state(job)
            self._retrieve_job_metadata(job)        
            table = self.client.get_table(table_id)  # Make an API request.

            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )
            return(job)

#-------------------------------
#          Storage
#------------------------------- 

class Bucket:
    def __init__(self, client, bucket_name:str):
        self.bucket_name = bucket_name
        self.client = client
    
    def blob_exists(self, filename:str):
        '''
        Checks if a file exists in the bucket
        '''

        print(f"Checking {filename} in {self.bucket_name}")
        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.blob(filename)
        return (blob.exists())

    #TODO
    def upload_blob(self, source_file_name:str, destination_blob_name:str):
        '''
        Uploads a file to the bucket
        '''
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The path to your file to upload
        # source_file_name = "local/path/to/file"
        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        return(
            "File {} uploaded to {} GCS directory.".format(
                source_file_name, destination_blob_name
            )
        )

    def download_blob(self, destination_blob_name, destination_file_name):
        '''
        Downloads a file to the bucket
        '''
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The path to your file to upload
        # source_file_name = "local/path/to/file"
        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.download_to_filename(destination_file_name)

        return(
            "GCS file {} downloaded to {}.".format(
                destination_blob_name, destination_file_name
            )
        )

    def list_files(self, prefix=None):
        '''
        Lists all the files contained in a bucket
        '''
        list_blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return([blob.name for blob in list_blobs])