from bigquery_uploader import load_df_to_bigquery
from dolar_value import request_dolar_value
from schema import SCHEMA


def main(request):

    try:

        # PROJECT VARIABLES
        BIGQUERY_PROJECT_ID = 'edv-project-304721'
        BIGQUERY_DATASET_ID = 'edv_dataset'
        BIGQUERY_TABLE_NAME = 'dolar_evolution'
        PARTITION_FIELD = 'load_timestamp'
        APPEND_DATA_ON_BIGQUERY = True

        # Request dolar data
        dolar_data = request_dolar_value()

        # Check is the created dataframe is not empty
        if dolar_data is None or len(dolar_data) == 0:
            print('No content, table has 0 rows')
            return ('No content, table has 0 rows', 204)

        # Save on Google Big Query
        print("saving data into Google Big Query....")
        http_status = load_df_to_bigquery(
            project_id=BIGQUERY_PROJECT_ID,
            dataset_id=BIGQUERY_DATASET_ID,
            table_name=BIGQUERY_TABLE_NAME,
            df=dolar_data,
            schema=SCHEMA,
            partition_field=PARTITION_FIELD,
            append=APPEND_DATA_ON_BIGQUERY
        )

        if http_status == 200:
            return ('Successful!', http_status)
        else:
            return ("Error. Please check the logging pannel", http_status)

    except Exception as e:
        error_message = "Error uploading data: {}".format(e)
        print('[ERROR] ' + error_message)
        return (error_message, '400')
