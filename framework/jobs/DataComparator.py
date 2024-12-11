# import datacompy
# import pandas as pd
# from framework.engines.athena import athena
# from framework.engines.jdbc import jdbc
# from framework.factory.ClassFactory import ClassFactory as cf
# import framework.utils.LoggerUtils as logger
# import json
# import boto3
# from botocore.exceptions import BotoCoreError, ClientError

# class DataComparator(object):
    # @staticmethod
    # def save_comparison_result_to_s3(result_dict, s3_path, logger):
    #     """
    #     Save the data comparison result as a JSON file to the given S3 path.

    #     Parameters:
    #         result_dict (dict): Dictionary containing comparison results.
    #         s3_path (str): S3 path where the JSON file should be saved (e.g., 's3://bucket-name/folder/file.json').
    #         logger: Logger instance for logging.

    #     Returns:
    #         bool: True if the file is successfully saved, False otherwise.
    #     """
    #     try:
    #         if not result_dict:
    #             raise ValueError("The result_dict cannot be empty.")
    #         if not isinstance(result_dict, dict):
    #             raise ValueError("The result_dict must be a dictionary.")
    #         if not s3_path or not s3_path.startswith("s3://"):
    #             raise ValueError("Invalid S3 path. It must start with 's3://'.")
            
    #         # Parse the S3 bucket and key from the s3_path
    #         s3_parts = s3_path.replace("s3://", "").split("/", 1)
    #         if len(s3_parts) != 2:
    #             raise ValueError("Invalid S3 path format. It must include both bucket and key.")

    #         bucket_name, object_key = s3_parts
            
    #         # Convert result_dict to JSON string
    #         json_data = json.dumps(result_dict, indent=4)
    #         logger.info("Converted result_dict to JSON format.")
            
    #         # Initialize S3 client
    #         s3_client = boto3.client("s3")
    #         logger.info(f"Attempting to upload JSON to S3: {s3_path}")

    #         # Upload JSON to S3
    #         s3_client.put_object(
    #             Bucket=bucket_name,
    #             Key=object_key,
    #             Body=json_data,
    #             ContentType="application/json"
    #         )
    #         logger.info(f"Successfully uploaded comparison result to {s3_path}.")
    #         return True
        
    #     except ValueError as ve:
    #         logger.error(f"ValueError: {ve}")
    #     except (BotoCoreError, ClientError) as boto_err:
    #         logger.error(f"An error occurred while interacting with S3: {boto_err}")
    #     except Exception as ex:
    #         logger.error(f"An unexpected error occurred: {ex}")
        
    #     return False    




#     @staticmethod
#     def compare_dataframes(details):
#         src_instance = cf.getEngineClass(details['src'])
#         tar_instance = cf.getEngineClass(details['dest'])

#         df_src = src_instance.get_data(details['src_table_name'])
#         df_dest = tar_instance.get_data(details['tar_table_name'])

#         if not df_src.index.equals(df_dest.index):
#             logger.warning("The DataFrames do not have the same index.")
        
#         comparison = datacompy.Compare(
#             df_src,
#             df_dest,
#             join_columns='key',  
#             df1_name=f"{details['table_name']}_src",
#             df2_name=f"{details['table_name']}_dest"
#         )
#         logger.info(comparison.report())
        # comparison_result = {
        #     "match": comparison.matches(),
        #     "mismatch_count": comparison.intersect_rows_mismatch,
        #     "df1_unmatched_rows": comparison.df1_unq_rows,
        #     "df2_unmatched_rows": comparison.df2_unq_rows,
        #     "df1_columns_missing_in_df2": comparison.df1.columns.difference(comparison.df2.columns).tolist(),
        #     "df2_columns_missing_in_df1": comparison.df2.columns.difference(comparison.df1.columns).tolist()
        # }

#         return comparison_result


##########################


# usage
# Example S3 path
# s3_path = "s3://my-bucket/data-comparison-results/result.json"

# # Save comparison result to S3
# result_saved = DataComparisonUtils.save_comparison_result_to_s3(comparison_result, s3_path, logger)
# if result_saved:
#     logger.info("Comparison result saved successfully.")
# else:
#     logger.error("Failed to save comparison result.")


###########################

# example result stored in s3


# {
#     "match": true,
#     "mismatch_count": 5,
#     "df1_unmatched_rows": [
#         {
#             "id": 1,
#             "value": "A"
#         }
#     ],
#     "df2_unmatched_rows": [
#         {
#             "id": 1,
#             "value": "B"
#         }
#     ],
#     "df1_columns_missing_in_df2": [
#         "extra_col_1"
#     ],
#     "df2_columns_missing_in_df1": [
#         "extra_col_2"
#     ]
# }
