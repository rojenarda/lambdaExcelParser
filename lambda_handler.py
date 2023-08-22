# import os
import io
import json
import boto3
import traceback
import pandas as pd


# test data start
class os:
    environ = {
        'BUCKET_NAME': 'cdl-compliance-checker-excel-test-bucket',
        'EXCEL_FILE_KEY': 'test.xlsx',
        'TABLE_NAME': 'excel-test-table'
    }
# test data end

def parse_json(json: dict, index_key: str, *args) -> list:
    '''
    Parses json created by pandas.
    Provide column names to be extracted
    :param json: dict created by pandas
    :param index_key: reference  column to parse json 
    :param args: additional column names to be extracted
    :return: parsed list of dictionaries in the format of:
    [
        {index_key: value, args[0]: value, ...},
        ...
    ]
    '''
    
    formatted_json = list()
    
    for index in json[index_key]:
        new_row = dict()
        new_row[index_key] = json[index_key][index]
        for col in args:
            if col not in json:
                raise Exception(f'Column {col} not found in json')

            # Data correctness checks
            if col == 'email' and '@' not in json[col][index]:
                raise Exception(f'Invalid email: {json[col][index]}')

            new_row[col] = json[col][index]
        formatted_json.append(new_row)
        
    return formatted_json

def write_to_dynamodb(items: list):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    with table.batch_writer() as batch:
        for item in items:
            reponse = batch.put_item(Item=item)
            
        

def lambda_handler():
    try:
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb')
        response = s3.get_object(
            Bucket=os.environ['BUCKET_NAME'],
            Key=os.environ['EXCEL_FILE_KEY']
        )
        body = response['Body'].read()
        pandas_json = json.loads(pd.read_excel(io.BytesIO(body), dtype=str).to_json())
        # keys: username, email
        formatted_json = parse_json(pandas_json, 'username', 'email')
        write_to_dynamodb(formatted_json)
    except:
        print(traceback.format_exc())

if __name__ == '__main__':
    lambda_handler()
