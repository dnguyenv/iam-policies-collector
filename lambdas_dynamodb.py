import boto3
import json
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import And, Attr
from pprint import pprint
from botocore.exceptions import ClientError

class table:
    db = None
    tableName = None
    table = None
    table_created = False

    def __init__(self):
        self.db  = boto3.resource('dynamodb',endpoint_url="http://localhost:8000")
        print("Initialized")
    
    def isTableExits(self, tableName = None):
        try:
            self.tableName = tableName
            self.table = self.db.Table(self.tableName)
            self.table_created = self.table.table_status in ("CREATING", "UPDATING",
                                             "DELETING", "ACTIVE")
            print('Table ' + tableName + ' is existed')
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ResourceNotFoundException':
                print ("Table " + tableName + " does not exist. Create the table first and try again.")
            else:
                print ("Unknown exception occurred while querying for the " + tableName + " table. Printing full error:")
                pprint.pprint(ce.response)
            self.table = None
            self.table_created = False
        return self.table_created

    '''
        Create Table
    '''
    def createTable(self, tableName = None , KeySchema = None, AttributeDefinitions = None, ProvisionedThroughput = None):
        self.tableName = tableName
        try:
            table = self.db.create_table(
            TableName=tableName,
            KeySchema=KeySchema,
            AttributeDefinitions=AttributeDefinitions,
            ProvisionedThroughput=ProvisionedThroughput
            )

            # Wait until the table exists.
            # table.meta.client.get_waiter('table_exists').wait(TableName=tableName)
            table.wait_until_exists()

            self.table = table
            print(f'Created Table {self.table}')
        except:
            self.table = self.db.Table(self.tableName)
            print(f'{self.tableName} exists in db')
    
    '''
        Insert item into table
    '''
    def insert_data(self, path):
        with open(path) as f:
            data = json.load(f)
            for item in data:
                try:
                    self.table.put_item(Item = item)
                except Exception as e:
                    print(e)            
        print(f'Inserted Data into {self.tableName}')
    
    '''
        Get Item from table
    '''
    def getItem(self,key):
        try:
            response = self.table.get_item(Key = key)
            return response['Item']
        except Exception as e:
            print('Item not found')
            return None
    
    def updateItem(self,key,updateExpression, conditionExpression,expressionAttributes):
        try:
            response = self.table.update_item(
                Key = key,
                UpdateExpression = updateExpression,
                ConditionExpression = conditionExpression,
                ExpressionAttributeValues = expressionAttributes
            )
        except Exception as e:
            print(e)
    
    def deleteItem(self, key, conditionExpression, expressionAttributes):
        try:
            response = self.table.delete_item(
                Key = key,
                ConditionExpression = conditionExpression,
                ExpressionAttributeValues = expressionAttributes
            )
        except Exception as e:
            print(e)
    
    def query(self,projectExpression,expressionAttributes,keyExpression):
        try:
            response = self.table.query(
                ProjectionExpression = projectExpression,
                KeyConditionExpression= keyExpression,
            )
            return response['Items']
        except Exception as e:
            print(e)
            return None

    def scan(self, projectExpression = None, expressionAttributes = None, filterExpression = None):
        try:
            scan_kwargs = {}
            done = False
            start_key = None
            items = []
            
            if filterExpression:
                scan_kwargs['FilterExpression'] = filterExpression

            if projectExpression:
                scan_kwargs['ProjectionExpression'] = projectExpression
            
            if expressionAttributes:
                scan_kwargs['ExpressionAttributeNames'] = expressionAttributes

            while not done:
                if start_key:
                    print("Has start key"+start_key)
                    scan_kwargs['ExclusiveStartKey'] = start_key
                response = self.table.scan(**scan_kwargs)
                #print(response)
                items.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
            return items
        except Exception as e:
            print(e)
            return None


'''
    For Testing Purposes
'''
if __name__ == '__main__':
    lambdas = table()
    primaryKey=[
        { 'AttributeName': 'Region', 'KeyType': 'HASH'}, #Partition Key
        { 'AttributeName': 'FunctionName','KeyType': 'RANGE'} #Sort Key
    ] 
    attributeDataType=[
        { 'AttributeName': 'Region', 'AttributeType': 'S'  }, #All String Type
        { 'AttributeName': 'FunctionName', 'AttributeType': 'S' }, #String
    ]
    provisionedThroughput={ 'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10 }
    if not lambdas.isTableExits("LambdaPolicies"):
        lambdas.createTable(
        tableName="LambdaPolicies",
        KeySchema=primaryKey,AttributeDefinitions=attributeDataType,ProvisionedThroughput=provisionedThroughput)
    else:
        print(lambdas.table)
        print(lambdas.table_created)
        print(lambdas.tableName)

    lambdas.insert_data(path = 'lambdas_report.json')
    

    print("Scan all Lambda policies")
    projection = None
    condExp = None
    expAttr = None
    print(lambdas.scan(projection,expAttr,condExp))