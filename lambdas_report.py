"""
Enumerates Lambda functions with roles, and policies
"""

from __future__ import print_function
from datetime import datetime
import argparse
import codecs
import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
from terminaltables import AsciiTable
import progressbar
import json

from typing import Dict, List

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
BYTE_TO_MB = 1024.0 * 1024.0


ALL_TABLE_HEADERS = [
    'Region',
    'Function',
    'Role Name',
    'Policies'
]

SORT_KEYS = ['region']


def list_available_lambda_regions():
    """
    Enumerates list of all Lambda regions
    :return: list of regions
    """
    session = Session()
    return session.get_available_regions('lambda')

def init_boto_client(client_name, region, args):
    """
    Initiates boto's client object
    :param client_name: client name
    :param region: region name
    :param args: arguments
    :return: Client
    """
    if args.token_key_id and args.token_secret:
        #print(f'create lambda client with key_id ={args.token_key_id} and key_secret={args.token_secret}')
        boto_client = boto3.client(
            client_name,
            aws_access_key_id=args.token_key_id,
            aws_secret_access_key=args.token_secret,
            region_name=region
        )
    elif args.profile:
        #print(f'create lambda client with profile = {args.profile}')
        session = boto3.session.Session(profile_name=args.profile)
        boto_client = session.client(client_name, region_name=region)
    else:
        #print(f'create lambda client with defaults')
        boto_client = boto3.client(client_name, region_name=region)

    return boto_client


def create_tables(lambdas_data, args):
    """
    Create the output tables
    :param lambdas_data: a list of the Lambda functions and their data
    :param args: argparse arguments
    :return: textual table-format information about the Lambdas
    """
    #all_table_data = [ALL_TABLE_HEADERS]
    all_table_data = []
    all_list_data = []
    for lambda_data in lambdas_data:
        function_data = lambda_data['function-data']

        all_table_data.append([
            lambda_data['region'],
            str(function_data['FunctionName']),
            lambda_data['rolename'],
            lambda_data['policies']
        ])

        all_list_data.append({
            'Region': lambda_data['region'],
            'FunctionName': str(function_data['FunctionName']),
            'RoleName': lambda_data['rolename'],
            'Policies': lambda_data['policies']
        })

    return all_list_data, all_table_data


def get_name_from_arn(arn):
    return arn.split('/')[-1]


def get_policies_for_roles(client_iam, role_names: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """ Create a mapping of role names and any policies they have attached to them by 
        paginating over list_attached_role_policies() calls for each role name. 
        Attached policies will include policy name and ARN.
    """
    policy_map = {}
    policy_paginator = client_iam.get_paginator('list_attached_role_policies')
    inline_policy_paginator = client_iam.get_paginator('list_role_policies')
    for name in role_names:
        role_policies = []

        print(f"***CHECK ATTACHED POLICIES for role: {name}****")
        for response in policy_paginator.paginate(RoleName=name):
            #print(response)
            rp = response.get('AttachedPolicies')
            print(rp)
            role_policies.extend(rp)
        
        print(f"***CHECK INLINE POLICYIESfor role: {name}****")
        for response in inline_policy_paginator.paginate(RoleName=name):
            #print(response)
            policy_names = response.get('PolicyNames')
            for pn in policy_names:
                print(f"***HAS INLINE POLICY: {pn}****")
                role_policies.extend([{"PolicyName":pn, "PolicyArn": None}])

        policy_map.update({name: role_policies})


    return policy_map


def get_policy_body_by_arn(client_iam, policy_arn):
    version_id = client_iam.get_policy(
            PolicyArn=policy_arn)['Policy']['DefaultVersionId']    
    response = client_iam.get_policy_version(
            PolicyArn=policy_arn, VersionId=version_id)
    return response['PolicyVersion']['Document']

def get_policy_inline_body_by_name(client_iam, role_name, policy_name):
    response = client_iam.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name)
    # see if an IAM policy written in YAML in CloudFormation is correctly parsed and returned here as an object
    return response['PolicyDocument']

def print_lambda_list(args):
    """
    Main function
    :return: None
    """
    # regions = list_available_lambda_regions()
    regions = ['us-east-1']
    print('Available regions: ')
    print(regions)
    progress_bar = progressbar.ProgressBar(max_value=len(regions))
    lambdas_data = []
    for region in progress_bar(regions):
        try:
            print('init boto client lambda in region: '+region)
            lambda_client = init_boto_client('lambda', region, args)
            
            next_marker = None
            response = lambda_client.list_functions()

            iam_client = init_boto_client('iam', region, args)
            while next_marker != '':
                next_marker = ''
                functions = response['Functions']
                if not functions:
                    continue

                for function_data in functions:

                    # Uncomment these lines to show function data
                    '''
                    print('Function Data')
                    print(function_data)
                    '''
                    
                    function_name = function_data['FunctionName']
                    role_arn = function_data['Role']

                    # extract role name from role arn
                    role_name = get_name_from_arn(role_arn)

                    # get all 

                    policy_map = get_policies_for_roles(iam_client, [role_name])

                    print(policy_map)
                    policies = []

                    '''
                    {'role_name':[list of policies]}
                    {'InvokeOtherLambdaRole': [{'PolicyName': 'InvokeOtherLambdaPolicy', 'PolicyArn': 'arn:aws:iam::838635938245:policy/InvokeOtherLambdaPolicy'}, {'PolicyName': 'AWSLambdaLambdaFunctionDestinationExecutionRole-840e09ab-f85d-48fa-ab8e-7e63bac259a0', 'PolicyArn': 'arn:aws:iam::838635938245:policy/service-role/AWSLambdaLambdaFunctionDestinationExecutionRole-840e09ab-f85d-48fa-ab8e-7e63bac259a0'}, {'PolicyName': 'AWSLambdaBasicExecutionRole', 'PolicyArn': 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'}]}
                    '''
                    if (len(policy_map)>0):
                        first_key = list(policy_map.keys())[0]
                        policies = policy_map[first_key]
                    
                    #print(policies)
                    policy_dict = {}
                    policy_list = []
                    for policy in policies:
                        #print(policy)
                        #print(policy['PolicyName'])
                        policy_name = policy['PolicyName']
                        policy_arn = policy['PolicyArn']

                        policy_dict["PolicyName"] = policy_name
                        policy_dict["PolicyArn"] = policy_arn
                        
                        if policy_arn:
                            print('geting document for attached policy: '+policy_arn)
                            if 'arn:aws:iam::aws:policy' in policy_arn:    
                                policy_dict["PolicyDocument"] = "built-in"
                            else:
                                policy_dict["PolicyDocument"] = get_policy_body_by_arn(iam_client, policy_arn)
                        else:
                            print('geting document for inline policy: '+policy_name)
                            policy_dict["PolicyDocument"] = get_policy_inline_body_by_name(iam_client, role_name, policy_name)
                            
                        policy_list.append(policy_dict)


                    lambdas_data.append({
                        'region': region,
                        'function-data': function_data,
                        'rolename': role_name,
                        'policies': policy_list
                    })

                # Verify if there is next marker
                if 'NextMarker' in response:
                    next_marker = response['NextMarker']
                    response = lambda_client.list_functions(Marker=next_marker)
        except Exception as exc:
            print(str(exc))
    

    # Sort data by the given key (default: by region)
    lambdas_data.sort(key=lambda x: x[args.sort_by])
    
    all_list_data, all_table_data = create_tables(lambdas_data, args)
    
    table = AsciiTable(all_table_data)
    #print(table.table)

    if not args.json:
        return

    with open(args.json, 'w', encoding='utf-8') as outfile:
        json.dump(all_list_data, outfile, indent=4)


if __name__ == '__main__':


    parser = argparse.ArgumentParser(
        description=(
            'Enumerates Lambda functions with its role and policies '            
        )
    )

    
    parser.add_argument(
        '--json',
        type=str,
        help='JSON filename to output full table data.',
        metavar='output_filename'
    )

    parser.add_argument(
        '--token-key-id',
        type=str,
        help=(
            'AWS access key id. Must provide AWS secret access key as well '
            '(default: from local configuration).'
        ),
        metavar='token-key-id'
    )
    parser.add_argument(
        '--token-secret',
        type=str,
        help=(
            'AWS secret access key. Must provide AWS access key id '
            'as well (default: from local configuration.'
        ),
        metavar='token-secret'
    )


    parser.add_argument(
        '--sort-by',
        type=str,
        help=(
            'Column name to sort by. Options: region, '
            'last-modified, last-invocation, '
            'runtime (default: region).'
        ),
        default='region',
        metavar='sort_by'
    )

    parser.add_argument(
        '--profile',
        type=str,
        help=(
            'AWS profile. Optional '
            '(default: "default" from local configuration).'
        ),
        metavar='profile'
    )

    arguments = parser.parse_args()
    print_lambda_list(arguments)