import collections

import boto3
import yaml

dry_run = False


def check_rds_status(rds):
    rds_client = boto3.client('rds')
    response = rds_client.describe_db_instances(
        DBInstanceIdentifier=rds
    )
    return response['DBInstances'][0]['DBInstanceStatus']


def remove_stopped(instances):
    instances_to_operate = instances
    for instance in instances:
        if check_rds_status(instance) in ['stopped', 'stopping']:
            instances_to_operate.remove(instance)

    return instances_to_operate


def stop_instances(rds_list):
    print('Stopping the RDS instances...')
    rds_list = remove_stopped(rds_list)
    print(','.join(rds_list))
    for rds in rds_list:
        stop_rds_instance(rds)


def get_tags_to_filter():
    with open('tags_filter.yaml', 'r') as tags_file:
        tags = yaml.load(tags_file, Loader=yaml.FullLoader)
    return tags['aws_tags']


def get_rds_instances_by_tag(tags):
    rds_client = boto3.client('rds')
    response = rds_client.describe_db_instances()
    rds_to_operate = []

    for tag in tags:
        for key, value in tag.items():
            for instance in response['DBInstances']:
                for rds_tag in instance['TagList']:
                    if rds_tag['Key'] == key and rds_tag['Value'] == value:
                        rds_to_operate.append(instance['DBInstanceIdentifier'])
                else:
                    continue
    rds_to_operate = [item for item, count in collections.Counter(rds_to_operate).items() if count == len(tags)]
    return rds_to_operate


def stop_rds_instance(rds):
    rds_client = boto3.client('rds')
    print('Stopping %s' % rds)
    rds_client.stop_db_instance(
        DBInstanceIdentifier=rds
    )


def lambda_handler(event, context):
    tags = get_tags_to_filter()
    rds_list = get_rds_instances_by_tag(tags)

    stop_instances(rds_list)
