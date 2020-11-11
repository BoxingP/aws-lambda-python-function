# -*- encoding:utf-8 -*-
import datetime
import time

import boto3

tag_key = 'schedule'
tag_value = 'yes'
ec2_client = boto3.client('ec2')
rds_client = boto3.client('rds')
dry_run = False


def check_ec2_status(ec2):
    response = ec2_client.describe_instances(
        InstanceIds=[ec2]
    )
    return response['Reservations'][0]['Instances'][0]['State']['Code']


def check_rds_status(rds):
    response = rds_client.describe_db_instances(
        DBInstanceIdentifier=rds
    )
    return response['DBInstances'][0]['DBInstanceStatus']


def remove_running(service, instances):
    instances_to_operate = instances
    for instance in instances:
        if service == 'ec2' and check_ec2_status(instance) == 16:
            instances_to_operate.remove(instance)
        elif service == 'rds' and check_rds_status(instance) == 'available':
            instances_to_operate.remove(instance)

    return instances_to_operate


def remove_stopped(service, instances):
    instances_to_operate = instances
    for instance in instances:
        if service == 'ec2' and check_ec2_status(instance) in [80, 64]:
            instances_to_operate.remove(instance)
        elif service == 'rds' and check_rds_status(instance) in ['stopped', 'stopping']:
            instances_to_operate.remove(instance)

    return instances_to_operate


def wait_until(condition, timeout, period=5, *args):
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    while datetime.datetime.now() < deadline:
        if condition(*args):
            return
        time.sleep(period)
    raise ValueError('%s is incorrect.' % condition)


def rds_is_on(instances):
    for instance in instances:
        if check_rds_status(instance) != 'available':
            print('%s is still in starting' % instance)
            return False
    return True


def ec2_is_off(instances):
    for instance in instances:
        if check_ec2_status(instance) != 80:
            print('%s is still in stopping' % instance)
            return False
    return True


def start_instances(ec2_list, rds_list):
    print('Starting the RDS instances...')
    rds_list = remove_running('rds', rds_list)
    print(','.join(rds_list))
    for rds in rds_list:
        start_rds_instance(rds)

    wait_until(rds_is_on, 800, 20, rds_list)

    print('Starting the EC2 instances...')
    ec2_list = remove_running('ec2', ec2_list)
    print(','.join(ec2_list))
    for ec2 in ec2_list:
        start_ec2_instance(ec2)


def stop_instances(ec2_list, rds_list):
    print('Stopping the EC2 instances...')
    ec2_list = remove_stopped('ec2', ec2_list)
    print(','.join(ec2_list))
    for ec2 in ec2_list:
        stop_ec2_instance(ec2)

    wait_until(ec2_is_off, 800, 20, ec2_list)

    print('Stopping the RDS instances...')
    rds_list = remove_stopped('rds', rds_list)
    print(','.join(rds_list))
    for rds in rds_list:
        stop_rds_instance(rds)


def get_ec2_instances_by_tag(key, value):
    response = ec2_client.describe_instances(
        Filters=[
            {
                'Name': 'tag:' + key,
                'Values': [value]
            }
        ]
    )

    ec2_to_operate = []
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            ec2_to_operate.append(instance['InstanceId'])

    return ec2_to_operate


def get_rds_instances_by_tag(key, value):
    response = rds_client.describe_db_instances()
    rds_to_operate = []
    for instance in response['DBInstances']:
        for tag in instance['TagList']:
            if tag['Key'] == key and tag['Value'] == value:
                rds_to_operate.append(instance['DBInstanceIdentifier'])
        else:
            continue
    return rds_to_operate


def start_ec2_instance(ec2):
    try:
        print('Starting %s' % ec2)
        response = ec2_client.start_instances(
            InstanceIds=[ec2],
            DryRun=dry_run
        )

    except Exception as e:
        print('No action taken: %s' % e)
        return False

    else:
        return response


def start_rds_instance(rds):
    print('Starting %s' % rds)
    rds_client.start_db_instance(
        DBInstanceIdentifier=rds
    )


def stop_ec2_instance(ec2):
    try:
        print('Stopping %s' % ec2)
        response = ec2_client.stop_instances(
            InstanceIds=[ec2],
            DryRun=dry_run
        )

    except Exception as e:
        print('No action taken: %s' % e)
        return False

    else:
        return response


def stop_rds_instance(rds):
    print('Stopping %s' % rds)
    rds_client.stop_db_instance(
        DBInstanceIdentifier=rds
    )


def policy():
    china_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    if china_now.time() <= datetime.time(12, 00):
        return True
    else:
        return False


def lambda_handler(event, context):
    ec2_list = get_ec2_instances_by_tag(tag_key, tag_value)
    rds_list = get_rds_instances_by_tag(tag_key, tag_value)

    if policy():
        start_instances(ec2_list, rds_list)
    else:
        stop_instances(ec2_list, rds_list)
