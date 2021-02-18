import datetime
import time
import boto3


def check_instance_status(service, instance):
    try:
        client = boto3.client(service)
        if service == 'ec2':
            response = client.describe_instances(
                InstanceIds=[instance]
            )
            return response['Reservations'][0]['Instances'][0]['State']['Name']
        elif service == 'rds':
            response = client.start_db_instance(
                DBInstanceIdentifier=instance
            )
            return response['DBInstances'][0]['DBInstanceStatus']
        return ''
    except Exception as e:
        print('No action taken: %s' % e)
        return ''


def remove_instance(service, instances, statuses):
    instances_to_operate = []
    for instance in instances:
        if not check_instance_status(service, instance) in statuses:
            instances_to_operate.append(instance)

    return instances_to_operate


def wait_until(condition, timeout, period=5, *args):
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    while datetime.datetime.now() < deadline:
        if condition(*args):
            return
        time.sleep(period)
    raise ValueError('%s is incorrect.' % condition)


def start_instance(service, instance):
    response = ''
    try:
        print('Starting %s' % instance)
        client = boto3.client(service)
        if service == 'ec2':
            response = client.start_instances(
                InstanceIds=[instance]
            )
        elif service == 'rds':
            response = client.start_db_instance(
                DBInstanceIdentifier=instance
            )
        return response
    except Exception as e:
        print('No action taken: %s' % e)
        return False


def instances_are_on(service, instances):
    for instance in instances:
        if check_instance_status(service, instance) not in ['running', 'available']:
            print('%s is still in starting' % instance)
            return False
    return True


def stop_instance(service, instance):
    response = ''
    try:
        print('Stopping %s' % instance)
        client = boto3.client(service)
        if service == 'ec2':
            response = client.stop_instances(
                InstanceIds=[instance]
            )
        elif service == 'rds':
            response = client.stop_db_instance(
                DBInstanceIdentifier=instance
            )
        return response
    except Exception as e:
        print('No action taken: %s' % e)
        return False


def instances_are_off(service, instances):
    for instance in instances:
        if check_instance_status(service, instance) != 'stopped':
            print('%s is still in stopping' % instance)
            return False
    return True


def start_instances(service, instances):
    print('Starting the {} instances...'.format(service))
    instances = remove_instance(service, instances, ['running', 'available'])
    print(','.join(instances))
    for instance in instances:
        start_instance(service, instance)

    wait_until(instances_are_on, 800, 20, service, instances)


def stop_instances(service, instances):
    print('Stopping the {} instances...'.format(service))
    instances = remove_instance(service, instances, ['stopping', 'stopped'])
    print(','.join(instances))
    for instance in instances:
        stop_instance(service, instance)

    wait_until(instances_are_off, 800, 20, service, instances)


def lambda_handler(event, context):
    if event['is_start']:
        start_instances(event['instance_type'], event['instance_ids'])
    else:
        stop_instances(event['instance_type'], event['instance_ids'])
