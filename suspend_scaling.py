import boto3
import yaml

from policy import policy


def get_tags_to_filter():
    with open('tags_filter.yaml', 'r') as tags_file:
        tags = yaml.load(tags_file, Loader=yaml.FullLoader)
    return tags['aws_tags']


def get_scaling_group_by_tag(tags):
    group_list = []
    filtered_group = []
    autoscaling_client = boto3.client('autoscaling')
    paginator = autoscaling_client.get_paginator('describe_auto_scaling_groups')
    page_iterator = paginator.paginate(
        PaginationConfig={'PageSize': 100}
    )
    group_filter = 'AutoScalingGroups[]'
    for tag in tags:
        key, value = list(tag.items())[0]
        group_filter = ('{} | [?contains(Tags[?Key==`{}`].Value, `{}`)]'.format(group_filter, key, value))
        filtered_group = page_iterator.search(group_filter)
    for group in filtered_group:
        group_list.append(group['AutoScalingGroupName'])
    return group_list


def suspend_scaling(group_name):
    autoscaling_client = boto3.client('autoscaling')
    response = autoscaling_client.suspend_processes(
        AutoScalingGroupName=group_name,
        ScalingProcesses=[
            'Launch',
            'Terminate',
            'AddToLoadBalancer',
            'AlarmNotification',
            'AZRebalance',
            'HealthCheck',
            'InstanceRefresh',
            'ReplaceUnhealthy',
            'ScheduledActions'
        ]
    )
    return response


def resume_scaling(group_name):
    autoscaling_client = boto3.client('autoscaling')
    response = autoscaling_client.resume_processes(
        AutoScalingGroupName=group_name,
        ScalingProcesses=[
            'Launch',
            'Terminate',
            'AddToLoadBalancer',
            'AlarmNotification',
            'AZRebalance',
            'HealthCheck',
            'InstanceRefresh',
            'ReplaceUnhealthy',
            'ScheduledActions'
        ]
    )
    return response


def lambda_handler(event, context):
    tags = get_tags_to_filter()
    group_list = get_scaling_group_by_tag(tags)

    if policy():
        for group in group_list:
            suspend_scaling(group)
    else:
        for group in group_list:
            resume_scaling(group)
