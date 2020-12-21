import boto3


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
    print('{} is suspended.'.format(group_name))
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
    print('{} is resumed.'.format(group_name))
    return response


def lambda_handler(event, context):
    if event['is_suspend']:
        for group in event['group_names']:
            suspend_scaling(group)
    else:
        for group in event['group_names']:
            resume_scaling(group)
