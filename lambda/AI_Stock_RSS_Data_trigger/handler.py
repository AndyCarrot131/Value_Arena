"""
Lambda Function to Trigger ECS Tasks for RSS Data Services
Supports 3 services: rss-collector, news-classifier, financial-reports
"""

import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ECS Configuration
ECS_CLUSTER = os.environ.get('ECS_CLUSTER', 'ai-stock-war-cluster')
SUBNETS = os.environ.get('SUBNETS', 'subnet-04fa1412b6915005b,subnet-00cf6fe01544e3d35').split(',')
SECURITY_GROUPS = os.environ.get('SECURITY_GROUPS', 'sg-030a73aaeff1f852d').split(',')
ASSIGN_PUBLIC_IP = os.environ.get('ASSIGN_PUBLIC_IP', 'DISABLED')

# Task Definition mapping
TASK_DEFINITIONS = {
    "rss-collector": os.environ.get('TASK_DEF_RSS_COLLECTOR', 'ai-stock-rss-collector'),
    "news-classifier": os.environ.get('TASK_DEF_NEWS_CLASSIFIER', 'ai-stock-news-classifier'),
    "financial-reports": os.environ.get('TASK_DEF_FINANCIAL_REPORTS', 'ai-stock-financial-reports')
}

# Container name mapping
CONTAINER_NAMES = {
    "rss-collector": "rss-collector",
    "news-classifier": "news-classifier",
    "financial-reports": "financial-reports"
}

# Valid jobs per service
VALID_JOBS = {
    "rss-collector": ['collect', 'deduplicate', 'all'],
    "news-classifier": ['classify'],
    "financial-reports": ['collect', 'extract', 'summary', 'all']
}

# Initialize ECS client
ecs_client = boto3.client('ecs', region_name='us-east-1')


def lambda_handler(event, context):
    """
    Lambda handler to trigger ECS task

    Event structure:
    {
        "service": "rss-collector" | "news-classifier" | "financial-reports",
        "job": "collect" | "classify" | "extract" | "summary" | "all",  # depends on service
        "test_mode": false,  # Optional: default false
        "batch_size": 100    # Optional: for news-classifier only
    }
    """
    try:
        # Parse event
        service = event.get('service')
        job = event.get('job', 'all')
        test_mode = event.get('test_mode', False)
        batch_size = event.get('batch_size', 100)

        if not service:
            raise ValueError("Missing required parameter: service")

        # Validate service
        if service not in TASK_DEFINITIONS:
            raise ValueError(f"Invalid service: {service}. Must be one of {list(TASK_DEFINITIONS.keys())}")

        # Validate job for service
        valid_jobs = VALID_JOBS.get(service, [])
        if job not in valid_jobs:
            raise ValueError(f"Invalid job '{job}' for service '{service}'. Must be one of {valid_jobs}")

        task_definition = TASK_DEFINITIONS[service]
        container_name = CONTAINER_NAMES[service]

        logger.info(f"Triggering ECS task: service={service}, job={job}, test_mode={test_mode}")

        # Build command
        command = [
            '--job', job,
            '--log-level', 'INFO'
        ]

        # Add test-mode flag if enabled
        if test_mode:
            command.append('--test-mode')

        # Add batch-size for news-classifier
        if service == 'news-classifier' and batch_size:
            command.extend(['--batch-size', str(batch_size)])

        # Run ECS task
        response = ecs_client.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=task_definition,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': SUBNETS,
                    'securityGroups': SECURITY_GROUPS,
                    'assignPublicIp': ASSIGN_PUBLIC_IP
                }
            },
            overrides={
                'containerOverrides': [
                    {
                        'name': container_name,
                        'command': command
                    }
                ]
            }
        )

        # Extract task information
        tasks = response.get('tasks', [])
        failures = response.get('failures', [])

        if failures:
            logger.error(f"ECS task failures: {failures}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'message': 'Failed to start ECS task',
                    'failures': failures
                })
            }

        if tasks:
            task_arn = tasks[0]['taskArn']
            task_id = task_arn.split('/')[-1]

            logger.info(f"ECS task started successfully: {task_id}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': True,
                    'message': 'ECS task started successfully',
                    'service': service,
                    'job': job,
                    'test_mode': test_mode,
                    'task_arn': task_arn,
                    'task_id': task_id
                })
            }
        else:
            logger.error("No tasks started and no failures reported")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'message': 'Unknown error: no tasks started'
                })
            }

    except Exception as e:
        logger.error(f"Error triggering ECS task: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': str(e)
            })
        }
