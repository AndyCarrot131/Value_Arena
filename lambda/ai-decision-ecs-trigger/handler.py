"""
Lambda Function to Trigger ECS Task for AI Decision Workflows
Supports 5 workflows: hourly_news_analysis, daily_summary, trading_decision, weekly_summary, stock_analysis
"""

import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ECS Configuration
ECS_CLUSTER = os.environ.get('ECS_CLUSTER', 'ai-stock-war-cluster')
TASK_DEFINITION = os.environ.get('TASK_DEFINITION', 'ai-decision-service')
SUBNETS = os.environ.get('SUBNETS', 'subnet-04fa1412b6915005b,subnet-00cf6fe01544e3d35').split(',')
SECURITY_GROUPS = os.environ.get('SECURITY_GROUPS', 'sg-030a73aaeff1f852d').split(',')
ASSIGN_PUBLIC_IP = os.environ.get('ASSIGN_PUBLIC_IP', 'DISABLED')

# Initialize ECS client
ecs_client = boto3.client('ecs', region_name='us-east-1')


def lambda_handler(event, context):
    """
    Lambda handler to trigger ECS task

    Event structure:
    {
        "workflow": "trading_decision",  # Required: workflow name
        "agent_id": "all",               # Optional: default "all"
        "test_mode": false,              # Optional: test mode (no DB writes), default false
        "symbols": ["AAPL", "GOOGL"]     # Optional: for stock_analysis workflow only
    }
    """
    try:
        # Parse event
        workflow = event.get('workflow')
        agent_id = event.get('agent_id', 'all')
        test_mode = event.get('test_mode', False)
        symbols = event.get('symbols')  # Optional: list of symbols for stock_analysis

        if not workflow:
            raise ValueError("Missing required parameter: workflow")

        # Validate workflow
        valid_workflows = [
            'hourly_news_analysis',
            'daily_summary',
            'trading_decision',
            'weekly_summary',
            'stock_analysis'
        ]

        if workflow not in valid_workflows:
            raise ValueError(f"Invalid workflow: {workflow}. Must be one of {valid_workflows}")

        # Validate symbols parameter
        if symbols and workflow != 'stock_analysis':
            logger.warning(f"symbols parameter ignored for workflow={workflow} (only valid for stock_analysis)")
            symbols = None

        logger.info(f"Triggering ECS task for workflow={workflow}, agent_id={agent_id}, test_mode={test_mode}, symbols={symbols}")

        # Build command
        command = [
            '--workflow', workflow,
            '--agent_id', agent_id,
            '--log-level', 'INFO',
            '--log-format', 'json'
        ]

        # Add test-mode flag if enabled
        if test_mode:
            command.append('--test-mode')

        # Add symbols for stock_analysis workflow
        if symbols and workflow == 'stock_analysis':
            if isinstance(symbols, list):
                symbols_str = ','.join(symbols)
            else:
                symbols_str = str(symbols)
            command.extend(['--symbols', symbols_str])

        # Run ECS task
        response = ecs_client.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=TASK_DEFINITION,
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
                        'name': 'ai-decision',
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

            response_body = {
                'success': True,
                'message': 'ECS task started successfully',
                'workflow': workflow,
                'agent_id': agent_id,
                'test_mode': test_mode,
                'task_arn': task_arn,
                'task_id': task_id
            }

            # Include symbols in response for stock_analysis
            if symbols and workflow == 'stock_analysis':
                response_body['symbols'] = symbols

            return {
                'statusCode': 200,
                'body': json.dumps(response_body)
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
