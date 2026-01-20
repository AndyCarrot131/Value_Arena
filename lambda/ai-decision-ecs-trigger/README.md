# ECS Workflow Trigger Lambda

Lambda function to trigger AI Decision ECS tasks on a schedule using EventBridge.

## Architecture

```
EventBridge Rule → Lambda Function → ECS Fargate Task
```

## Workflows Supported

| Workflow | Schedule | Description |
|----------|----------|-------------|
| `daily_learning` | Daily 23:00 UTC | Learning period analysis (Day 1-7) |
| `hourly_news_analysis` | Every hour | Analyze recent news without RAG |
| `daily_summary` | Daily 23:00 UTC | Generate daily summary with RAG |
| `trading_decision` | Mon-Fri 13:30 UTC | Make trading decisions (09:30 EST) |
| `weekly_report` | Friday 23:00 UTC | Weekly report + stock analysis |

## Deployment Steps

### 1. Create IAM Role

```bash
chmod +x create-iam-role.sh
./create-iam-role.sh
```

This creates the `lambda-ecs-trigger-role` with permissions to:
- Run ECS tasks
- Pass IAM roles to ECS
- Write CloudWatch logs

### 2. Deploy Lambda Function

```bash
chmod +x deploy.sh
./deploy.sh
```

This creates/updates the `ai-decision-ecs-trigger` Lambda function.

### 3. Create EventBridge Rules

```bash
chmod +x eventbridge-rules.sh
./eventbridge-rules.sh
```

This creates 5 EventBridge rules that trigger the Lambda on schedule.

## Manual Testing

Test individual workflows:

```bash
# Test trading decision
aws lambda invoke \
  --function-name ai-decision-ecs-trigger \
  --payload '{"workflow":"trading_decision","agent_id":"all"}' \
  response.json \
  --region us-east-1

# Test daily learning for specific agent
aws lambda invoke \
  --function-name ai-decision-ecs-trigger \
  --payload '{"workflow":"daily_learning","agent_id":"agent_001"}' \
  response.json \
  --region us-east-1
```

## Event Payload Format

```json
{
  "workflow": "trading_decision",  // Required
  "agent_id": "all"                 // Optional, defaults to "all"
}
```

Valid workflows:
- `daily_learning`
- `hourly_news_analysis`
- `daily_summary`
- `trading_decision`
- `weekly_report`

Valid agent_ids:
- `all` - Run for all enabled agents
- `agent_001` - Claude Analyst
- `agent_002` - GPT Trader
- `agent_003` - Gemini Investor

## Monitoring

### CloudWatch Logs

Lambda logs: `/aws/lambda/ai-decision-ecs-trigger`
ECS task logs: `/ecs/ai-decision`

### View EventBridge Rules

```bash
aws events list-rules --region us-east-1
```

### Disable a Rule

```bash
aws events disable-rule --name trading-decision-schedule --region us-east-1
```

### Enable a Rule

```bash
aws events enable-rule --name trading-decision-schedule --region us-east-1
```

## Troubleshooting

### Lambda fails to start ECS task

Check:
1. IAM role has `ecs:RunTask` permission
2. IAM role can pass `ecsTaskExecutionRole` and `ai-decision-task-role`
3. ECS cluster and task definition exist
4. Network configuration (subnets, security groups) is correct

### ECS task starts but fails immediately

Check ECS task logs in CloudWatch: `/ecs/ai-decision`

Common issues:
- Missing environment variables
- Database connection failures
- Bedrock permission errors

## Cost Optimization

- Lambda: Minimal cost (~$0.20/month for 5 rules)
- ECS Fargate: ~$0.04 per task-hour
- EventBridge: First 1 million events free

Estimated monthly cost: **~$10-20** depending on task duration.
