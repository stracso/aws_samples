# Glue Job Monitoring - Custom CloudWatch Dashboard

This replicates the built-in AWS Glue console **Monitoring** page as a custom CloudWatch dashboard, including:

- **Job Runs Summary** — Total runs, Running, Cancelled, Successful, Failed, Success rate, DPU hours
- **Job Runs Table** — All recent job runs with name, status, type, start/end time, run time, capacity, worker type, DPU hours
- **Resource Usage** — Quota utilization for Glue resources
- **Job Type Breakdown** — Success/Failed/Running counts grouped by job type
- **Worker Type Breakdown** — Success/Failed/Running counts grouped by worker type (G.1X, G.2X, etc.)
- **Job Runs Timeline** — Success/Failed/Running/Cancelled counts over time

## Architecture

The Glue console Monitoring page pulls data from the Glue API (not CloudWatch metrics), so standard metric widgets can't replicate it. This solution uses **CloudWatch Custom Widgets** backed by a Lambda function that queries the Glue API and renders HTML.

## Deployment Steps

### 1. Deploy the Lambda Function

```bash
# Zip the Lambda code
zip glueMonitorWidget.zip glueMonitorWidget.py

# Create the Lambda function
aws lambda create-function \
  --function-name glueMonitorWidget \
  --runtime python3.12 \
  --handler glueMonitorWidget.lambda_handler \
  --role arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME> \
  --zip-file fileb://glueMonitorWidget.zip \
  --timeout 60 \
  --memory-size 256
```

### 2. IAM Role Permissions

The Lambda execution role needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetJobs",
        "glue:GetJob",
        "glue:GetJobRuns",
        "glue:ListJobs"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "servicequotas:GetServiceQuota"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

### 3. Create the Dashboard

Replace `${GlueMonitorWidgetLambdaArn}` in `jobDashboard_console.json` with your Lambda ARN, then:

```bash
# Replace placeholder with actual ARN
LAMBDA_ARN="arn:aws:lambda:<REGION>:<ACCOUNT_ID>:function:glueMonitorWidget"
sed "s|\${GlueMonitorWidgetLambdaArn}|${LAMBDA_ARN}|g" jobDashboard_console.json > /tmp/dashboard.json

# Create the dashboard
aws cloudwatch put-dashboard \
  --dashboard-name "GlueJobMonitoring" \
  --dashboard-body file:///tmp/dashboard.json
```

Or paste the contents of `jobDashboard_console.json` (after replacing the ARN) directly into the CloudWatch console dashboard source editor.

### 4. Replace `${AWS::Region}`

Also replace `${AWS::Region}` with your actual region (e.g., `us-east-1`) in the dashboard JSON if not using CloudFormation.

## Files

| File | Purpose |
|------|---------|
| `jobDashboard_console.json` | Dashboard JSON (paste into CloudWatch console source editor) |
| `jobDashboard.json` | Dashboard JSON wrapped for `put-dashboard` API / CloudFormation |
| `glueMonitorWidget.py` | Lambda function powering the custom widgets |
| `jobMonitorPolicies.json` | IAM policy for read-only Glue monitoring access |

## Notes

- The dashboard respects the time range selector in CloudWatch — the Lambda receives the selected time window and filters job runs accordingly.
- Custom widgets refresh on dashboard load and when you change the time range.
- The job runs table is limited to the 50 most recent runs for performance.
- For accounts with many jobs, consider adding pagination or filtering by job name prefix.
