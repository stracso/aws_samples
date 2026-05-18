"""
Lambda function to process Glue Job State Change events from EventBridge
and publish notifications to an SNS topic.

Enriches the notification with job run arguments and CloudWatch log links
by calling the Glue GetJobRun API.
"""

import json
import os
import urllib.parse
import boto3

sns_client = boto3.client("sns")
glue_client = boto3.client("glue")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def get_job_run_details(job_name, job_run_id):
    """Fetch job run details from Glue to get arguments and log groups."""
    try:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        return response.get("JobRun", {})
    except Exception as e:
        print(f"Failed to get job run details: {e}")
        return {}


def build_cloudwatch_log_link(region, log_group, job_run_id):
    """Build a CloudWatch Logs console URL for the given log group and run."""
    # Log stream name for Glue jobs follows the pattern: jr_<run_id>
    encoded_group = urllib.parse.quote_plus(urllib.parse.quote_plus(log_group))
    encoded_stream = urllib.parse.quote_plus(urllib.parse.quote_plus(job_run_id))
    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region}#logsV2:log-groups/log-group/{encoded_group}"
        f"/log-events/{encoded_stream}"
    )


def lambda_handler(event, context):
    """
    Handles Glue Job State Change events from EventBridge and publishes
    a formatted notification to SNS.

    Expected event structure (EventBridge Glue Job State Change):
    {
        "version": "0",
        "id": "...",
        "detail-type": "Glue Job State Change",
        "source": "aws.glue",
        "account": "123456789012",
        "time": "...",
        "region": "...",
        "resources": [],
        "detail": {
            "jobName": "my-glue-job",
            "severity": "INFO",
            "state": "SUCCEEDED",
            "jobRunId": "jr_...",
            "message": "..."
        }
    }
    """
    account = event.get("account", "unknown")
    region = event.get("region", "unknown")
    detail = event.get("detail", {})

    job_name = detail.get("jobName", "unknown")
    job_run_id = detail.get("jobRunId")
    state = detail.get("state", "unknown")

    # Fetch job run details for arguments and log configuration
    job_run = {}
    if job_run_id:
        job_run = get_job_run_details(job_name, job_run_id)

    # Extract job input arguments (user-defined --params)
    arguments = job_run.get("Arguments", {})

    # Build CloudWatch log links
    output_log_group = job_run.get(
        "LogGroupName", f"/aws-glue/jobs/output"
    )
    error_log_group = job_run.get(
        "ErrorLogGroupName", f"/aws-glue/jobs/error"
    )

    logs = {}
    if job_run_id:
        logs["outputLogLink"] = build_cloudwatch_log_link(
            region, output_log_group, job_run_id
        )
        logs["errorLogLink"] = build_cloudwatch_log_link(
            region, error_log_group, job_run_id
        )

    # Build SNS subject (max 100 chars)
    subject = f"[{account}] Glue Job '{job_name}' - {state}"
    if len(subject) > 100:
        subject = subject[:97] + "..."

    # Build SNS message body with job details
    body = {
        "account": account,
        "region": region,
        "jobName": job_name,
        "state": state,
        "jobRunId": job_run_id,
        "severity": detail.get("severity"),
        "message": detail.get("message"),
        "startedOn": detail.get("startedOn"),
        "completedOn": detail.get("completedOn"),
        "executionTime": job_run.get("ExecutionTime"),
        "eventTime": event.get("time"),
        "arguments": arguments if arguments else None,
        "logs": logs if logs else None,
    }

    # Remove None values for cleaner output
    body = {k: v for k, v in body.items() if v is not None}

    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=json.dumps(body, indent=2),
    )

    message_id = response.get("MessageId")
    print(f"Published SNS message {message_id} for job '{job_name}' state '{state}'")

    return {"statusCode": 200, "messageId": message_id}
