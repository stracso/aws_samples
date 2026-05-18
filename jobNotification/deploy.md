# Deploy Glue Job Notification Lambda

## Prerequisites

- AWS CLI configured with appropriate credentials
- An existing SNS topic (or create one below)
- Python 3.9+ runtime

## 1. Set Variables

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
FUNCTION_NAME=glue-job-notification
SNS_TOPIC_NAME=glue-job-alerts
ROLE_NAME=glue-job-notification-lambda-role
```

## 2. Create SNS Topic

```bash
SNS_TOPIC_ARN=$(aws sns create-topic --name $SNS_TOPIC_NAME --query TopicArn --output text)
echo "SNS Topic ARN: $SNS_TOPIC_ARN"
```

Subscribe an email address (confirm the subscription from your inbox):

```bash
aws sns subscribe \
  --topic-arn $SNS_TOPIC_ARN \
  --protocol email \
  --notification-endpoint your-email@example.com
```

## 3. Create IAM Role

Create the trust policy:

```bash
cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
```

Create the role:

```bash
aws iam create-role \
  --role-name $ROLE_NAME \
  --assume-role-policy-document file:///tmp/trust-policy.json
```

Attach basic Lambda execution policy:

```bash
aws iam attach-role-policy \
  --role-name $ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

Create and attach SNS publish policy:

```bash
cat > /tmp/sns-publish-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "$SNS_TOPIC_ARN"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name $ROLE_NAME \
  --policy-name sns-publish \
  --policy-document file:///tmp/sns-publish-policy.json
```

Create and attach Glue read policy (needed to fetch job run arguments and log config):

```bash
cat > /tmp/glue-read-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "glue:GetJobRun",
      "Resource": [
        "arn:aws:glue:${REGION}:${ACCOUNT_ID}:job/*"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name $ROLE_NAME \
  --policy-name glue-read \
  --policy-document file:///tmp/glue-read-policy.json
```

Wait a few seconds for the role to propagate:

```bash
sleep 10
```

## 4. Package and Deploy the Lambda

```bash
cd jobNotification
zip function.zip glue_job_notification.py
```

Create the function:

```bash
ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}

aws lambda create-function \
  --function-name $FUNCTION_NAME \
  --runtime python3.12 \
  --handler glue_job_notification.lambda_handler \
  --role $ROLE_ARN \
  --zip-file fileb://function.zip \
  --environment "Variables={SNS_TOPIC_ARN=$SNS_TOPIC_ARN}" \
  --timeout 30
```

To update the function code later:

```bash
zip function.zip glue_job_notification.py
aws lambda update-function-code \
  --function-name $FUNCTION_NAME \
  --zip-file fileb://function.zip
```

## 5. Create EventBridge Rule

```bash
aws events put-rule \
  --name glue-job-state-change \
  --event-pattern '{
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"]
  }' \
  --description "Captures all Glue Job state changes"
```

Grant EventBridge permission to invoke the Lambda:

```bash
LAMBDA_ARN=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}

aws lambda add-permission \
  --function-name $FUNCTION_NAME \
  --statement-id eventbridge-invoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/glue-job-state-change
```

Add the Lambda as a target:

```bash
aws events put-targets \
  --rule glue-job-state-change \
  --targets "Id=glue-notification-lambda,Arn=$LAMBDA_ARN"
```

## 6. Test

Invoke the Lambda with a sample event:

```bash
cat > /tmp/test-event.json << 'EOF'
{
  "version": "0",
  "id": "abcdef00-1234-5678-9abc-def012345678",
  "detail-type": "Glue Job State Change",
  "source": "aws.glue",
  "account": "123456789012",
  "time": "2024-01-15T10:30:00Z",
  "region": "us-east-1",
  "resources": [],
  "detail": {
    "jobName": "my-etl-job",
    "severity": "INFO",
    "state": "SUCCEEDED",
    "jobRunId": "jr_abc123",
    "message": "Job run succeeded"
  }
}
EOF

aws lambda invoke \
  --function-name $FUNCTION_NAME \
  --payload fileb:///tmp/test-event.json \
  /tmp/response.json

cat /tmp/response.json
```

## Cleanup

```bash
aws events remove-targets --rule glue-job-state-change --ids glue-notification-lambda
aws events delete-rule --name glue-job-state-change
aws lambda delete-function --function-name $FUNCTION_NAME
aws iam delete-role-policy --role-name $ROLE_NAME --policy-name sns-publish
aws iam delete-role-policy --role-name $ROLE_NAME --policy-name glue-read
aws iam detach-role-policy --role-name $ROLE_NAME --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name $ROLE_NAME
aws sns delete-topic --topic-arn $SNS_TOPIC_ARN
rm function.zip
```
