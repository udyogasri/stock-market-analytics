#!/usr/bin/env bash
# ==============================================================================
# Deploy AWS Infrastructure for Phase 5 (S3, Redshift Serverless, Lambda, DLQ)
# ==============================================================================
set -euo pipefail

STACK_NAME="${STACK_NAME:-stock-pipeline-warehouse-stack}"
AWS_REGION="${AWS_REGION:-us-east-1}"
STAGE="${STAGE:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "================================================================================"
echo "Deploying AWS CloudFormation Stack: ${STACK_NAME} (Region: ${AWS_REGION})"
echo "================================================================================"

# 1. Validate template
echo "Validating CloudFormation template..."
aws cloudformation validate-template \
    --template-body "file://${REPO_ROOT}/infra/template.yaml" \
    --region "${AWS_REGION}" > /dev/null

# 2. Deploy CloudFormation Stack
echo "Deploying stack '${STACK_NAME}'..."
aws cloudformation deploy \
    --template-file "${REPO_ROOT}/infra/template.yaml" \
    --stack-name "${STACK_NAME}" \
    --parameter-overrides EnvironmentName="${STAGE}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}"

# 3. Retrieve Stack Outputs
echo "Retrieving stack outputs..."
FUNCTION_NAME=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='LambdaFunctionArn'].OutputValue" \
    --output text \
    --region "${AWS_REGION}")

BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" \
    --output text \
    --region "${AWS_REGION}")

WORKGROUP_NAME=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='RedshiftWorkgroupName'].OutputValue" \
    --output text \
    --region "${AWS_REGION}")

echo "Lambda Function : ${FUNCTION_NAME}"
echo "S3 Bucket       : ${BUCKET_NAME}"
echo "Redshift Group  : ${WORKGROUP_NAME}"

# 4. Package and update Lambda function code (pure Python, NO local duckdb)
echo "Packaging Lambda code from lambda/load_to_redshift/..."
TEMP_ZIP=$(mktemp /tmp/lambda-package-XXXXXX.zip)
(
    cd "${REPO_ROOT}/lambda/load_to_redshift"
    zip -q -r "${TEMP_ZIP}" handler.py
)

echo "Updating Lambda function code..."
aws lambda update-function-code \
    --function-name "${FUNCTION_NAME}" \
    --zip-file "fileb://${TEMP_ZIP}" \
    --region "${AWS_REGION}" > /dev/null

rm -f "${TEMP_ZIP}"

# 5. Initialize Redshift Tables via Redshift Data API
echo "Initializing Redshift warehouse tables from warehouse/schema.sql..."
SCHEMA_SQL=$(<"${REPO_ROOT}/warehouse/schema.sql")

STMT_ID=$(aws redshift-data execute-statement \
    --workgroup-name "${WORKGROUP_NAME}" \
    --database "dev" \
    --sql "${SCHEMA_SQL}" \
    --region "${AWS_REGION}" \
    --query "Id" \
    --output text)

echo "Executed DDL statement ID: ${STMT_ID}. Polling for completion..."
aws redshift-data wait-for-statement \
    --id "${STMT_ID}" \
    --region "${AWS_REGION}"

echo "================================================================================"
echo "Deployment Complete!"
echo "S3 Datalake Bucket : ${BUCKET_NAME}"
echo "Redshift Workgroup : ${WORKGROUP_NAME}"
echo "Lambda Trigger     : ${BUCKET_NAME}/export/**/manifest.json"
echo "================================================================================"
