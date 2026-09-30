#!/usr/bin/env bash
# ==============================================================================
# Teardown AWS Infrastructure for Phase 5 (Empties S3 bucket & deletes stack)
# ==============================================================================
set -euo pipefail

STACK_NAME="${STACK_NAME:-stock-pipeline-warehouse-stack}"
AWS_REGION="${AWS_REGION:-us-east-1}"

echo "================================================================================"
echo "Tearing Down AWS Stack: ${STACK_NAME} (Region: ${AWS_REGION})"
echo "================================================================================"

# 1. Retrieve bucket name to empty prior to stack deletion
BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" \
    --output text \
    --region "${AWS_REGION}" 2>/dev/null || echo "")

if [[ -n "${BUCKET_NAME}" && "${BUCKET_NAME}" != "None" ]]; then
    echo "Emptying S3 Bucket: ${BUCKET_NAME}..."
    aws s3 rm "s3://${BUCKET_NAME}" --recursive --region "${AWS_REGION}" || true
fi

# 2. Delete CloudFormation stack
echo "Deleting CloudFormation Stack '${STACK_NAME}'..."
aws cloudformation delete-stack \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}"

echo "Waiting for stack deletion to complete..."
aws cloudformation wait stack-delete-complete \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}"

echo "================================================================================"
echo "Stack ${STACK_NAME} successfully deleted. All billable resources removed."
echo "================================================================================"
