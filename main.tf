provider  "aws"{
    region = "us-east-2"
    access_key = "AKIA6KZQ3UPJ6GQFNQLV"
    secret_key = "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"
}
resource "aws_iam_role_policy" "Aws-step-policy_02" {
  name = "Aws-step-policy_02"
  role = aws_iam_role.step_function_role_02.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "*"
    }
  ]
}
EOF
}
resource "aws_iam_role" "step_function_role_02" {
  name = "step-function-role_02"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
      "Service": "states.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": "StepFunctionAssumeRole"
      }
    ]
  }
  EOF
}
resource "aws_sfn_state_machine" "sfn_state_machine_23" {
  name     = "adi-state-machine-30"
  role_arn = aws_iam_role.step_function_role_02.arn

  definition = <<EOF
{
  "Comment": "transaction state machine",
  "StartAt": "ProcessTransaction",
  "States": {
    "ProcessTransaction": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.TransactionType",
          "StringEquals": "PURCHASE",
          "Next": "ProcessPurchase"
        },
        {
          "Variable": "$.TransactionType",
          "StringEquals": "REFUND",
          "Next": "ProcessRefund"
        }
      ]
    },
    "ProcessRefund": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-2:985260598227:function:refund",
      "End": true
    },
    "ProcessPurchase": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-2:985260598227:function:purchase",
      "End": true
    }
  }
}
EOF
}
