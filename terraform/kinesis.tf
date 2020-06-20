provider "aws" {
  profile = "default"
  region  = "us-east-1"
}

resource "aws_s3_bucket" "bucket" {
  bucket = "cdc-event-bucket"
  acl    = "private"
}

resource "aws_iam_role" "firehose_role" {
  name = "firehose_test_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "firehose.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_kinesis_firehose_delivery_stream" "extended_s3_stream" {
  name        = "terraform-kinesis-firehose-extended-s3-test-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.bucket.arn

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.kinesis_firehose.name
      log_stream_name = aws_cloudwatch_log_stream.cdc_events.name
    }
  }
}


resource "aws_cloudwatch_log_group" "kinesis_firehose" {
  name = "kinesis-firehose"

  tags = {
    Environment = "production"
    Application = "alluvium"
  }
}


resource "aws_cloudwatch_log_stream" "cdc_events" {
  name           = "cdc-events"
  log_group_name = aws_cloudwatch_log_group.kinesis_firehose.name
}
