resource "aws_iam_role" "emr_serverless_execution_role" {
  name = "${var.project_name}-emr-serverless-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "emr-serverless.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "emr_serverless_s3_policy" {
  name = "${var.project_name}-emr-serverless-s3-policy"
  role = aws_iam_role.emr_serverless_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Resource = flatten([
          for bucket in aws_s3_bucket.data_lake_buckets :
          [
            "arn:aws:s3:::${bucket.bucket}",
            "arn:aws:s3:::${bucket.bucket}/*"
          ]
        ])
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      }
    ]
  })
}
