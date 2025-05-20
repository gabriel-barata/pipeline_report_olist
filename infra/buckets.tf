resource "aws_s3_bucket" "data_lake_buckets" {
  for_each = var.lake_bucket_layer

  bucket        = "${var.project_name}-datalake-${var.aws_env}-${var.aws_region}-${var.aws_account_id}-${each.key}"
  force_destroy = true

  tags = {
    Environment = var.aws_env
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake_retention" {
  for_each = {
    for key, value in var.lake_bucket_layer : key => value
    if value.retention
  }

  bucket = aws_s3_bucket.data_lake_buckets[each.key].id

  rule {
    id     = "expire-after-7-days"
    status = "Enabled"

    expiration {
      days = 7
    }
  }
}

resource "aws_s3_bucket" "source_code_bucket" {
  bucket = "${var.project_name}-datalake-${var.aws_env}-${var.aws_region}-${var.aws_account_id}-source-code"

  tags = {
    Environment = var.aws_env
    Type        = "source"
  }
}

resource "aws_s3_bucket_versioning" "source_code" {
  bucket = aws_s3_bucket.source_code_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}
