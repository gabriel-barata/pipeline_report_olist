output "lake_bucket_names" {
  value = { for k, b in aws_s3_bucket.data_lake_buckets : k => b.bucket }
}
