variable "project_name" {
  type    = string
  default = "olist"
}

variable "aws_account_id" {
  type    = string
  default = "269012942764"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_env" {
  type    = string
  default = "prd"
}

variable "lake_bucket_layer" {
  type = map(object({
    retention = bool
  }))
  description = "Data lake bucket names/layers"
  default = {
    "landing" = {
      retention = true,
    },
    raw = {
      retention = false,
    }
    curated = {
      retention = false
    }
  }
}
