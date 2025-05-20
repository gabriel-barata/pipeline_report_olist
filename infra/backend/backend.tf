terraform {
  backend "s3" {
    bucket         = "terraform-backend-269012942764"
    key            = "autoglass-case/infra.tfstate"
    region         = "us-east-1"
    encrypt        = true
  }
}
