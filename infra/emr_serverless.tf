# resource "aws_emrserverless_application" "spark_app" {
#   name          = "${var.project_name}-emr-serverless"
#   release_label = "emr-7.3.0"

#   type = "spark"

#   maximum_capacity {
#     cpu    = "2 vCPU"
#     memory = "16 GB"
#     disk   = "100 GB"
#   }

#   network_configuration {
#     subnet_ids         = [aws_subnet.public.id]
#     security_group_ids = [aws_security_group.emr_sg.id]
#   }

#   auto_start_configuration {
#     enabled = true
#   }

#   auto_stop_configuration {
#     enabled           = true
#     idle_timeout_minutes = 10
#   }

#   tags = {
#     Environment = var.aws_env
#   }
# }
