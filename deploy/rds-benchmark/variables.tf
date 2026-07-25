variable "aws_region" {
  type        = string
  description = "AWS region containing the EKS canary cluster."
  default     = "us-east-1"
}

variable "eks_cluster_name" {
  type        = string
  description = "Existing EKS cluster used by both benchmark applications."
  default     = "ursula-canary-eks"
}

variable "db_identifier" {
  type        = string
  description = "RDS instance identifier."
  default     = "workflow-benchmark-postgres"
}

variable "db_instance_class" {
  type        = string
  description = "Production-shaped RDS instance class."
  default     = "db.m7g.large"
}

variable "db_allocated_storage_gib" {
  type        = number
  description = "Initial encrypted gp3 storage allocation."
  default     = 100
}

variable "kubernetes_namespace" {
  type        = string
  description = "Namespace receiving the benchmark database URL Secret."
  default     = "workflow-benchmark-postgres"
}
