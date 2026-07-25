data "aws_vpc" "canary" {
  filter {
    name   = "tag:Name"
    values = [var.eks_cluster_name]
  }
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.canary.id]
  }

  filter {
    name   = "tag:kubernetes.io/role/internal-elb"
    values = ["1"]
  }
}

resource "random_password" "postgres" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "benchmark" {
  name       = var.db_identifier
  subnet_ids = data.aws_subnets.private.ids

  tags = {
    Name      = var.db_identifier
    Project   = "workflow-world-ursula"
    ManagedBy = "opentofu"
  }
}

resource "aws_security_group" "postgres" {
  name        = var.db_identifier
  description = "Postgres access from the isolated Ursula canary VPC"
  vpc_id      = data.aws_vpc.canary.id

  ingress {
    description = "Postgres from canary VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.canary.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name      = var.db_identifier
    Project   = "workflow-world-ursula"
    ManagedBy = "opentofu"
  }
}

resource "aws_db_instance" "benchmark" {
  identifier = var.db_identifier

  engine         = "postgres"
  engine_version = "17"
  instance_class = var.db_instance_class
  db_name        = "workflow"
  username       = "workflow"
  password       = random_password.postgres.result
  port           = 5432

  multi_az               = true
  publicly_accessible    = false
  db_subnet_group_name   = aws_db_subnet_group.benchmark.name
  vpc_security_group_ids = [aws_security_group.postgres.id]

  allocated_storage     = var.db_allocated_storage_gib
  max_allocated_storage = 500
  storage_type          = "gp3"
  iops                  = 3000
  storage_throughput    = 125
  storage_encrypted     = true

  backup_retention_period      = 7
  performance_insights_enabled = true
  apply_immediately            = true
  auto_minor_version_upgrade   = true
  copy_tags_to_snapshot        = true

  deletion_protection = false
  skip_final_snapshot = true

  tags = {
    Name        = var.db_identifier
    Environment = "benchmark"
    Project     = "workflow-world-ursula"
    ManagedBy   = "opentofu"
  }
}

resource "kubernetes_namespace_v1" "benchmark" {
  metadata {
    name = var.kubernetes_namespace
  }
}

resource "kubernetes_secret_v1" "postgres" {
  metadata {
    name      = "workflow-postgres-benchmark"
    namespace = kubernetes_namespace_v1.benchmark.metadata[0].name
  }

  data = {
    url = "postgresql://${aws_db_instance.benchmark.username}:${random_password.postgres.result}@${aws_db_instance.benchmark.address}:${aws_db_instance.benchmark.port}/${aws_db_instance.benchmark.db_name}"
  }

  type = "Opaque"
}
