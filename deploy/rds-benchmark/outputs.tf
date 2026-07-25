output "db_identifier" {
  value = aws_db_instance.benchmark.identifier
}

output "db_instance_class" {
  value = aws_db_instance.benchmark.instance_class
}

output "db_multi_az" {
  value = aws_db_instance.benchmark.multi_az
}

output "db_allocated_storage_gib" {
  value = aws_db_instance.benchmark.allocated_storage
}

output "postgres_url" {
  value     = kubernetes_secret_v1.postgres.data.url
  sensitive = true
}
