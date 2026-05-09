# Public IPs (SSH access, Grafana/FastAPI public endpoints)
output "infra_ip" {
  description = "Public IP of the infra droplet (Kafka, Cassandra)"
  value       = digitalocean_droplet.infra.ipv4_address
}

output "compute_ip" {
  description = "Public IP of the compute droplet (Spark, HBase, HDFS)"
  value       = digitalocean_droplet.compute.ipv4_address
}

output "serve_ip" {
  description = "Public IP of the serve droplet (FastAPI :8000, Grafana :3000)"
  value       = digitalocean_droplet.serve.ipv4_address
}

# VPC private IPs — injected into docker-compose files by Ansible
output "infra_private_ip" {
  description = "VPC private IP of infra (used by compute and serve to reach Kafka/Cassandra)"
  value       = digitalocean_droplet.infra.ipv4_address_private
}

output "compute_private_ip" {
  description = "VPC private IP of compute (used by serve to reach HBase Thrift)"
  value       = digitalocean_droplet.compute.ipv4_address_private
}

output "serve_private_ip" {
  description = "VPC private IP of serve"
  value       = digitalocean_droplet.serve.ipv4_address_private
}

output "grafana_url" {
  description = "Public Grafana dashboard URL"
  value       = "https://${var.grafana_subdomain}.${var.domain}"
}

output "api_url" {
  description = "Public FastAPI URL (only created when expose_api = true)"
  value       = var.expose_api ? "https://${var.api_subdomain}.${var.domain}" : "internal only"
}
