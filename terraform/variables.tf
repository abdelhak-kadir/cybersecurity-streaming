variable "region" {
  description = "Digital Ocean region"
  default     = "fra1" # Frankfurt — low latency for EU
}

variable "domain" {
  description = "Root domain managed by DigitalOcean DNS (nameservers must point to DO)"
  default     = "project-demo.tech"
}

variable "grafana_subdomain" {
  description = "Subdomain for Grafana (must be under var.domain)"
  default     = "grafana"
}

variable "api_subdomain" {
  description = "Subdomain for FastAPI (must be under var.domain)"
  default     = "api"
}

variable "expose_api" {
  description = "Create a DNS record for the API subdomain (set to true for demo)"
  type        = bool
  default     = false
}

variable "ssh_allowed_ips" {
  description = "CIDR blocks allowed to SSH into droplets (restrict to your IP/VPN)"
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"] # Override in terraform.tfvars with your IP
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key to upload to Digital Ocean"
  default     = "~/.ssh/do_cyber.pub"
}

variable "ssh_private_key_path" {
  description = "Path to the SSH private key (used by Ansible)"
  default     = "~/.ssh/do_cyber"
}
