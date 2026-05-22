terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

# Reads DIGITALOCEAN_TOKEN from environment — never hardcode the token
provider "digitalocean" {}

# ── SSH key (uploaded once, referenced by all droplets) ───────────────────────
resource "digitalocean_ssh_key" "cyber" {
  name       = "cyber-cluster"
  public_key = file(pathexpand(var.ssh_public_key_path))
}

# ── VPC (private network shared by all 3 droplets) ───────────────────────────
resource "digitalocean_vpc" "cyber" {
  name   = "cyber-vpc"
  region = var.region
}

# ── Droplets ──────────────────────────────────────────────────────────────────

resource "digitalocean_droplet" "infra" {
  name     = "cyber-infra"
  region   = var.region
  size     = "s-2vcpu-4gb" # ~$24/mo — Kafka + Zookeeper + Cassandra
  image    = "ubuntu-22-04-x64"
  vpc_uuid = digitalocean_vpc.cyber.id
  ssh_keys = [digitalocean_ssh_key.cyber.fingerprint]
  tags     = ["cyber", "infra"]
}

resource "digitalocean_droplet" "compute" {
  name     = "cyber-compute"
  region   = var.region
  size     = "s-4vcpu-8gb" # ~$48/mo — Spark + HBase + HDFS
  image    = "ubuntu-22-04-x64"
  vpc_uuid = digitalocean_vpc.cyber.id
  ssh_keys = [digitalocean_ssh_key.cyber.fingerprint]
  tags     = ["cyber", "compute"]
}

resource "digitalocean_droplet" "serve" {
  name     = "cyber-serve"
  region   = var.region
  size     = "s-2vcpu-2gb" # ~$12/mo — FastAPI + Grafana
  image    = "ubuntu-22-04-x64"
  vpc_uuid = digitalocean_vpc.cyber.id
  ssh_keys = [digitalocean_ssh_key.cyber.fingerprint]
  tags     = ["cyber", "serve"]
}

# ── Firewalls ─────────────────────────────────────────────────────────────────

# Infra: Kafka (9092) from compute only, Cassandra (9042) from compute + serve
resource "digitalocean_firewall" "infra" {
  name        = "cyber-infra-fw"
  droplet_ids = [digitalocean_droplet.infra.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.ssh_allowed_ips
  }
  inbound_rule {
    protocol           = "tcp"
    port_range         = "9092"
    source_droplet_ids = [digitalocean_droplet.compute.id]
  }
  inbound_rule {
    protocol           = "tcp"
    port_range         = "9042"
    source_droplet_ids = [digitalocean_droplet.compute.id, digitalocean_droplet.serve.id]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

# Compute: HBase Thrift (9090) from serve only
# Spark UI (8080), HDFS (9000/9870), ZK (2181) stay closed externally
resource "digitalocean_firewall" "compute" {
  name        = "cyber-compute-fw"
  droplet_ids = [digitalocean_droplet.compute.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.ssh_allowed_ips
  }
  inbound_rule {
    protocol           = "tcp"
    port_range         = "9090"
    source_droplet_ids = [digitalocean_droplet.serve.id]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

# Serve: only Caddy is public-facing (ports 80 + 443).
# Grafana (:3000) and FastAPI (:8000) are internal — Caddy proxies to them.
resource "digitalocean_firewall" "serve" {
  name        = "cyber-serve-fw"
  droplet_ids = [digitalocean_droplet.serve.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.ssh_allowed_ips
  }
  # HTTP — required for Let's Encrypt ACME HTTP-01 challenge
  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }
  # HTTPS — all public traffic enters here
  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }
  # FastAPI direct access
  inbound_rule {
    protocol         = "tcp"
    port_range       = "8000"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

# ── DNS — DigitalOcean manages project-demo.tech ──────────────────────────────
# Prerequisite: nameservers at registrar must point to
#   ns1.digitalocean.com / ns2.digitalocean.com / ns3.digitalocean.com

resource "digitalocean_domain" "cyber" {
  name = var.domain
}

# grafana.project-demo.tech → serve droplet public IP
resource "digitalocean_record" "grafana" {
  domain = digitalocean_domain.cyber.id
  type   = "A"
  name   = var.grafana_subdomain
  value  = digitalocean_droplet.serve.ipv4_address
  ttl    = 300
}

# api.project-demo.tech → serve droplet public IP (only when expose_api = true)
resource "digitalocean_record" "api" {
  count  = var.expose_api ? 1 : 0
  domain = digitalocean_domain.cyber.id
  type   = "A"
  name   = var.api_subdomain
  value  = digitalocean_droplet.serve.ipv4_address
  ttl    = 300
}
