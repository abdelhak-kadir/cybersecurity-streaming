#!/usr/bin/env bash
# Reads terraform outputs and writes ansible/inventory.ini.
# Run after: make tf-apply
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../terraform"

echo "Reading terraform outputs..."
INFRA_IP=$(terraform output -raw infra_ip)
COMPUTE_IP=$(terraform output -raw compute_ip)
SERVE_IP=$(terraform output -raw serve_ip)
INFRA_PRIV=$(terraform output -raw infra_private_ip)
COMPUTE_PRIV=$(terraform output -raw compute_private_ip)

cat > "$SCRIPT_DIR/inventory.ini" <<EOF
[infra]
cyber-infra ansible_host=${INFRA_IP}

[compute]
cyber-compute ansible_host=${COMPUTE_IP}

[serve]
cyber-serve ansible_host=${SERVE_IP}

[all:vars]
ansible_user=root
ansible_ssh_private_key_file=${ANSIBLE_SSH_KEY:-~/.ssh/do_cyber}
ansible_ssh_common_args='-o StrictHostKeyChecking=no'
project_dir=/opt/cyber

infra_private_ip=${INFRA_PRIV}
compute_private_ip=${COMPUTE_PRIV}
EOF

echo "inventory.ini written:"
echo "  infra   public=${INFRA_IP}   private=${INFRA_PRIV}"
echo "  compute public=${COMPUTE_IP}  private=${COMPUTE_PRIV}"
echo "  serve   public=${SERVE_IP}"
