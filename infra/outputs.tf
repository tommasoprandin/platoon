output "grafana_url" {
  description = "URL to access Grafana"
  value       = "http://${google_compute_instance.monitoring.network_interface[0].access_config[0].nat_ip}:3000"
}

output "platoon_nodes_internal_ips" {
  description = "Internal IPs of the platoon nodes"
  value = {
    for i, node in google_compute_instance.platoon_node :
    "node${i + 1}" => node.network_interface[0].network_ip
  }
}

output "platoon_nodes_external_ips" {
  description = "External IPs of the platoon nodes (for SSH access)"
  value = {
    for i, node in google_compute_instance.platoon_node :
    "node${i + 1}" => node.network_interface[0].access_config[0].nat_ip
  }
}

output "monitoring_internal_ip" {
  description = "Internal IP of the monitoring instance"
  value       = google_compute_instance.monitoring.network_interface[0].network_ip
}
