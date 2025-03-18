provider "google" {
  project = var.project_id
  zone = var.zone
}

resource "google_compute_network" "platoon_network" {
  name                    = "platoon-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "platoon_subnet" {
  name          = "platoon-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.platoon_network.id
}

# Firewall rule for internal communication
resource "google_compute_firewall" "platoon_internal" {
  name    = "platoon-internal"
  network = google_compute_network.platoon_network.id

  allow {
    protocol = "tcp"
    ports    = ["3100", "5001", "8001"]
  }

  allow {
    protocol = "icmp"
  }

  source_ranges = ["10.0.0.0/24"]
}

# Allow external access to Grafana
resource "google_compute_firewall" "grafana_access" {
  name    = "grafana-access"
  network = google_compute_network.platoon_network.id

  allow {
    protocol = "tcp"
    ports    = ["3000"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["monitoring"]
}

#Allow SSH for debug
resource "google_compute_firewall" "ssh_access" {
  name    = "ssh-access"
  network = google_compute_network.platoon_network.id

  allow {
    protocol = "tcp"
    ports = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
}

# Service account for instances
resource "google_service_account" "platoon_service_account" {
  account_id   = "platoon-service-account"
  display_name = "Platoon Service Account"
}

# Grant Artifact Registry Reader role to the service account
resource "google_project_iam_member" "artifact_registry_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.platoon_service_account.email}"
}

# Platoon node instances
resource "google_compute_instance" "platoon_node" {
  count        = var.node_count
  name         = "platoon-node-${count.index + 1}"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
      size  = 20
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.platoon_subnet.id
    access_config {
      // Ephemeral IP
    }
  }

  metadata = {
    gce-container-declaration = templatefile("${path.module}/templates/container.tpl", {
      container_image = "gcr.io/${var.project_id}/platoon:latest"
      node_id         = count.index + 1
      cluster_size    = var.node_count
      raft_port       = 5001
      platoon_port    = 8001
      rnd_seed        = 123456 + count.index * 1000
      read_period     = 500
      write_period    = 1000
    })
  }

  service_account {
    email  = google_service_account.platoon_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["node"]

  depends_on = [
    google_compute_instance.monitoring,
    google_project_iam_member.artifact_registry_reader
  ]
}

# Monitoring instance (Loki + Grafana)
resource "google_compute_instance" "monitoring" {
  name         = "platoon-monitoring"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size  = 30
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.platoon_subnet.id
    access_config {
      // Ephemeral IP
    }
  }

  metadata_startup_script = file("${path.module}/scripts/monitoring.sh")

  service_account {
    email  = google_service_account.platoon_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["monitoring"]
}
