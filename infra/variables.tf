variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources"
  type        = string
  default     = "europe-west8"
}

variable "zone" {
  description = "The GCP zone to deploy resources"
  type        = string
  default     = "europe-west8-a"
}

variable "node_count" {
  description = "Number of nodes in the platoon cluster"
  type        = number
  default     = 3

  validation {
    condition     = var.node_count >= 1
    error_message = "Node count must be at least 1."
  }
}

variable "image_tag" {
  description = "The Docker image tag to use"
  type        = string
  default     = "latest"
}
