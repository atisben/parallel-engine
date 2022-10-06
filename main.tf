terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 3.53"
    }
  }
}

variable "project" {
  type        = string
  description = "Google Cloud Platform Project ID"
}


variable "region" {
  default = "europe-west1"
  type    = string
}


provider "google" {
  project = var.project
}

locals {

  vertex_ai_parallelizer_sa  = "serviceAccount:${google_service_account.vertex_ai_parallelizer.email}"
}

# Enable services

resource "google_project_service" "iam" {
  service = "iam.googleapis.com"
  disable_on_destroy = false
}

# Create a service account
resource "google_service_account" "vertex_ai_parallelizer" {
  account_id   = "vertex-ai-parallelizer"
  display_name = "vertexAI SA"
  description  = "Identity used by a vertexAI service tu run custom jobs"
}


# Set permissions
resource "google_project_iam_binding" "service_permissions" {
  for_each = toset([
    "bigquery.Admin", "storage.objectAdmin", "aiplatform.user"
  ])

  role       = "roles/${each.key}"
  members    = [local.vertex_ai_parallelizer_sa]
  depends_on = [google_service_account.vertex_ai_parallelizer]
}