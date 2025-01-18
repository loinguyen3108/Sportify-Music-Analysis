terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = "loideproject"
  region  = "us-central1"
  credentials = "loideproject-2e2ee44a24c3.json"
}