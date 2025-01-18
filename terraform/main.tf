resource "google_storage_bucket" "spotify-data-lake" {
  name          = "spotify-data-lake"
  location      = "US-CENTRAL1"
  force_destroy = true
}

resource "google_storage_bucket" "spotify-temporary-dwh" {
  name          = "spotify-temporary-dwh"
  location      = "US-CENTRAL1"
  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "spotify_dwh"
  friendly_name               = "spotify"
  description                 = "This is a Spotify Data Warehouse"
  location                    = "US-CENTRAL1"
  default_table_expiration_ms = 604800000
}
