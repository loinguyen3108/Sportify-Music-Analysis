from google.cloud import storage


class GCSHelper:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.get_bucket(bucket_name)

    def get_bucket(self, bucket_name: str):
        return self.client.bucket(bucket_name)

    def is_exists(self, blob_name: str):
        blobs = self.client.list_blobs(
            bucket_or_name=self.bucket.name, prefix=blob_name,
            max_results=10
        )
        return len(list(blobs)) > 1
