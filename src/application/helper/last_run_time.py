import os



class LastRunTime:
    def __init__(self, spark):
        self.bucket_name = os.getenv("BUCKET_NAME")
        self.spark = spark
        
        
    def resolve_metadata_file_path(self, root, root_child_name):
        return f"{self.bucket_name}/{root}/{root_child_name}/metadata/last_run_time.json"

    def get(self, root, root_child_name):
        try:
            metadata_file_path = self.resolve_metadata_file_path(root, root_child_name)
            metadata_df = self.spark.read.json(metadata_file_path)
            last_run_time = metadata_df.collect()[0]["last_run_time"]
            return last_run_time
        except Exception as e:
            print(f"Error loading last_run_time : {e}")
            return "1970-01-01T00:00:00"

    def update(self, new_time, root, root_child_name):
        try:
            metadata_file_path = self.resolve_metadata_file_path(root, root_child_name)
            new_metadata = [{"last_run_time": new_time}]
            metadata_df = self.spark.createDataFrame(new_metadata)
            metadata_df.write.mode("overwrite").json(metadata_file_path)
        except Exception as e:
            print(f"Error updating last_run_time : {e}")
