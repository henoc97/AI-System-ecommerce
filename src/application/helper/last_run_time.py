import datetime
import os

from application.helper.boto_s3_utils import BotoS3Utils




class LastRunTime:
    def __init__(self, spark):
        self.boto_s3_utils = BotoS3Utils()
        self.bucket_base_url = os.getenv("BUCKET_BASE_URL_BOTO")
        self.spark = spark
        
        
    def resolve_metadata_file_path(self, root, root_child_name):
        return f"{root}/{root_child_name}/metadata/last_run_time.json"

    def get(self, root, root_child_name):
        try:
            s3_key = self.resolve_metadata_file_path(root, root_child_name)
            result = self.boto_s3_utils.download_json(s3_key=s3_key)
            return result.get("last_run_time")
        except Exception as e:
            print(f"Erreur lors du chargement de last_run_time : {e}")
            return "1970-01-01 00:00:00"

    def update(self, root, root_child_name):
        try:
            metadata_file_path = f"{root}/{root_child_name}/metadata/last_run_time.json"
            
            new_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_metadata = [{"last_run_time": new_time}]
            metadata_df = self.spark.createDataFrame(new_metadata)
            
            # Convertir le DataFrame en format JSON en mémoire
            json_data = metadata_df.toJSON().collect()  # Crée une liste de JSON en chaînes de caractères
            json_content = "\n".join(json_data)  # Convertir en format JSON

            # metadata_df.write.format('json').save(metadata_file_path, mode="overwrite")
            self.boto_s3_utils.upload(json_content.encode(), metadata_file_path)
            print("metadata file created successfully")
        except Exception as e:
            print(f"Error updating last_run_time : {e}")
            
    def get_update_metadata(element):
        def intern_decorator(function):
            def wrapper(self, *args, **kwargs):
                root, root_child_name = element
                last_run_time = self.get(root, root_child_name)
                result = function(self, *args, last_run_time=last_run_time, **kwargs)
                self.update(root, root_child_name)
                return result
            return wrapper
        return intern_decorator
    
