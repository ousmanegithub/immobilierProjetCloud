import json
import boto3
import os
from datetime import datetime
from scrapy.utils.project import get_project_settings

class S3UploadPipeline:

    def open_spider(self, spider):
        # Créer le dossier data si absent
        os.makedirs("data", exist_ok=True)

        # Fichier NDJSON
        self.local_file_path = "data/dernieres_annonces_senegal.ndjson"
        self.file = open(self.local_file_path, "w", encoding="utf-8")

        # S3
        self.bucket_name = "m2dsia-faye-ousmane"
        self.s3_object_name = "input/dernieres_annonces_senegal.ndjson"
        self.s3 = boto3.client("s3")

    def process_item(self, item, spider):
        record = {
            "url_annonce": item.get("url_annonce"),
            "type_de_bien": item.get("type_de_bien"),
            "localisation": item.get("localisation"),
            "prix": item.get("prix"),
            "chambres": item.get("chambres"),
            "salles_de_bains": item.get("salles_de_bains"),
            "date_scraping": datetime.utcnow().isoformat() + "Z"
        }

        # 1 ligne = 1 objet JSON
        self.file.write(json.dumps(record, ensure_ascii=False) + "\n")
        return item

    def close_spider(self, spider):
        self.file.close()
        spider.logger.info("Fichier NDJSON généré")

        try:
            self.s3.upload_file(
                self.local_file_path,
                self.bucket_name,
                self.s3_object_name
            )
            spider.logger.info(
                f"Upload S3 terminé : s3://{self.bucket_name}/{self.s3_object_name}"
            )
        except Exception as e:
            spider.logger.error(f"Erreur upload S3 : {e}")
