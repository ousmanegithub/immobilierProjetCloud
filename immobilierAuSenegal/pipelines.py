# immobilierAuSenegal/pipelines.py

import json
import boto3
import os
from scrapy.utils.project import get_project_settings

class S3UploadPipeline:
    def open_spider(self, spider):
        """Initialise le fichier JSON local au début du spider."""
        # Assurez-vous que le dossier 'data' existe
        if not os.path.exists('data'):
            os.makedirs('data')
            
        self.file = open('data/resultats.json', 'w', encoding='utf-8')
        self.file.write('[')
        self.first_item = True
        
        # Configuration S3
        settings = get_project_settings()
        self.bucket_name = "m2dsia-soumare-ibrahima" # Remplacez par votre nom de bucket exact
        self.s3_object_name = "dernieres_annonces_senegal.json"
        self.local_file_path = 'data/resultats.json'
        self.s3 = boto3.client("s3")

    def close_spider(self, spider):
        """Ferme le fichier JSON local et lance l'upload S3 à la fin du spider."""
        self.file.write(']')
        self.file.close()
        
        spider.logger.info(f"Fichier local créé : {self.local_file_path}")
        self.upload_to_s3(spider)
        spider.logger.info(f"Upload vers S3://{self.bucket_name}/{self.s3_object_name} terminé.")

    def process_item(self, item, spider):
        """Écrit chaque item dans le fichier JSON."""
        if not self.first_item:
            self.file.write(',')
        else:
            self.first_item = False
            
        # Utilisation de json.dumps pour formater correctement l'item en JSON
        line = json.dumps(dict(item), ensure_ascii=False, indent=4)
        self.file.write(line)
        return item

    def upload_to_s3(self, spider):
        """Fonction utilitaire pour l'upload S3."""
        try:
            self.s3.upload_file(self.local_file_path, self.bucket_name, self.s3_object_name)
        except Exception as e:
            spider.logger.error(f"Erreur lors de l'upload S3 : {e}")

