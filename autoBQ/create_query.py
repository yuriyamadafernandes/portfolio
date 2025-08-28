# create_query.py

from google.cloud import bigquery_datatransfer
from google.oauth2 import service_account
import os
from config import CLIENTS_CONFIG
import datetime

def create_bigquery_client(credentials_path):
    """
    Cria um cliente do BigQuery Data Transfer Service usando as credenciais fornecidas.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
        )
        print("Credenciais carregadas com sucesso!")
        return bigquery_datatransfer.DataTransferServiceClient(credentials=credentials)
    except Exception as e:
        print(f"Erro ao carregar credenciais: {e}")
        raise

def create_scheduled_query(client, project_id, dataset_id, query_config, location="US"):
    """
    Cria uma consulta programada no BigQuery usando o Data Transfer Service.
    """
    try:
        # Configurar o parent (projeto + localização)
        parent = f"projects/{project_id}/locations/{location}"

        # Converter start_time e end_time para timestamps
        start_time = datetime.datetime.strptime(query_config["start_time"], "%Y-%m-%dT%H:%M:%SZ")
        end_time = datetime.datetime.strptime(query_config["end_time"], "%Y-%m-%dT%H:%M:%SZ")

        # Configuração da consulta programada
        transfer_config = {
            "display_name": query_config["query_name"],
            "data_source_id": "scheduled_query",  # Identificador para consultas programadas
            "destination_dataset_id": dataset_id,  # Dataset de destino
            "params": {
                "query": query_config["query"],
                "destination_table_name_template": query_config["destination_table_id"],
                "write_disposition": "WRITE_TRUNCATE",  # Sempre substituir a tabela
                "partitioning_field": "",  # Não usaremos particionamento neste exemplo
            },
            "schedule": query_config["schedule_interval"],  # Exemplo: "every 1 hours"
            "schedule_options": {
                "start_time": {"seconds": int(start_time.timestamp())},
                "end_time": {"seconds": int(end_time.timestamp())},
            },
        }

        # Criar a consulta programada
        response = client.create_transfer_config(
            parent=parent,
            transfer_config=transfer_config
        )

        print(f"Consulta programada '{query_config['query_name']}' criada com sucesso! ID: {response.name}")
    except Exception as e:
        print(f"Erro ao criar a consulta programada: {e}")
        raise

if __name__ == "__main__":
    # Definir as configurações do cliente
    selected_client = "cliente1"
    config = CLIENTS_CONFIG[selected_client]
    project_id = config["project_id"]
    dataset_id = config["dataset_id"]
    credentials_path = config["credentials_path"]

    # Criar cliente do BigQuery Data Transfer Service
    bigquery_client = create_bigquery_client(credentials_path)

    # Processar todas as consultas programadas
    for query_config in config["scheduled_queries"]:
        create_scheduled_query(
            bigquery_client,
            project_id,
            dataset_id,
            query_config
        )