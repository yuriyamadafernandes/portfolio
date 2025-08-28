    # bigquery_utils.py

from google.cloud import bigquery, bigquery_datatransfer
from google.oauth2 import service_account
import datetime

def create_bigquery_client(credentials):
    """
    Cria um cliente do BigQuery usando as credenciais fornecidas diretamente.
    """
    try:
        credentials_obj = service_account.Credentials.from_service_account_info(
            credentials,
            scopes=[
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform"
            ]
        )
        print("Credenciais carregadas com sucesso!")
        return bigquery.Client(credentials=credentials_obj)
    except Exception as e:
        print(f"Erro ao carregar credenciais: {e}")
        raise


def create_external_table(client, project_id, dataset_id, table_id, sheet_id, sheet_range, schema=None):
    """
    Cria uma tabela externa vinculada ao Google Sheets no BigQuery.
    Substitui a tabela se ela já existir.
    """
    try:
        # Verificar ou criar o dataset
        dataset_ref = f"{project_id}.{dataset_id}"
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset '{dataset_id}' já existe.")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            client.create_dataset(dataset)
            print(f"Dataset '{dataset_id}' criado.")

        # Configurar a tabela externa vinculada ao Google Sheets
        external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
        external_config.source_uris = [f"https://docs.google.com/spreadsheets/d/{sheet_id}"]
        external_config.options.skip_leading_rows = 1  # Ignorar a primeira linha (cabeçalhos)

        # OBS: O BigQuery Python SDK não suporta especificar "range" via código, apenas a primeira aba será usada

        if schema:
            validated_schema = [
                bigquery.SchemaField(field["name"], field["type"]) for field in schema
            ]
            external_config.schema = validated_schema
        else:
            external_config.autodetect = True  # Usar autodetecção se o schema não for fornecido

        # Montar a referência completa da tabela
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        # Excluir a tabela existente, se necessário
        try:
            client.delete_table(full_table_id)
            print(f"Tabela '{full_table_id}' excluída para substituição.")
        except Exception as e:
            if "Not found" not in str(e):
                print(f"Erro ao excluir a tabela: {e}")
                raise

        # Criar a tabela externa
        table = bigquery.Table(full_table_id)
        table.external_data_configuration = external_config
        table = client.create_table(table)
        print(f"Tabela externa '{full_table_id}' criada com sucesso.")

    except Exception as e:
        print(f"Erro ao criar a tabela externa: {e}")
        raise



def create_scheduled_query(project_id, query, query_name, destination_table_name, start_time, end_time, schedule_interval, credentials, dataset_id=None):
    """
    Cria uma consulta programada no BigQuery usando o Data Transfer Service.
    """
    try:
        # Criar credenciais explícitas para o cliente do Data Transfer
        credentials_obj = service_account.Credentials.from_service_account_info(
            credentials,
            scopes=[
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive.readonly"
            ]
        )

        # Criar um cliente do BigQuery Data Transfer
        transfer_client = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials_obj)

        # Construir o nome do pai (parent)
        parent = f"projects/{project_id}/locations/US"

        # Converter start_time e end_time para timestamps
        start_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        end_time = datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")

        # Configurar a consulta programada
        transfer_config = {
            "display_name": query_name,  # Nome da consulta programada
            "data_source_id": "scheduled_query",  # Identificador da fonte de dados
            "params": {
                "query": query,  # Consulta SQL a ser executada
                "destination_table_name_template": destination_table_name,  # Apenas o nome da tabela
                "write_disposition": "WRITE_TRUNCATE",  # Sobrescrever os dados existentes
                "partitioning_field": "",  # Sem particionamento por campo
            },
            "schedule": schedule_interval,  # Intervalo de execução
            "schedule_options": {
                "start_time": {"seconds": int(start_time.timestamp())},  # Tempo de início
                "end_time": {"seconds": int(end_time.timestamp())},  # Tempo de término
            },
        }

        # Criar a configuração de transferência
        response = transfer_client.create_transfer_config(
            parent=parent,
            transfer_config=transfer_config
        )
        print(f"Consulta programada '{query_name}' criada com sucesso! ID: {response.name}")
        
        # Se o dataset foi especificado, configurar o dataset de destino
        if dataset_id:
            print(f"Dataset de destino: {dataset_id}")
            print("NOTA: Para configurar o dataset de destino, vá ao BigQuery Console e edite a consulta programada.")

    except Exception as e:
        print(f"Erro ao criar a consulta programada: {e}")
        raise