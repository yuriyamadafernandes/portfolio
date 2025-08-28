import os   
from config import CLIENTS_CONFIG
from bigquery_utils import create_bigquery_client, create_external_table, create_scheduled_query
from google.cloud import bigquery, bigquery_datatransfer

def validate_client_config(client_name):
    """
    Valida se o cliente existe nas configurações e se todas as chaves obrigatórias estão presentes.
    Retorna a configuração do cliente ou None em caso de erro.
    """
    if client_name not in CLIENTS_CONFIG:
        print(f"Erro: O cliente '{client_name}' não está configurado.")
        return None

    config = CLIENTS_CONFIG[client_name]
    required_keys = ["project_id", "tag", "tables", "credentials", "scheduled_queries", "google_sheets_folder_id"]
    missing_keys = [key for key in required_keys if key not in config]

    if missing_keys:
        print(f"Erro: As seguintes chaves estão faltando na configuração de '{client_name}': {missing_keys}")
        return None

    print(f"Configuração do cliente '{client_name}' carregada com sucesso!")
    return config

def create_dataset_if_not_exists(bigquery_client, project_id, dataset_id):
    """
    Cria o dataset no BigQuery se ele ainda não existir.
    """
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        bigquery_client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' já existe.")
    except Exception:
        # Criar o dataset se ele não existir
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        bigquery_client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' criado com sucesso.")

def process_tables(bigquery_client, project_id, dataset_id, tables):
    """
    Processa todas as tabelas externas vinculadas ao Google Sheets.
    """
    for table_config in tables:
        table_id = table_config["table_id"]
        sheet_id = table_config["sheet_id"]
        sheet_range = table_config["sheet_range"]
        schema = table_config.get("schema")  # Schema pode ser None
        
        print(f"Processando tabela: {dataset_id}.{table_id}")
        create_external_table(
            bigquery_client,
            project_id,
            dataset_id,
            table_id,
            sheet_id,
            sheet_range,
            schema
        )
    
def table_exists(bigquery_client, project_id, dataset_id, table_id):
    """
    Verifica se uma tabela existe no BigQuery.
    """
    try:
        bigquery_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        return True
    except Exception:
        return False

def create_table_if_not_exists(bigquery_client, project_id, dataset_id, table_id, schema):
    """
    Cria a tabela no BigQuery se ela ainda não existir.
    """
    if not table_exists(bigquery_client, project_id, dataset_id, table_id):
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        print(f"Tabela '{table_id}' criada com sucesso no dataset '{dataset_id}'.")

def process_scheduled_queries(project_id, dataset_id, scheduled_queries, credentials, tag):
    """
    Processa todas as consultas programadas no BigQuery.
    """
    bigquery_client = create_bigquery_client(credentials)

    # Criar o dataset se ele não existir
    create_dataset_if_not_exists(bigquery_client, project_id, dataset_id)

    for query_config in scheduled_queries:
        original_query_name = query_config["query_name"]
        # Adicionar a tag ao nome da consulta
        query_name = f"{tag}_{original_query_name.replace('-', '_').upper()}"
        start_time = query_config["start_time"]
        end_time = query_config["end_time"]
        schedule_interval = query_config["schedule_interval"]
        query = query_config["query"]  # Consulta SQL diretamente da configuração

        # Gerar apenas o nome da tabela (sem o dataset)
        destination_table_name = original_query_name.replace('-', '_').upper()

        # Verificar se a tabela de destino existe
        if not table_exists(bigquery_client, project_id, dataset_id, destination_table_name):
            print(f"Tabela de destino '{dataset_id}.{destination_table_name}' não existe. Criando...")
            # Não definir o schema manualmente; o BigQuery inferirá o schema a partir da consulta SQL
            create_table_if_not_exists(bigquery_client, project_id, dataset_id, destination_table_name, schema=None)

        print(f"Criando consulta programada: {dataset_id}.{query_name}")
        create_scheduled_query(
            project_id,
            query,
            query_name=query_name,  # Passar o nome modificado aqui
            destination_table_name=destination_table_name,  # Passar apenas o nome da tabela
            start_time=start_time,
            end_time=end_time,
            schedule_interval=schedule_interval,
            credentials=credentials,
            dataset_id=dataset_id  # Passar o dataset_id para especificar onde criar a tabela
        )

def main(client_name, task="all"):
    """
    Função principal que coordena todo o processo para um cliente específico.
    Args:
        client_name (str): Nome do cliente.
        task (str): Tarefa específica a ser executada ("tables", "queries", ou "all").
    """
    print(f"Iniciando o processo para o cliente: {client_name}...")
    
    # Validar e carregar a configuração do cliente
    config = validate_client_config(client_name)
    if not config:
        return

    # Extrair configurações necessárias
    project_id = config["project_id"]
    tag = config["tag"]
    dataset_id = tag  # Dataset é igual à TAG
    tables = config["tables"]
    credentials = config["credentials"]
    scheduled_queries = config["scheduled_queries"]

    # Criar cliente do BigQuery
    try:
        bigquery_client = create_bigquery_client(credentials)
    except Exception as e:
        print(f"Erro ao criar o cliente do BigQuery: {e}")
        return

    # Criar o dataset apenas uma vez
    create_dataset_if_not_exists(bigquery_client, project_id, dataset_id)

    # Executar tarefas específicas ou todas
    if task == "tables" or task == "all":
        print("Executando a criação de tabelas...")
        process_tables(bigquery_client, project_id, dataset_id, tables)

    if task == "queries" or task == "all":
        print("Executando a criação de consultas programadas...")
        process_scheduled_queries(project_id, dataset_id, scheduled_queries, credentials, tag)

if __name__ == "__main__":
    # Escolha o cliente desejado
    selected_client = "cliente1"  # Altere para "cliente2", "cliente3", etc.

    # Escolha a tarefa a ser executada ("tables", "queries", ou "all")
    selected_task = "queries"  # Altere para "tables" ou "queries" conforme necessário

    main(selected_client, task=selected_task)   