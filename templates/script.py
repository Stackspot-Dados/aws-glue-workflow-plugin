import logging
import sys
import boto3
import json
from awsglue.utils import getResolvedOptions
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

glue_client = boto3.client('glue')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# Step 1: update crawler
def update_crawler(crawler_name: str, bucket_nome: str, path_s3: str) -> None:
    try:
        response = glue_client.update_crawler(
            Name=crawler_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': bucket_nome + '/' + path_s3
                    }
                ]
            }
        )
    except Exception as e:
        logger.error(e)
        raise Exception('Erro ao executar crawler - Crawler: {}'.format(crawler_name))

def get_crawler_name(db_crawler_relation: str, data_base_glue: str) -> str: 
    try: 
        dict_db_crawler_relation = json.loads(db_crawler_relation) 
        return dict_db_crawler_relation[data_base_glue] 
    except ValueError as e: 
        logger.error(e) 
        raise ValueError("Erro ao carregar JSON do parâmetro DB_CRAWLER_RELATION") 
    except KeyError as e: 
        logger.error(e) 
        raise KeyError("Não foi encontrado o crawler associado ao banco de dados - Crawler: ? | Banco de dados (Glue): {}".format(data_base_glue))

# Step 2: limpar tabelas do glue
def list_tables_of_glue_database(database_name: str) -> list: 
    tables_list = [] 
    try: 
        response_get_tables = glue_client.get_tables(DatabaseName=database_name) 
    except Exception as e: 
        logger.error(e) 
        raise Exception( 'Erro ao obter as tabelas do banco de dados - Banco de dados (Glue): {}' .format(database_name) ) 
    tables_list = list(map(lambda table: table['Name'], response_get_tables.get('TableList')))
    logger.info( 'Banco de dados (Glue): {} | Quantidade de tabelas: {} | Tabelas: {}'.format(database_name, len(tables_list), tables_list) ) 
    return tables_list
    
def delete_tables_in_glue_database(tables_list: list, database_name: str) -> None: 
    logger.info( 'Iniciando exclusão das tabelas do banco de dados - Banco de dados (Glue): {} | Tabelas: {}' .format(database_name, tables_list) ) 
    try: 
        glue_client.batch_delete_table(DatabaseName=database_name, TablesToDelete=tables_list) 
    except Exception as e: 
        logger.error(e) 
        raise Exception( 'ERROR: Erro ao deletar as tabelas do banco de dados - Banco de dados (Glue): {}' .format(database_name) ) 
    logger.info( 'Finalizada a exclusão das tabelas do banco de dados - Banco de dados (Glue): {}' .format(database_name) )

# Step 3: startar o crawler
def start_crawler(crawler_name: str, bucket_nome: str, path_s3: str) -> None:
    logger.info('Iniciando a execução do crawler - Crawler: {}'.format(crawler_name)) 
    try: 
        glue_client.start_crawler( Name=crawler_name ) 
    except Exception as e: 
        logger.error(e) 
        raise Exception('Erro ao executar crawler - Crawler: {}'.format(crawler_name))

def get_crawler_name(db_crawler_relation: str, data_base_glue: str) -> str: 
    try: 
        dict_db_crawler_relation = json.loads(db_crawler_relation) 
        return dict_db_crawler_relation[data_base_glue] 
    except ValueError as e: 
        logger.error(e) 
        raise ValueError("Erro ao carregar JSON do parâmetro DB_CRAWLER_RELATION") 
    except KeyError as e: 
        logger.error(e) 
        raise KeyError("Não foi encontrado o crawler associado ao banco de dados - Crawler: ? | Banco de dados (Glue): {}".format(data_base_glue))

# Step 4: limpar s3
def list_folders(bucket_resource, prefixo_pasta_s3: str) -> list:
    try:
        objects_bucket = s3_client.list_objects_v2(
            Bucket=bucket_resource.name,
            Delimiter='/',
            Prefix=prefixo_pasta_s3)['CommonPrefixes']
    except s3_client.exceptions.NoSuchBucket as e:
        logger.error(e)
        raise Exception(
            "Bucket não existe - Bucket: {}"
            .format(bucket_resource.name)
        )
    except KeyError as e:
        logger.error(e)
        raise KeyError(
            "Snapshot não existe no bucket - Bucket: {} | Prefixo: {}"
            .format(bucket_resource.name, prefixo_pasta_s3)
        )
    return list(
                map(lambda object: object['Prefix'],
                    filter(
                        lambda object: object['Prefix'].startswith(prefixo_pasta_s3),
                        objects_bucket
                    )
                )
            )

def create_dictionary_with_path_and_date(folders_list: list) -> list:
    try:
        path_with_dates = list(
                map(
                    lambda folder: {'folderName': folder,'date': datetime.strptime(folder[-5:-1] + '-' + folder[-7:-5] + '-' + folder[-9:-7], '%Y-%m-%d')},
                    folders_list
                )
            )
        return sorted(path_with_dates, key=lambda path: path['date'])
    except Exception as e:
        logger.error(e)
        raise Exception(
            "Erro ao criar dicionário com a estrutura de snapshots disponíveis em produção"
        )

def delete_folder(bucket_resource, prefixo_pasta_s3: str) -> None:
    logger.info(
        'Iniciando deleção de objetos no S3 | Pasta: {}'
        .format(prefixo_pasta_s3)
    )
    for object in bucket_resource.objects.filter(Prefix=prefixo_pasta_s3):
        s3.Object(bucket_resource.name, object.key).delete()
    logger.info(
        'Finalizada deleção de objetos no S3 | Pasta: {}'
        .format(prefixo_pasta_s3)
    )

def main(): 
    args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'DB_CRAWLER_RELATION']) 
    workflow_name = args['WORKFLOW_NAME'] 
    workflow_run_id = args['WORKFLOW_RUN_ID'] 
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)['RunProperties'] 
    DATABASE_GLUE = workflow_params['DATABASE_GLUE'] 
    BUCKET_NOME = workflow_params['BUCKET_NOME'] 
    PREFIXO_PASTA_S3 = workflow_params['PREFIXO_PASTA_S3'] 
    update_crawler(get_crawler_name(args['DB_CRAWLER_RELATION'], DATABASE_GLUE), BUCKET_NOME, PREFIXO_PASTA_S3)

    while True:
        tables_list = list_tables_of_glue_database(DATABASE_GLUE)
        if len(tables_list) == 0:
            break

        delete_tables_in_glue_database(tables_list, DATABASE_GLUE)

    logger.info('Atualizando dados do banco de dados - Banco de dados (Glue): {}'.format(DATABASE_GLUE)) 
    start_crawler(get_crawler_name(args['DB_CRAWLER_RELATION'], DATABASE_GLUE),BUCKET_NOME, PREFIXO_PASTA_S3)

    bucket_resource = s3.Bucket(BUCKET_NOME)
    logger.info(
        '____________ Atualizando snapshot - Bucket: {} - Prefixo: {} ____________'
            .format(BUCKET_NOME, PREFIXO_PASTA_S3)
    )

    database_snapshot = PREFIXO_PASTA_S3.split('/')[0] + '/'
    folders_list = list_folders(bucket_resource, database_snapshot)

    logger.info('Lista de snapshots disponíveis em {}: {}'.format(database_snapshot, folders_list))
    path_with_dates = create_dictionary_with_path_and_date(folders_list)

    while len(path_with_dates) > 3:
        delete_folder(bucket_resource, path_with_dates[0]['folderName'])
        path_with_dates.pop(0)

    logger.info('Mantendo apenas os seguintes snapshots: {}'.format(list_folders(bucket_resource, database_snapshot)))

main()