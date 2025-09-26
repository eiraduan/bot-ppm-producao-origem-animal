import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import logging 

def main():
    load_dotenv()
    logger = logging.getLogger(__name__)

    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = os.getenv("DB_PORT", 5432)

    DIRETORIO_SCRIPT = Path(__file__).parent
    PASTA_ARQUIVOS = DIRETORIO_SCRIPT.parent / "files"
    TABELA_DESTINO = "dados_ppm_producao_origem_animal"

    logger.info("Iniciando o processo de ETL (Extrair, Transformar, Carregar)...")

    try:
        url_object = URL.create(
            "postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )
        engine = create_engine(url_object)
        logger.info("Conexão com o banco de dados estabelecida com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        exit()

    arquivo_consolidado = PASTA_ARQUIVOS / "PPM_RO_PRODUCAO_ORIGEM_ANIMAL_FINAL.xlsx"

    if not arquivo_consolidado.exists():
        logger.info(f"Erro: O arquivo '{arquivo_consolidado.name}' não foi encontrado na pasta '{PASTA_ARQUIVOS}'.")
    else:
        try:
            logger.info(f"Processando o arquivo: {arquivo_consolidado.name}")
            
            df = pd.read_excel(arquivo_consolidado)
     
            df.rename(columns={
                "Nível Territorial (Código)": "nivel_territorial_codigo",
                "Nível Territorial": "nivel_territorial",
                "Unidade de Medida (Código)": "unidade_de_medida_codigo",
                "Unidade de Medida": "unidade_de_medida",
                "Valor": "valor",
                "Município (Código)": "municipio_codigo",
                "Município": "municipio",
                "Ano (Código)": "ano_codigo",
                "Ano": "ano",
                "Variável (Código)": "variavel_codigo",
                "Variável": "variavel",
                "Tipo de produto de origem animal (Código)": "tipo_produto_origem_animal_codigo",
                "Tipo de produto de origem animal": "tipo_produto_origem_animal"
            }, inplace=True)

            df['unidade_de_medida_codigo'] = df['unidade_de_medida_codigo'].replace({'','-','..','...'}, pd.NA)
            df['unidade_de_medida'] = df['unidade_de_medida'].replace({'','-','..','...'}, pd.NA)
            df['valor'] = df['valor'].replace({'','-','..','...'}, pd.NA)

    

            if not df.empty:
                logger.info(f"{len(df)} linhas prontas para serem carregadas.")
                
                # 4. Salva os dados no banco de dados
                df.to_sql(
                    name=TABELA_DESTINO,
                    con=engine,
                    if_exists='append', # Adiciona os dados à tabela existente
                    index=False # Não salva o índice do DataFrame como uma coluna
                )
                logger.info(f"Dados do arquivo '{arquivo_consolidado.name}' salvos na tabela '{TABELA_DESTINO}' com sucesso.")
            else:
                logger.info(f"Nenhuma linha encontrada no arquivo {arquivo_consolidado.name}.")
                
        except Exception as e:
            logger.error(f"Erro ao processar o arquivo {arquivo_consolidado.name}: {e}")

    logger.info("Processamento finalizado.")

if __name__ == "__main__":
    main()