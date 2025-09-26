import logging
import download
# import clean_table
# import create_table_map
# import insert
# import join_files
from pathlib import Path

def setup_master_logging(log_file='processo_completo.log'):
    project_dir = Path(__file__).resolve().parent.parent
    log_dir = project_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / log_file

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_path, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def main():
    # --- Passo 1: Configurar o logger principal (executar apenas uma vez) ---
    setup_master_logging()
    logger = logging.getLogger(__name__)

    logger.info("Iniciando o processo de ETL completo.")

    try:
        logger.info("--- Etapa 1: Download dos arquivos ---")
        download.main()
        
        # logger.info("--- Etapa 2: Unindo os arquivos ---")
        # join_files.main()
        
        # logger.info("--- Etapa 3: Limpando tabelas no banco de dados ---")
        # clean_table.main()
        
        # logger.info("--- Etapa 4: Inserindo dados na tabela principal ---")
        # insert.main()
        
        # logger.info("--- Etapa 5: Criando a tabela de mapa ---")
        # create_table_map.main()
        
        logger.info("Processo concluído com sucesso!")

    except Exception as e:
        logger.error(f"O processo falhou em alguma etapa: {e}")

if __name__ == "__main__":
    main()