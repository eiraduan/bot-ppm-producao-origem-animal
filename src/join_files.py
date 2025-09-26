import pandas as pd
from pathlib import Path
import logging


def juntar_arquivos_excel(pasta_origem: Path):
    logger = logging.getLogger(__name__)

    nome_arquivo_saida = "PPM_RO_PRODUCAO_ORIGEM_ANIMAL_FINAL.xlsx"
    caminho_completo_saida = pasta_origem / nome_arquivo_saida
    
    if caminho_completo_saida.exists():
        logger.info(f"Arquivo '{nome_arquivo_saida}' existente. Apagando...")
        caminho_completo_saida.unlink()
    
    logger.info(f"Buscando arquivos na pasta: {pasta_origem}")
    
    lista_de_dataframes = []

    arquivos_excel = sorted([
        arquivo for arquivo in pasta_origem.glob("PPM_RO_PRODUCAO_ORIGEM_ANIMAL_*.xlsx") 
        if arquivo.name != nome_arquivo_saida
    ])

    if not arquivos_excel:
        logger.error("Não foram encontrados arquivos .xlsx para serem unidos.")
        return

    for arquivo in arquivos_excel:
        logger.info(f"Lendo arquivo: {arquivo.name}")
        try:
            df_temp = pd.read_excel(arquivo)
            lista_de_dataframes.append(df_temp)
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo {arquivo.name}: {e}")
            continue

    df_final = pd.concat(lista_de_dataframes, ignore_index=True)

    df_final.to_excel(caminho_completo_saida, index=False)

    logger.info("Processo de união concluído com sucesso!")
    logger.info(f"Todos os dados foram salvos no arquivo: {caminho_completo_saida}")
    logger.info(f"O DataFrame final possui {len(df_final)} linhas.")

def main():
    
    diretorio_script = Path(__file__).parent
    pasta_origem_dados = diretorio_script.parent / "files"
    
    juntar_arquivos_excel(pasta_origem_dados)

if __name__ == "__main__":
    main()