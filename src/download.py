import sidrapy
import datetime
from pathlib import Path
import logging
import pandas as pd
import sys

def main():
    logger = logging.getLogger(__name__)

    current_year = datetime.datetime.now().year
    first_year = 2019
 
    project_dir = Path(__file__).resolve().parent.parent
    print(project_dir)
    data_dir = project_dir / "files"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Pasta de dados criada ou já existente em: {data_dir}")
    logger.info("Iniciando o processo de download de arquivos anuais da Pesquisa da Pecuária Municipal (PPM) - Produção de origem animal, por tipo de produto")
    
    # Códigos de municípios do estado de Rondônia (RO)
    municipality_codes_ro = [
        "1100106", "1100205", "1100015", "1100023", "1100049", "1100056",
        "1100064", "1100080", "1100098", "1100114", "1100122", "1100155",
        "1100189", "1100254", "1100288", "1100304", "1100320", "1100338",
        "1100130", "1100148", "1100296", "1100346", "1100031", "1100379",
        "1100452", "1100924", "1100940", "1101435", "1101450", "1101468",
        "1101476", "1101484", "1101492", "1101559", "1101757", "1100072",
        "1100262", "1100403", "1100502", "1100601", "1100700", "1100809",
        "1100908", "1101005", "1101104", "1101203", "1101302", "1101401",
        "1101500", "1101609", "1101708", "1101807"
    ]
    
    municipalities_str = ",".join(municipality_codes_ro)
    
    for year in range(first_year, current_year):
        try:
            logger.info(f"Iniciando download para o ano: {year}")
            
            sidra_data = sidrapy.get_table(
                table_code="74",
                territorial_level="6",
                ibge_territorial_code=municipalities_str,
                period=f"{year}",
                variable="all",
                classifications={
                    "80": "0,2682,2685,2686,2687,2683,2684"
                }
            )
            
            df = pd.DataFrame(sidra_data)

            df.columns = df.iloc[0]
            df = df.iloc[1:, :]

            file_name = f"PPM_RO_PRODUCAO_ORIGEM_ANIMAL_{year}.xlsx"
            full_path = data_dir / file_name
            
            df.to_excel(full_path, index=False)
            
            logger.info(f"Arquivo '{file_name}' gerado com sucesso em: {full_path}")
            
        except Exception as e:
            logger.error(f"Ocorreu um erro ao gerar o arquivo Excel para o ano {year}: {e}")

if __name__ == '__main__':
    main()