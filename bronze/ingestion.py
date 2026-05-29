"""
Script de Ingestão - Camada Bronze.
Consome os dados brutos da taxa SELIC diária da API do Banco Central do Brasil
e os armazena em formato Parquet no Data Lake simulado.
"""

import os
import requests
import pandas as pd


def ingest_selic_data() -> None:
    """
    Consome a API do BCB e salva os dados brutos em formato Parquet
    na pasta correspondente à camada Bronze.
    """
    url = (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
        "?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024"
    )

    base_dir = "/opt/airflow/data/bronze"
    os.makedirs(base_dir, exist_ok=True)
    output_path = os.path.join(base_dir, "selic_raw.parquet")

    print(f"Iniciando a ingestão de dados da API: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  

        data = response.json()

        df = pd.DataFrame(data)

        df.to_parquet(output_path, index=False, engine="pyarrow")

        print(f"Dados brutos salvos com sucesso em: {output_path}")
        print(f"Total de registros carregados: {len(df)}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a API do BCB: {e}")
        raise e
    except Exception as e:
        print(f"Erro inesperado durante a ingestão: {e}")
        raise e


if __name__ == "__main__":
    ingest_selic_data()