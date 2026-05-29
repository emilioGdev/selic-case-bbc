"""
Script de Transformação - Camada Silver.
Lê os dados brutos da Bronze, aplica conversões de tipos,
ordenação, validações de qualidade (Data Quality) e salva o resultado limpo.
"""

import os
import pandas as pd


def run_data_quality_checks(df: pd.DataFrame) -> None:
    """
    Executa testes de qualidade de dados (Data Quality).
    Se falhar em alguma regra crítica, interrompe o pipeline.
    """
    print("Iniciando checagens de qualidade de dados (Data Quality)...")

    if df.empty:
        raise ValueError("Data Quality FALHOU: O DataFrame está vazio.")

    null_dates = df["data"].isnull().sum()
    null_values = df["valor"].isnull().sum()
    if null_dates > 0 or null_values > 0:
        raise ValueError(
            f"Data Quality FALHOU: Encontrados valores nulos "
            f"(Datas nulas: {null_dates}, Valores nulos: {null_values})."
        )

    if (df["valor"] < 0).any():
        raise ValueError(
            "Data Quality FALHOU: Encontrados valores negativos na taxa SELIC."
        )

    print("Todos os testes de Data Quality passaram com sucesso! ✅")


def transform_selic_data() -> None:
    """
    Lê o Parquet da camada Bronze, padroniza os dados,
    aplica regras de qualidade e salva na camada Silver.
    """
    input_path = "/opt/airflow/data/bronze/selic_raw.parquet"
    base_dir = "/opt/airflow/data/silver"
    os.makedirs(base_dir, exist_ok=True)
    output_path = os.path.join(base_dir, "selic_clean.parquet")

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Arquivo bruto não encontrado na Bronze: {input_path}"
        )

    print(f"Lendo dados brutos da Bronze: {input_path}")
    df = pd.read_parquet(input_path)

    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")

    df["valor"] = df["valor"].astype(float)

    df = df.sort_values(by="data").reset_index(drop=True)

    run_data_quality_checks(df)

    df.to_parquet(output_path, index=False, engine="pyarrow")
    print(f"Dados limpos e validados salvos na Silver: {output_path}")


if __name__ == "__main__":
    transform_selic_data()