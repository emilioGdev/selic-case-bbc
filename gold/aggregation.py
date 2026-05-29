"""
Script de Agregação - Camada Gold.
Lê os dados limpos da Silver, calcula as métricas de negócio exigidas
(média mensal, variação mensal e acumulado anual) e salva o resultado final.
"""

import os
import pandas as pd


def aggregate_selic_data() -> None:
    """
    Agrupa os dados da Silver e gera indicadores consolidados
    mensais e anuais da taxa SELIC utilizando capitalização composta.
    """
    input_path = "/opt/airflow/data/silver/selic_clean.parquet"
    base_dir = "/opt/airflow/data/gold"
    os.makedirs(base_dir, exist_ok=True)
    output_path = os.path.join(base_dir, "selic_metrics.parquet")

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Arquivo limpo não encontrado na Silver: {input_path}"
        )

    print(f"Lendo dados da Silver: {input_path}")
    df = pd.read_parquet(input_path)

    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month

    print("Calculando agregados mensais (Média e Variação)...")
    monthly_df = (
        df.groupby(["ano", "mes"])["valor"].mean().reset_index()
    )
    monthly_df = monthly_df.rename(columns={"valor": "media_mensal"})
    monthly_df = monthly_df.sort_values(by=["ano", "mes"]).reset_index(drop=True)

    monthly_df["variacao_mensal_pct"] = monthly_df["media_mensal"].pct_change() * 100
    monthly_df["variacao_mensal_pct"] = monthly_df["variacao_mensal_pct"].fillna(0.0)

    print("Calculando taxa acumulada anual (Juros Compostos)...")
    df["fator"] = 1 + (df["valor"] / 100)
    
    annual_df = df.groupby(["ano"])["fator"].prod().reset_index()
    
    annual_df["taxa_acumulada_anual"] = (annual_df["fator"] - 1) * 100
    annual_df = annual_df[["ano", "taxa_acumulada_anual"]]

    final_gold_df = pd.merge(monthly_df, annual_df, on="ano", how="left")

    final_gold_df.to_parquet(output_path, index=False, engine="pyarrow")
    
    print(f"Métricas consolidadas salvas com sucesso na Gold: {output_path}")


if __name__ == "__main__":
    aggregate_selic_data()