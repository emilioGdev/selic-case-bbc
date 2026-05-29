# Pipeline de Dados SELIC — Banco Central do Brasil

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Parquet](https://img.shields.io/badge/Storage-Parquet-50ABF1?style=flat-square&logo=apacheparquet&logoColor=white)
![Status](https://img.shields.io/badge/Status-Concluído-28a745?style=flat-square)

Solução desenvolvida para o case técnico de Engenharia de Dados da **beAnalytic**. O projeto automatiza o pipeline de extração, tratamento e consolidação analítica dos dados históricos da Taxa SELIC diária (período de 2020 a 2024), consumindo a API oficial do Banco Central do Brasil (SGS — Sistema Gerenciador de Séries Temporais).

A infraestrutura é orquestrada pelo **Apache Airflow** e executada em ambiente isolado via **Docker Compose**.

---

## Sumário

1. [Arquitetura — Padrão Medallion](#1-arquitetura--padrão-medallion)
2. [Stack Tecnológica](#2-stack-tecnológica)
3. [Decisões Técnicas e Objetos de Avaliação](#3-decisões-técnicas-e-objetos-de-avaliação)
4. [Estrutura do Repositório](#4-estrutura-do-repositório)
5. [Instruções para Execução Local](#5-instruções-para-execução-local-e-comandos-operacionais)

---

## 1. Arquitetura — Padrão Medallion

O pipeline divide o ciclo de vida do dado em três camadas lógicas independentes, armazenadas localmente em formato **Parquet** na pasta `data/`:

```
┌─────────────────────────────────────────────────────────────────┐
│                        API BCB (SGS 11)                         │
│                    Taxa SELIC Diária 2020–2024                  │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP REST (JSON)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE — Ingestão                                           │
│  bronze/ingestion.py                                            │
│  Persistência do payload bruto sem transformações               │
│  → data/bronze/selic_raw.parquet                                │
└────────────────────────────┬────────────────────────────────────┘
                             │ Leitura Parquet
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥈 SILVER — Transformação + Data Quality                       │
│  silver/transformation.py                                       │
│  Conversão de tipos · Ordenação · Validações                    │
│  → data/silver/selic_clean.parquet                              │
└────────────────────────────┬────────────────────────────────────┘
                             │ Leitura Parquet
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥇 GOLD — Agregação Financeira                                 │
│  gold/aggregation.py                                            │
│  Média mensal · Variação % · Acumulado anual (juros compostos)  │
│  → data/gold/selic_metrics.parquet                              │
└─────────────────────────────────────────────────────────────────┘
```

| Camada | Script | Saída |
|--------|--------|-------|
| **Bronze** (Ingestão) | `bronze/ingestion.py` | `data/bronze/selic_raw.parquet` |
| **Silver** (Transformação) | `silver/transformation.py` | `data/silver/selic_clean.parquet` |
| **Gold** (Agregação) | `gold/aggregation.py` | `data/gold/selic_metrics.parquet` |

- **Camada Bronze:** Consome a API REST do BCB via requisição HTTP, extrai o payload bruto em formato JSON e o persiste sem alterações estruturais.
- **Camada Silver:** Lê o arquivo da Bronze, executa conversão de tipos (`data` → datetime, `valor` → float), ordena cronologicamente os registros e aplica regras de Data Quality.
- **Camada Gold:** Consome o dado limpo da Silver para calcular os indicadores consolidados: média mensal, variação percentual da média entre meses consecutivos e a taxa acumulada anual.

---

## 2. Stack Tecnológica

| Categoria | Tecnologia | Finalidade |
|-----------|-----------|------------|
| Linguagem | `Python 3.11` | Desenvolvimento dos scripts de pipeline |
| Orquestração | `Apache Airflow 2.x` | Agendamento e monitoramento das DAGs |
| Containerização | `Docker / Docker Compose` | Isolamento e reprodutibilidade do ambiente |
| Manipulação de dados | `pandas` | Transformações, agregações e Data Quality |
| Formato de armazenamento | `Apache Parquet` via `pyarrow` | Persistência eficiente em formato colunar |
| Ingestão HTTP | `requests` | Consumo da API REST do BCB |
| Banco de metadados | `PostgreSQL` | Backend de metadados do Airflow |

---

## 3. Decisões Técnicas e Objetos de Avaliação

### Orquestração da DAG e Estrutura do Código

- **`schedule=None` e `catchup=False`:** Como a API fornece um intervalo de tempo fixo e estático (2020–2024), a DAG foi configurada para execução sob demanda. O agendamento recorrente foi desativado para evitar requisições redundantes e desperdício de recursos computacionais. O `catchup=False` impede que o Airflow crie execuções passadas para cada dia do intervalo de cinco anos, processando o histórico em uma única execução unificada.
- **Resiliência e Retentativas:** Configuração de `retries` e `retry_delay` no nível da DAG para mitigar falhas temporárias de rede ou indisponibilidade da API do BCB.
- **Modularidade e PEP-8:** Código segmentado em scripts Python dedicados por camada, com assinaturas tipadas (*type hints*), docstrings descritivas e conformidade com as boas práticas do PEP-8.

### Lógica Financeira na Camada Gold

**Capitalização de Juros Compostos:** A série SGS 11 reporta a taxa SELIC em percentual diário. No contexto do mercado financeiro, somar taxas linearmente (`.sum()`) para obter o acumulado de um período longo é metodologicamente incorreto.

Para garantir precisão, a camada Gold:
1. Converte a taxa diária em fator geométrico: `(1 + taxa / 100)`
2. Calcula o produtório acumulado anual via `.prod()` por grupo de ano
3. Reverte o valor final para a base percentual

### Qualidade de Dados (Data Quality)

Antes de persistir os dados na camada Silver, o pipeline executa testes automatizados que validam a integridade dos dados brutos:

- DataFrame não está vazio
- Ausência de valores nulos nos campos estruturais
- Consistência de mercado (taxas SELIC não podem ser negativas)

Qualquer falha aciona uma exceção que interrompe o fluxo imediatamente, impedindo a contaminação das camadas analíticas subsequentes.

---

## 4. Estrutura do Repositório

```text
├── dags/
│   └── dag_selic_bcb.py        # Definição e topologia da DAG do Airflow
├── bronze/
│   └── ingestion.py            # Script de ingestão da API do BCB
├── silver/
│   └── transformation.py       # Script de limpeza e testes de Data Quality
├── gold/
│   └── aggregation.py          # Script de cálculo das métricas financeiras
├── data/                       # Data Lake simulado (Bronze, Silver e Gold em Parquet)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yaml         # Configuração dos containers (Airflow + Postgres)
├── requirements.txt            # Dependências das bibliotecas Python
└── README.md                   # Documentação técnica do projeto
```

> **Nota:** O diretório `data/` deve ser adicionado ao `.gitignore` para evitar o versionamento dos arquivos Parquet gerados localmente.

---

## 5. Instruções para Execução Local e Comandos Operacionais

### Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e em execução
- [Git](https://git-scm.com/) instalado

### Passo a Passo para Inicialização

**1. Clonar o repositório e acessar a pasta correta:**

```bash
git clone https://github.com/emilioGdev/selic-case-bbc.git
cd selic-case-bbc/beAnalyticss
```

**2. Inicializar o banco de dados do Airflow:**

Prepara o ambiente de metadados criando as tabelas necessárias no Postgres interno.

```bash
docker compose up airflow-init
```

> ⚠️ Aguarde o encerramento do container (código de saída `0`) antes de prosseguir.

**3. Subir os serviços em segundo plano:**

Inicializa os containers do Postgres, Scheduler e Webserver.

```bash
docker compose up -d
```

---

### Comandos de Monitoramento e Validação

**Verificar o status dos containers:**

```bash
docker compose ps
```

> O container do Webserver deve exibir o status `Up (healthy)`. O serviço foi mapeado para a porta **8089** para evitar conflitos de conectividade.

**Acessar a Interface Gráfica:**

Abra o navegador e acesse: [http://localhost:8089](http://localhost:8089)

| Campo | Valor |
|-------|-------|
| Usuário | `airflow` |
| Senha | `airflow` |

**Executar o pipeline via terminal:**

Para acionar o pipeline manualmente sem a interface web:

```bash
docker compose exec airflow-scheduler airflow dags trigger pipeline_selic_bcb
```

**Visualizar logs de serviço (troubleshooting):**

```bash
docker compose logs airflow-webserver
```

**Encerrar o ambiente:**

Para parar os containers e liberar os recursos da máquina após os testes:

```bash
docker compose down
```

---

> Os resultados processados pelo pipeline são gravados automaticamente no diretório local `data/` após a execução bem-sucedida de todas as tarefas.

---

<div align="center">
  Desenvolvido por <strong>Emilio G.</strong> · Case Técnico beAnalytic · Engenharia de Dados
</div>
