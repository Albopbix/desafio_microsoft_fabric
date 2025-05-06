# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "043273b5-16e4-48d5-8110-9c96fe67973d",
# META       "default_lakehouse_name": "lake_dmf",
# META       "default_lakehouse_workspace_id": "04cc3925-b258-426a-9ace-c7323cfd193c",
# META       "known_lakehouses": [
# META         {
# META           "id": "043273b5-16e4-48d5-8110-9c96fe67973d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Raw Layer Notebook

# CELL ********************

# STEP 1 - Importar bibliotecas
import requests
import json
import os

# STEP 2 - Criar diretório RAW se ainda não existir
os.makedirs("Files/raw/vagas_indeed", exist_ok=True)

# STEP 3 - Configuração da API
url = "https://indeed12.p.rapidapi.com/jobs/search"

querystring = {
    "query": "Data Analyst",
    "page": "1",
    "num_pages": "1",
    "country": "us"
}

headers = {
    "X-RapidAPI-Key": "b30b9b165emshf68d614728c8232p135f17jsn28498a8ed302",  # Substitua pela sua chave
    "X-RapidAPI-Host": "indeed12.p.rapidapi.com"
}

# STEP 4 - Requisição à API
response = requests.get(url, headers=headers, params=querystring)

# STEP 5 - Verificação da resposta e estruturação dos dados
if response.status_code == 200:
    raw_data = response.json()

    # Lista para armazenar os dados processados
    jobs_list = []

    for job in raw_data.get("hits", []):
        salary_data = job.get("salary", {})
        jobs_list.append({
            "company_name": job.get("company_name"),
            "job_id": job.get("id"),
            "job_title": job.get("title"),
            "job_location": job.get("location"),
            "locality": job.get("locality"),
            "posted_date": job.get("formatted_relative_time"),
            "salary_min": salary_data.get("min"),
            "salary_max": salary_data.get("max"),
            "salary_type": salary_data.get("type")
        })

    # Organiza os dados finais
    result = {"jobs": jobs_list}

    # STEP 6 - Salvar o arquivo JSON no path do Lakehouse
    output_path = "Files/raw/vagas_indeed/mock_vagas_indeed_us.json"
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Arquivo JSON salvo com sucesso em: {output_path}")

else:
    print(f"Erro {response.status_code}: {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 7 - Leitura do JSON salvo no Lakehouse
from pyspark.sql.functions import explode, col

# Caminho do arquivo salvo
json_path = "Files/raw/vagas_indeed/mock_vagas_indeed_us.json"

# Leitura do JSON
df_json = spark.read.option("multiline", "true").json(json_path)

# A estrutura tem uma lista dentro da chave "jobs", então precisamos explodir
df_vagas = df_json.select(explode(col("jobs")).alias("vaga")).select("vaga.*")

# Exibir schema (opcional)
df_vagas.printSchema()

# Exibir preview (opcional)
display(df_vagas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Caminho destino para salvar como Delta Table
delta_path = "Tables/vagas_indeed_us"

# Salvar como Tabela Delta
df_vagas.write.mode("overwrite").format("delta").save(delta_path)

# Registrar como tabela no Lakehouse
spark.sql(f"DROP TABLE IF EXISTS vagas_indeed_us")
spark.sql(f"CREATE TABLE vagas_indeed_us USING DELTA LOCATION '{delta_path}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura do arquivo JSON salvo na camada RAW
import pandas as pd
import json

with open("Files/raw/vagas_indeed/mock_vagas_indeed_us.json", "r") as f:
    data = json.load(f)

# Criação do DataFrame
vagas_indeed_us = pd.DataFrame(data["jobs"])

# Exibir o DataFrame
display(vagas_indeed_us)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
