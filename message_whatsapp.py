#%%
from utils import (
    ZAPIClient, SQLServer, SempreLeitura,
    select_phone_number, validar_cpf, TemplateMensagem
)
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import logging

nome_projeto = "aviso_pontos_a_expirar"

data_inicio_pontuacao = pd.Timestamp.now() - pd.DateOffset(months=12) + pd.DateOffset(days=10)
data_fim_pontuacao = data_inicio_pontuacao + pd.DateOffset(days=20)

#%%
sqlserver_db = SQLServer()

query_pontuacao = f"""
SELECT mov.usuario, mov.data_hora, mov.valor, mov.tipo, mov.data_cupom, usr.nome_cliente, usr.ddd, usr.telefone, usr.ddd2, usr.telefone2
FROM sl_movimentacao_conta_corrente mov
LEFT JOIN sl_usuarios usr
ON mov.usuario = usr.usuario
WHERE mov.tipo = 'C'
    AND CONVERT(DATETIME, mov.data_hora, 120) >= CONVERT(DATETIME, '{data_inicio_pontuacao.strftime("%Y-%m-%d")}', 120)
    AND CONVERT(DATETIME, mov.data_hora, 120) < CONVERT(DATETIME, '{data_fim_pontuacao.strftime("%Y-%m-%d")}', 120)
"""

df_pontuacao = sqlserver_db.pandas_read_sql(query_pontuacao)

#%%
pontos_acumulados= df_pontuacao.copy()\
    .groupby("usuario")\
    .agg(
        pontos_acumulados_periodo=("valor", "sum")
    )\
    .reset_index()\
    .merge(
        df_pontuacao.drop_duplicates(subset="usuario"),
        on="usuario",
        how="left"
    )\
    .query("pontos_acumulados_periodo > 500")\
    .sort_values("pontos_acumulados_periodo", ascending=False)\
    .reset_index(drop=True)

#%%
usuarios = pontos_acumulados["usuario"].to_list()

usuarios = usuarios[100:130]

#%%
saldos = []
for usuario in tqdm(usuarios, total=len(usuarios)):
    try:
        sempreleitura = SempreLeitura()

        df = sempreleitura.getMovimentosContaCorrente(
            usuario=usuario
        )

        saldos.append(
            sempreleitura.calculate_balance(df)
        )
    except Exception as e:
        logging.error(f"Error processing user {usuario}: {e}")

saldos = pd.DataFrame(saldos)

#%%
pontos_clientes = (
    saldos
        .merge(
            pontos_acumulados,
            on="usuario",
            how="left"
        )
        .rename(columns={
            "Créditos a Expirar": "creditos_a_expirar",
        })
        .query("creditos_a_expirar > 500")
        .query("Saldo > 1000")
        .sort_values("Saldo", ascending=False)
        .assign(
            filtro_pontuacao_data_inicio = data_inicio_pontuacao,
            filtro_pontuacao_data_fim = data_fim_pontuacao,
            telefone_principal = lambda x: x["ddd"].astype(str) + x["telefone"].astype(str),
            telefone_secundario = lambda x: x["ddd2"].astype(str) + x["telefone2"].astype(str)
        )
        .assign(
            telefone_contato = lambda x: x.apply(
                lambda row: select_phone_number(row["telefone_principal"], row["telefone_secundario"]),
                axis=1
            ),
            data_min_a_expirar = lambda x: x["datas_a_expirar"].apply(
                lambda x: min(x) if isinstance(x, list) and len(x) > 0 else pd.NA
            ),
            data_max_a_expirar = lambda df: df["datas_a_expirar"].apply(
                lambda x: max(x) if isinstance(x, list) and len(x) > 1 else pd.NA
            )
        )
        .reset_index(drop=True)
)

#%%
# Prepare the message that is going to be sent
sempreleitura = (
    pontos_clientes
        .query("telefone_contato.notnull()")
        .assign(
            cpf = lambda x: x["usuario"].apply(validar_cpf),
            dinheiro = lambda x: np.floor(x["Saldo"] / 100),
            primeiro_nome = lambda x: x["nome_cliente"].str.split(" ").str[0].str.capitalize(),
            telefone_contato = lambda x: x["telefone_contato"].astype(str)
        ).
        query("cpf.notnull()")
        .reset_index(drop=True)
)

sempreleitura['mensagem'] = sempreleitura.apply(
    lambda row: TemplateMensagem(row['primeiro_nome'], row["cpf"]).pontos_a_expirar(
        row["data_min_a_expirar"], row["data_max_a_expirar"],
        row["creditos_a_expirar"], row['Saldo']
    ), axis=1
).str.strip().to_list()

#%%
# Filter clients that have already received the message
messages_sent = os.listdir("data/messages_sent/")

if len(messages_sent) > 0:
    messages_sent = pd.concat(
        [pd.read_parquet(f"data/messages_sent/{file}") for file in messages_sent]
    )\
        .query("nome_projeto == @nome_projeto")

    sempreleitura = (
        sempreleitura
            .query("cpf not in @messages_sent['Cliente']")
            .reset_index(drop=True)
    )

#%%
# Send messages and save the results
zapi_client = ZAPIClient()

message_results = []
for index, row in tqdm(sempreleitura.iterrows(), total=sempreleitura.shape[0]):
    wpp_response = zapi_client.send_text(
        row['telefone_contato'],
        row['mensagem']
    )

    message_results.append(
        wpp_response.json() | {
            "Cliente": row['cpf'], 
            "data_envio": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            "nome_projeto": nome_projeto
        }
    )
    
messages_results = (
    pd.DataFrame(message_results)
        .merge(sempreleitura, on="Cliente", how="left")
)

#%%
messages_results.to_parquet(
    f"data/messages_sent/{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}.parquet", 
    index=False
)

# %%
