#%%
from utils import (
    ZAPIClient, select_phone_number, validar_cpf, TemplateMensagem
)
import pandas as pd
import numpy as np
from tqdm import tqdm
import os

nome_projeto = "aviso_pontos_a_expirar"

#%%
# Read Data
ranking_associados = pd.read_csv(
    "data/ranking_sempre_leitura_20250307180134.csv",
    sep=";", 
    decimal=",",
    thousands=".",
    dtype={"Telefone": str, "Telefone 2": str, "Cliente": str}
)

historico_pontuacao = pd.read_excel(
    "data/pontuacao_sempre_leitura_20250307172925.xls",
    decimal=",",
    thousands=".",
    dtype={"Cliente": str}
)

#%%
# Prepare the data
historico_pontuacao = historico_pontuacao\
    .rename(columns={
        "Data/Hora": "date",
        "Pontos": "pontos_a_expirar"
    })\
    .assign(
        date = lambda x: pd.to_datetime(x["date"], format='%d/%m/%Y %H:%M:%S'),
        data_a_expirar = lambda x: x["date"] + pd.DateOffset(months=12) - pd.DateOffset(days=1),
        pontos_a_expirar = lambda x: x["pontos_a_expirar"].astype(float)
    )
    
#%%
# Filter clients that have points to expire
data_inicial = pd.Timestamp.now() + pd.DateOffset(days=10)
data_final = pd.Timestamp.now() + pd.DateOffset(days=30)

clientes_selecionados = historico_pontuacao\
    .query("data_a_expirar >= @data_inicial and data_a_expirar <= @data_final")\
    .groupby("Cliente")\
    .agg(
        data_a_expirar=("data_a_expirar", "min"),
        pontos_a_expirar=("pontos_a_expirar", "sum")
    )\
    .reset_index()

clientes_selecionados = clientes_selecionados[[
    "Cliente", "data_a_expirar", "pontos_a_expirar"
]]

#%%
ranking_clientes = (
    ranking_associados
        .merge(
            clientes_selecionados, on="Cliente", how="inner"
        )
        .sort_values("Saldo", ascending=False)
        .assign(
            telefone_contato = lambda x: x.apply(
                lambda row: select_phone_number(row["Telefone"], row["Telefone 2"]),
                axis=1
            )
        )
)

#%%
# Prepare the message that is going to be sent
sempreleitura = (
    ranking_clientes
        .query("Saldo > 1000")
        .sort_values("Saldo", ascending=False) 
        .query("telefone_contato.notnull()")
        .assign(
            cpf = lambda x: x["Cliente"].apply(validar_cpf),
            dinheiro = lambda x: np.floor(x["Saldo"] / 100),
            primeiro_nome = lambda x: x["Nome Cliente"].str.split(" ").str[0].str.capitalize(),
            telefone_contato = lambda x: x["telefone_contato"].astype(str)
        ).
        query("cpf.notnull()")
        .reset_index(drop=True)
)

sempreleitura['mensagem'] = sempreleitura.apply(
    lambda row: TemplateMensagem(row['primeiro_nome'], row["cpf"]).pontos_a_expirar(
        row["data_a_expirar"], row["pontos_a_expirar"], row['Saldo']
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
