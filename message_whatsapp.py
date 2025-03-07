#%%
from utils import (
    ZAPIClient, select_phone_number, validar_cpf, template_mensagem
)
import pandas as pd
import numpy as np
from tqdm import tqdm
import os

#%%
# Read Data
ranking_associados = pd.read_csv(
    "data/ranking_sempre_leitura_20250127205915.csv",
    sep=";", 
    decimal=",",
    thousands=".",
    dtype={"Telefone": str, "Telefone 2": str, "Cliente": str}
)

historico_pontuacao = pd.read_excel(
    "data/historico_pontuacao.ods",
    engine="odf",
    dtype={"Cliente": str}
)

#%%
# FIlter clients from a specific store
clientes_loja = (
    historico_pontuacao
        .query("Loja == 'MG/BH - Boulevard BH'")
)["Cliente"].unique()

ranking_clientes = (
    ranking_associados
        .query("Cliente in @clientes_loja")
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
    lambda row: template_mensagem(
        row['primeiro_nome'], 'Livraria Leitura do Boulevard Shopping', 
        row['Saldo'], row["cpf"]
    ), axis=1
).str.strip().to_list()

#%%
# Filter clients that have already received the message
messages_sent = os.listdir("data/messages_sent/")

if len(messages_sent) > 0:
    messages_sent = pd.concat(
        [pd.read_parquet(f"data/messages_sent/{file}") for file in messages_sent]
    )

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
    wpp_response = zapi_client.send_image(
        row['telefone_contato'],
        row['mensagem'],
        "https://cardano-open-files.s3.us-east-1.amazonaws.com/promocao_va_2025.jpeg"
    )

    message_results.append(
        wpp_response.json() | {
            "Cliente": row['cpf'], 
            "data_envio": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
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

#%%
message_info = ZAPIClient().read_message("3DB74FF113DC20BA87CF46BAC10442D5", "3136817065")
# %%
chats = ZAPIClient().get_chat_metadata("31988954634")
# %%
chats.json()
# %%
message_info.json()
# %%
