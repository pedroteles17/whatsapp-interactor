#%%
from utils import (
    ZAPIClient, select_phone_number, validar_cpf, template_mensagem
)
import pandas as pd
import numpy as np

#%%
ranking_associados = pd.read_csv(
    "data/ranking_sempre_leitura_20250114190058.csv",
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
)

sempreleitura['mensagem'] = sempreleitura.apply(
    lambda row: template_mensagem(
        row['primeiro_nome'], 'Livraria Leitura do Boulevard Shopping', 
        row['Saldo'], row["cpf"]
    ), axis=1
).str.strip().to_list()

#%%
zapi_client = ZAPIClient()

zapi_client.send_image(
    "5531984754371",
    "Welcome to *Z-API*",
    "https://cardano-open-files.s3.us-east-1.amazonaws.com/message_image.jpeg"
)

#%%