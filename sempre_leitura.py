#%%
import pandas as pd
import numpy as np
import logging
from utils import (
    whatsapp_link, template_mensagem, 
    validar_cpf
)

#%%
sempreleitura = pd.read_excel(
    "ranking_sempre_leitura_20241220201721.xls",
    decimal=",",
    thousands=".",
    dtype={"Telefone": str, "Telefone 2": str, "Cliente": str}
)

#%%
sempreleitura['telefone_contato'] = pd.NA

for index, row in sempreleitura.iterrows():
    sempreleitura.at[index, "telefone_contato"] = row["Telefone"] if not pd.isna(row["Telefone"]) else row["Telefone 2"]
    
#%%
sempreleitura = (
    sempreleitura
        .query("Saldo > 1000")
        
        .sort_values("Saldo", ascending=False) 
        .query("telefone_contato.notnull()")
        .assign(
            cpf = lambda x: x["Cliente"].apply(validar_cpf),
            dinheiro = lambda x: np.floor(x["Saldo"] / 100),
            primeiro_nome = lambda x: x["Nome Cliente"].str.split(" ").str[0].str.capitalize(),
            telefone_contato = lambda x: x["telefone_contato"].astype(str),
            whats_app_link = lambda x: x["telefone_contato"].apply(whatsapp_link)
        ).
        query("cpf.notnull()")
)

sempreleitura['mensagem'] = sempreleitura.apply(
    lambda row: template_mensagem(
        row['primeiro_nome'], 'Livraria Leitura do Boulevard Shopping', 
        row['Saldo'], row["cpf"]
    ), axis=1
).str.strip().to_list()

# %%
sempreleitura_export = sempreleitura[[
    "cpf", "primeiro_nome", "telefone_contato", "Saldo",  
    "dinheiro", "whats_app_link", "mensagem"
]]

sempreleitura_export.to_excel("sempre_leitura_com_mensagem.xlsx", index=False)

# %%
