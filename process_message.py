#%%
import pandas as pd
import os
from utils import ZAPIClient
from tqdm import tqdm
import logging

zapi_client = ZAPIClient()

#%%
messages_sent_folder = "data/messages_sent/"

# List all Parquet files in the folder
parquet_files = [f for f in os.listdir(messages_sent_folder) if f.endswith('.parquet')]

# Read and concatenate all Parquet files
messages_sent = pd.concat(
    [pd.read_parquet(os.path.join(messages_sent_folder, file)) for file in parquet_files],
    ignore_index=True
)

#%%
# Get the metadata of the messages sent
messages_results = []
for index, row in tqdm(messages_sent.iterrows(), total=messages_sent.shape[0]):
    try:
        message_result = zapi_client.get_chat_metadata(row["telefone_contato"]).json() 
        messages_results.append(
            message_result | {"zaapId": row["zaapId"], "messageId": row["messageId"]}
        )
    except Exception as e:
        logging.error(
            f"Error while getting the metadata: {e}"
        )

#%%
messages_sent = messages_sent\
    .merge(
        pd.DataFrame(messages_results), 
        on=["zaapId", "messageId"], 
        how="left"
    )

#%%
messages_sent.to_parquet("data/messages_sent_with_metadata.parquet", index=False)

messages_sent = pd.read_parquet("data/messages_sent_with_metadata.parquet")

# %%
# Read info on the redemption of points
resgate_sempreleitura = pd.read_excel(
    "data/resgate_sempre_leitura_20250210194104.xls",
    dtype={"Cliente": str},
    decimal=",",
    thousands="."
)

resgate_sempreleitura = resgate_sempreleitura[[
    "Cliente", "Data/Hora", "ID Resgate", "Pontos", "Loja"
]]\
    .rename(columns={"Data/Hora": "Data Resgate"})

#%%
# Merge the messages sent with the redemption of points
messages_sent = messages_sent\
    .merge(
        resgate_sempreleitura, 
        on="Cliente", 
        how="left"
    )
# %%
results = messages_sent\
    .query("message != 'Internal server error'")\
    .assign(
        message=lambda x: x["message"].apply(lambda y: "Phone found" if pd.isna(y) else y),
        has_redeemed=lambda x: x["ID Resgate"].notna()
    )

# %%
results\
    .groupby("message")\
    .sum()
# %%
