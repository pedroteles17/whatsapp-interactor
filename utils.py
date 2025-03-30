import numpy as np
import pandas as pd
import logging
import dotenv
import os
import requests
import re
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from babel import Locale
from babel.dates import format_date

locale = Locale('pt', 'BR')

dotenv.load_dotenv()

def select_phone_number(phone, phone2):
    # If one is null, return the other
    if pd.isna(phone):
        return phone2
    if pd.isna(phone2):
        return phone

    phone, phone2 = str(phone), str(phone2)

    # If its not a mobile phone, return the other
    if len(phone) == 10 and phone[2] in ["1", "3"]:
        return phone2

    return phone if phone else phone2

class SempreLeitura:
    TIPO_MOVIMENTO_CREDITO = "C"
    TIPO_MOVIMENTO_DEBITO = "D"
    TIPO_MOVIMENTO_RESGATE = "R"
    
    ORIGEM_PONTUACAO_COMPRA = "1"
    ORIGEM_RESGATE = "2"
    ORIGEM_MANUTENCAO = "3"

    VALIDADE_PONTOS_DIAS = 365
    DIAS_A_EXPIRAR = 30

    def __init__(self):
        self.engine = SQLServer().define_engine()

    def getMovimentosContaCorrente(self, usuario: str):
        query = f"""
        SELECT 
            mf.*, 
            u.nome_cliente, 
            e.descricao AS nome_loja, 
            ISNULL(mf.data_cupom, CONVERT(VARCHAR(10), mf.data_hora, 120)) AS data_cupom_mod, 
            ISNULL((
                SELECT SUM(ISNULL(hr.valor_resgate, 0))
                FROM sl_historico_resgate hr (NOLOCK)
                WHERE hr.usuario = mf.usuario
                AND hr.extra_info = mf.extra_info
                AND hr.cnpj_empresa_credito = mf.cnpj_empresa
            ), 0) AS valor_resgatado, 
            u.email 
        FROM 
            sl_movimentacao_conta_corrente mf (NOLOCK)
        INNER JOIN 
            sl_usuarios u (NOLOCK) ON u.usuario = mf.usuario
        INNER JOIN 
            simpleset.dbo.ss_empresas e (NOLOCK) ON e.cnpj = mf.cnpj_empresa
        WHERE 
            1 = 1
            AND mf.usuario = '{usuario}'
        ORDER BY 
            ISNULL(mf.data_cupom, CONVERT(VARCHAR(10), mf.data_hora, 120)), 
            mf.id;
        """

        with self.engine.connect() as conn:
            df = pd.read_sql_query(query, conn)

        return df

    def calculate_balance(self, df):
        if df.empty:
            return {}

        # Nos subtraimos um dia a data de expiração para garantir que o último dia seja considerado
        data_limite_expiracao = (datetime.today() - timedelta(days=self.VALIDADE_PONTOS_DIAS))
        data_limite_expiracao = data_limite_expiracao.replace(hour=0, minute=0, second=0, microsecond=0)
        data_limite_a_expirar = (data_limite_expiracao + timedelta(days=self.DIAS_A_EXPIRAR))

        a_totalizadores = {
            'Créditos': 0,
            'Débitos': 0,
            'Saldo': 0,
            'Créditos Expirados': 0,
            'Créditos a Expirar': 0
        }

        datas_a_expirar = []
        for _, movimento in df.iterrows():
            valor = movimento['valor']
            valor_resgatado = movimento.get('valor_resgatado', 0)
            data_cupom = datetime.strptime(movimento['data_cupom_mod'], "%Y-%m-%d")
            
            if movimento['tipo'] == self.TIPO_MOVIMENTO_CREDITO:
                a_totalizadores['Créditos'] += valor
                a_totalizadores['Saldo'] += valor

                if movimento['origem'] == self.ORIGEM_PONTUACAO_COMPRA:
                    valor_disponivel = valor - valor_resgatado
                    if valor_disponivel > 0:
                        if data_cupom < data_limite_expiracao:
                            a_totalizadores['Créditos Expirados'] += valor_disponivel
                        elif data_limite_expiracao <= data_cupom < data_limite_a_expirar:
                            a_totalizadores['Créditos a Expirar'] += valor_disponivel
                            datas_a_expirar.append(data_cupom + timedelta(days=self.VALIDADE_PONTOS_DIAS))

            elif movimento['tipo'] in [self.TIPO_MOVIMENTO_DEBITO, self.TIPO_MOVIMENTO_RESGATE]:
                a_totalizadores['Débitos'] += valor
                a_totalizadores['Saldo'] -= valor

        a_totalizadores['Saldo'] -= a_totalizadores['Créditos Expirados']
        
        return {
            "usuario": df.iloc[0]['usuario'],
            "datas_a_expirar": datas_a_expirar,
        } | a_totalizadores

class SQLServer:
    def __init__(self, server: str = None, database: str = None, username: str = None, password: str = None):
        self.server = server if server else os.getenv("DB_SERVER")
        self.database = database if database else os.getenv("DB_NAME")
        self.username = username if username else os.getenv("DB_USERNAME")
        self.password = password if password else os.getenv("DB_PASSWORD")

        if not all([self.server, self.database, self.username, self.password]):
            raise ValueError("Missing database connection parameters")

        self.engine = self.define_engine()

    def define_engine(self):
        return create_engine(
            f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}?driver=ODBC+Driver+11+for+SQL+Server"
        )

    def disconnect(self):
        self.engine.dispose()
    
    def pandas_read_sql(self, query: str, params: dict = None):
        """
        Executes a parameterized SQL query safely and returns a Pandas DataFrame.
        
        :param query: SQL query with named placeholders (e.g., "SELECT * FROM users WHERE id = :id")
        :param params: Dictionary of parameters to safely inject into the query
        :return: Pandas DataFrame with the query result
        """
        self._validate_sql_read_query(query)
        with self.engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        return df

    def _validate_sql_read_query(self, query: str):
        """Validates SQL query to check for suspicious patterns"""
        dangerous_patterns = [
            r"(--|#)",  # SQL comments
            r"(/\*.*\*/)",  # Block comments
            #r"(;)",  # Multiple statements
            r"\b(DELETE|DROP|TRUNCATE|INSERT|UPDATE)\b"  # DDL/DML operations
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                raise ValueError("Potential SQL injection detected!")

class ZAPIClient:
    def __init__(self, instance_id=None, instance_token=None, client_token=None):
        self.instance_id = instance_id if instance_id else os.getenv("ZAPI_INSTANCE_ID")
        self.instance_token = instance_token if instance_token else os.getenv("ZAPI_INSTANCE_TOKEN")
        self.client_token = client_token if client_token else os.getenv("ZAPI_CLIENT_TOKEN")

    def send_text(self, phone, message, delay_message=10):
        url = f"https://api.z-api.io/instances/{self.instance_id}/token/{self.instance_token}/send-text"
        headers = {
            "client-token": self.client_token,
            "Content-Type": "application/json"
        }
        data = {
            "phone": phone,
            "message": message,
            "delayMessage": delay_message
        }

        return requests.post(url, headers=headers, json=data)

    def send_image(self, phone, caption, image_url, delay_message=10):
        url = f"https://api.z-api.io/instances/{self.instance_id}/token/{self.instance_token}/send-image"
        headers = {
            "client-token": self.client_token,
            "Content-Type": "application/json"
        }
        data = {
            "phone": phone,
            "caption": caption,
            "image": image_url,
            "delayMessage": delay_message
        }

        return requests.post(url, headers=headers, json=data)

    def read_message(self, message_id, phone):
        url = f"https://api.z-api.io/instances/{self.instance_id}/token/{self.instance_token}/read-message"
        headers = {
            "client-token": self.client_token,
            "Content-Type": "application/json"
        }
        data = {
            "phone": phone,
            "message_id": message_id
        }

        return requests.post(url, headers=headers, json=data)

    def retrieve_chats(self):
        url = f"https://api.z-api.io/instances/{self.instance_id}/token/{self.instance_token}/chats"
        headers = {
            "client-token": self.client_token
        }
        return requests.get(url, headers=headers)

    def get_chat_metadata(self, phone):
        url = f"https://api.z-api.io/instances/{self.instance_id}/token/{self.instance_token}/chats/{phone}"
        headers = {
            "client-token": self.client_token
        }
        return requests.get(url, headers=headers)

def validar_cpf(cpf: str) -> bool:
    cpf = str(cpf)

    numbers = [int(digit) for digit in cpf if digit.isdigit()]
    
    if len(numbers) != 11 or len(set(numbers)) == 1:
        logging.error(f"O CPF {cpf} não possui 11 dígitos ou todos os dígitos são iguais.")
        return None

    sum_of_products = sum(a*b for a, b in zip(numbers[0:9], range(10, 1, -1)))
    expected_digit = (sum_of_products * 10 % 11) % 10
    if numbers[9] != expected_digit:
        logging.error(f"O primeiro dígito verificador do CPF {cpf} é inválido.")
        return None

    sum_of_products = sum(a*b for a, b in zip(numbers[0:10], range(11, 1, -1)))
    expected_digit = (sum_of_products * 10 % 11) % 10
    if numbers[10] != expected_digit:
        logging.error(f"O segundo dígito verificador do CPF {cpf} é inválido.")
        return None

    return cpf

def clean_column_names(column_names):
    cleaned_names = []
    for name in column_names:

        # Remove leading/trailing underscores
        cleaned_name = re.sub(r'^_+|_+$', '', cleaned_name)
        # Convert to lowercase
        cleaned_name = cleaned_name.lower()
        cleaned_names.append(cleaned_name)
    return cleaned_names

class TemplateMensagem:
    def __init__(self, nome_cliente, cpf):
        self.nome_cliente = nome_cliente
        self.cpf = cpf

    def loja_especifica(self, nome_loja, numero_pontos):
        if numero_pontos < 1000:
            raise ValueError("O número de pontos deve ser maior ou igual a 1000.")

        return (
        f"Olá, {self.nome_cliente}! Tudo bem?\n\n"
        f"Aqui é a Júlia, da *{nome_loja}*, e tenho uma notícia incrível:"
        f" você acumulou {self._formatar_numero_pontos(numero_pontos)} pontos no programa Sempre Leitura, que equivalem a *R${self._transformar_pontos_em_dinheiro(numero_pontos)}* de desconto na sua próxima compra em nossa loja! 🎉📚\n\n"
        "Com a Volta às Aulas chegando, é uma ótima oportunidade para garantir o material escolar de alguém especial! E, claro, você também pode aproveitar seus pontos para levar aquele livro que está de olho há um tempo!\n\n"
        f"Passe na *{nome_loja}*, onde temos tudo o que você precisa — desde materiais escolares até os melhores livros!\n\n"
        "Estamos super ansiosos para te receber e te ajudar no que precisar! 😊\n\n"
        f"*Os pontos estão atrelados ao CPF {self._hide_cpf()}, não podem ser transferidos e têm validade, hein! 😉 Quer saber mais? Dá uma olhada no regulamento lá no nosso site!" 
        )   

    def pontos_a_expirar(self, data_a_expirar_inicio, data_a_expirar_fim, pontos_a_expirar, numero_pontos):
        if numero_pontos < 1000:
            raise ValueError("O número de pontos deve ser maior ou igual a 1000.")
        
        # Se as datas forem iguais, não precisa do intervalo
        if pd.notna(data_a_expirar_fim) and data_a_expirar_inicio == data_a_expirar_fim:
            data_a_expirar_fim = pd.NA
        
        if pd.isna(data_a_expirar_fim):
            mensagem_data = f"no dia {self.format_date_to_text(data_a_expirar_inicio)}"
        else:
            mensagem_data = f"entre {self.format_date_to_text(data_a_expirar_inicio)} e {self.format_date_to_text(data_a_expirar_fim)}"

        return (
        f"Olá, {self.nome_cliente}! Tudo bem?\n\n"
        f"Aqui é a Júlia, do programa de pontos *Sempre Leitura*. Passando para te avisar que {self._formatar_numero_pontos(pontos_a_expirar)} dos seus pontos vão expirar {mensagem_data}! 📅\n\n"
        f"Que tal aproveitar essa oportunidade para garantir aquele livro dos sonhos ou qualquer outro produto que esteja na sua lista? No total, você tem {self._formatar_numero_pontos(numero_pontos)} pontos, que valem *R${self._transformar_pontos_em_dinheiro(numero_pontos)}* em crédito na *Livraria Leitura*! 💰📚\n\n"
        "Mas atenção: os pontos que expiram não voltam! Então não deixe para depois—vem garantir seu resgate enquanto dá tempo!\n\n"
        "Te esperamos na loja! Qualquer dúvida, é só me chamar. 😉\n\n"
        f"*Os pontos estão atrelados ao CPF {self._hide_cpf()} e não podem ser transferidos! Quer saber mais? Dá uma olhada no regulamento lá no nosso site!\n\n" 
        "PS: Não quer mais receber esses lembretes? Sem problema! É só responder SAIR que eu paro de te enviar mensagens! 😉\n\n"
        )
    
    def format_date_to_text(self, date):
        formatted_date = format_date(date, locale=locale, format='long')
        formatted_date_no_year = ' '.join(formatted_date.split()[:-2])
        return formatted_date_no_year

    def _transformar_pontos_em_dinheiro(self, numero_pontos):
        return int(np.floor(numero_pontos / 100))

    def _formatar_numero_pontos(self, numero_pontos):
        return "{:,}".format(int(np.floor(numero_pontos))).replace(",", ".")

    def _hide_cpf(self) -> str:
        cpf = str(self.cpf)
        return f"{cpf[:3]}.XXX.{cpf[6:9]}-XX"
