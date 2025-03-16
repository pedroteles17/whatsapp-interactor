import numpy as np
import pandas as pd
import logging
import base64
import dotenv
import os
import requests
import locale
from datetime import datetime

locale.setlocale(locale.LC_TIME, "pt_BR.utf8")
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

def whatsapp_link(cellphone):
    if len(cellphone) not in [10, 11]:
        logging.error(f"O número {cellphone} deve ter 11 ou 10 dígitos, mas tem {len(cellphone)}.")
        return None

    if not cellphone.isnumeric():
        logging.error(f"O número {cellphone} deve conter apenas dígitos.")
        return None

    return f"https://wa.me/55{cellphone}"

class TemplateMensagem:
    def __init__(self, nome_cliente, cpf):
        self.nome_cliente = nome_cliente
        self.cpf = cpf

    def loja_especifica(nome_loja, numero_pontos):
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

    def pontos_a_expirar(self, data_a_expirar, pontos_a_expirar, numero_pontos):
        if numero_pontos < 1000:
            raise ValueError("O número de pontos deve ser maior ou igual a 1000.")

        return (
        f"Olá, {self.nome_cliente}! Tudo bem?\n\n"
        f"Aqui é a Júlia, do programa de pontos *Sempre Leitura*. Passando para te avisar que {self._formatar_numero_pontos(pontos_a_expirar)} dos seus pontos vão expirar a partir de {data_a_expirar.strftime('%d de %B')}! 📅\n\n"
        f"Que tal aproveitar essa oportunidade para garantir aquele livro dos sonhos ou qualquer outro produto que esteja na sua lista? No total, você tem {self._formatar_numero_pontos(numero_pontos)} pontos, que valem *R${self._transformar_pontos_em_dinheiro(numero_pontos)}* em crédito na *Livraria Leitura*! 💰📚\n\n"
        "Mas atenção: os pontos que expiram não voltam! Então não deixe para depois—vem garantir seu resgate enquanto dá tempo!\n\n"
        "Te esperamos na loja! Qualquer dúvida, é só me chamar. 😉\n\n"
        f"*Os pontos estão atrelados ao CPF {self._hide_cpf()} e não podem ser transferidos! Quer saber mais? Dá uma olhada no regulamento lá no nosso site!" 
        )

    def _transformar_pontos_em_dinheiro(self, numero_pontos):
        return int(np.floor(numero_pontos / 100))

    def _formatar_numero_pontos(self, numero_pontos):
        return "{:,}".format(int(np.floor(numero_pontos))).replace(",", ".")

    def _hide_cpf(self) -> str:
        cpf = str(self.cpf)
        return f"{cpf[:3]}.XXX.{cpf[6:9]}-XX"
