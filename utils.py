# -*- coding: utf-8 -*-
import random
from faker import Faker

fake = Faker('pt_BR')

def gerar_numero_cartao():
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

def gerar_transacao():
    return {
        "id_usuario": random.randint(1, 1000),
        "data_transacao": fake.date_time_this_year().isoformat(),
        "valor": round(random.uniform(10, 1000), 2),
        "estabelecimento": fake.company(),
        "categoria": random.choice(["Alimentação", "Transporte", "Entretenimento", "Saúde", "Educação"]),
        "status": random.choice(["Aprovada", "Negada", "Pendente"])
    }

def gerar_cliente():
    ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
    return {
        "id_usuario": random.randint(1, 1000),
        "nome": fake.name(),
        "email": fake.email(),
        "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "endereco": fake.address().replace('\n', ', '),
        "limite_credito": random.choice([1000, 2000, 5000, 10000, 20000]),
        "numero_cartao": gerar_numero_cartao(),
        "id_uf": random.choice(ufs)
    }

def gerar_dados(nome_tabela, num_records):
    if nome_tabela == 'transacoes_cartao':
        return [gerar_transacao() for _ in range(num_records)]
    elif nome_tabela == 'clientes':
        return [gerar_cliente() for _ in range(num_records)]
    else:
        raise ValueError(f"Tabela desconhecida: {nome_tabela}")
