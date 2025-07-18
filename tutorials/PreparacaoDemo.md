# Preparação dos recursos necessários a demonstração

Será considerado que o **Environment** já foi criado e que o usuário tenha acesso de adminstração do ambiente.

## Habilitar o serviço do CDE (Cloudera Data Engineering)

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois no **Data Engineering**
2. Clicar em **Administration**, no menu da coluna à esquerda e na nova página, na coluna de Serviços, clicar no símbolo de mais (**+**)
   - Caso não exista nenhum serviço habilitado, o botão **Enable a Service** será apresentado. Clicar nesse botão para seguir.
3. Para essa demonstração iremos preencher apenas os campos obrigatórios:
   - **Name:** Pode ser o nome de uma área que utilizara: userXXX-env
   - **Environment:** Caixa de seleção para selecionar o seu ambiente criado.
   - **Workload Type:** MemoryOptimized - r5.4xlarge
   - **Selecionar** Enable Public Load Balancer (Para acesso público)
   - **Manter o resto das configurações padrão**
   - Caso queira, adicionar tags para facilitar a administração financeira dos recursos
   - Desabilitar a criação do **Default Virtual Cluster**

Aguardar em torno de 1 hora para a habilitar o serviço.

## Criação do CDE Virtual Cluster

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois no **Data Engineering**
2. Clicar em **Administration**, no menu da coluna à esquerda e na nova página, na coluna de Serviços, selecionar o serviço habilitado do seu ambiente
3. Depois na coluna a esquerda, Virtual Clusters, clicar no símbolo de mais (**+**)
4. Iremos preencher apenas os campos obrigatórios:
   **Virtual Cluster Details**
   - **Name:** nome do seu cluster virtual: userXXX-vc
   - **Service:** selecionar o serviço habilitado do seu ambiente
   - **Spark Version Type:** Spark 3.5.1 (Security Hardened)
   **Tier Type and Settings**
   - **Select tier:** All-Purpose (Para podermos testar as Sessions)
   - **Session timeout:** 1 hour
   - **Manter o resto das configurações padrão**

Aguardar em torno de 1 hora para a criação do cluster virtual.

## Habilitar o serviço do CDW (Cloudera Data Warehouse)

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois na opção **Data Warehouse**
2. Clicar na aba **Environments**, encontre o seu ambiente criado e clique em **Activate**
3. **Deployment Mode:** Public Load Balancer, Public Executors (Somente Desenvolvimento)
4. **ACTIVATE**

## Criação do CDW Virtual Warehouse

1. Com o serviço ativo e em **Good Health**, clicar na aba Virtual Warehouses e depois em **New Virtual Warehouse**
2. **Name:** userXXX-vw
3. **Type:** Hive
4. **Environments:** Ambiente criado anteriormente
5. **Database Catalog:** Ambiente_criado-default
6. **Compute Instance Types** r5d.4xlarge
7. **Hive Image Version** 2025.0.19.2-5 (Escolha conforme necessidade)
8. **Hue Image Version** 2025.0.19.2-5 (Escolha conforme necessidade)
9. **Size** xsmall - 2 Executors
10. **Manter o resto das configurações padrão**
11. **Create Virtual Warehouse**

## Criação do repositório local com git

Para facilitar as explicações, iremos adotar como editor o [Microsoft Visual Code](https://code.visualstudio.com/)

1. Abrir o editor MS-Visual Code
2. Acessar o site https://github.com/jcaseir0/sebrdemos, clicar no botão **<> Code** e copiar o link do HTTPS

[Retornar a página da demontração CDE](repositório-do-git.md)

[Retornar a página inicial](../README.md)