# Preparação dos recursos necessários a demonstração

Será considerado que o **Environment** já foi criado e que o usuário tenha acesso de adminstração do ambiente.

## Habilitar o serviço do CDE (Cloudera Data Engineering)

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois no **Data Engineering**
2. Clicar em **Administration**, no menu da coluna à esquerda e na nova página, na coluna de Serviços, clicar no símbolo de mais (**+**)
   - Caso não exista nenhum serviço habilitado, o botão **Enable a Service** será apresentado. Clicar nesse botão para seguir.
3. Para essa demonstração iremos preencher apenas os campos obrigatórios:
   - **Name:** Pode ser o nome de uma área que utilizara: credito-env
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
   - **Name:** nome do seu cluster virtual: credito-vc
   - **Service:** selecionar o serviço habilitado do seu ambiente
   - **Spark Version Type:** Spark 3.5.1 (Security Hardened)
   **Tier Type and Settings**
   - **Select tier:** All-Purpose (Para podermos testar as Sessions)
   - **Session timeout:** 1 hour
   - **Manter o resto das configurações padrão**

Aguardar em torno de 1 hora para a criação do cluster virtual.

## Criação do Data Engineering Data Hub

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois na opção **Data Hub**
2. Clicar em **Create Data Hub**
3. Selecionar a opção do **Data Engineering Data Hub**
4. Iremos preencher apenas os campos obrigatórios:
   - **Name:** nome do seu data hub: credito-dedh
   - 

## Habilitar o serviço do CDW (Cloudera Data Warehouse)

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois na opção **Data Warehouse**
2. Clicar em **Create Data Hub**

[Retornar a página da demontração CDE](repositório-do-git.md)

[Retornar a página inicial](../README.md)