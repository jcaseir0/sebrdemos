# Migração de Tabelas Hive Parquet para Iceberg em um Grande Banco Financeiro

## Introdução

Imagine um **grande banco financeiro** com operações em todo o país, onde o departamento analítico armazena e processa um **vasto volume de dados** em tabelas Hive no formato Parquet. Com o crescimento das demandas por governança, performance e flexibilidade, surge a necessidade de migrar essas tabelas para o formato **Apache Iceberg**, aproveitando recursos avançados de gerenciamento de dados e integração com plataformas modernas como o **Cloudera Data Engineering (CDE)**. Este documento apresenta o contexto, fundamentos do Iceberg, melhores práticas de migração e uma demonstração prática para implantação no CDE.

---

## O Formato de Tabela Iceberg

**Apache Iceberg** é um formato de tabela open source projetado para grandes volumes de dados analíticos em data lakes. Suas principais funcionalidades incluem:

- **Esquema evolutivo**: Permite adicionar, remover e renomear colunas sem recriar a tabela.
- **Particionamento flexível**: Alteração de estratégias de particionamento sem reescrever todos os dados.
- **Transações ACID**: Suporte a operações atômicas, evitando inconsistências em ambientes concorrentes.
- **Time travel**: Consulta a versões históricas dos dados para auditoria e recuperação.
- **Performance otimizada**: Gerenciamento eficiente de metadados e leitura seletiva de arquivos.

Essas características tornam o Iceberg ideal para ambientes de dados modernos, promovendo governança, escalabilidade e integração com engines como Spark, Flink e Trino.

---

## Migração de Parquet/Hive para Iceberg – Melhores Práticas

A migração de tabelas Hive Parquet para Iceberg pode ser realizada de duas formas principais: **in-place** ou **shadow**. A escolha depende de fatores como downtime aceitável, volume de dados e requisitos de auditoria.

### Opções de Migração

<table style="border-collapse: collapse;" alignment="center">
  <tr>
    <td align="center"><b>Estratégia</b></td>
    <td align="center"><b>Vantagens</b></td>
    <td align="center"><b>Desvantagens</b></td>
  </tr>
  <tr>
    <td align="center" rowspan="3"><b>In-place</b></td>
    <td align="center">Sem duplicação de dados</td>
    <td align="center">Downtime potencial</td>
    <tr>
      <td align="center">Menor uso de storage</td>
      <td align="center">Risco de falhas durante a conversão</td>
    </tr>
    <tr>
      <td align="center">Menor complexidade operacional</td>
      <td align="center">Sem rollback fácil</td>
    </tr>
  </tr>
  <tr>
    <td align="center" rowspan="3"><b>Shadow</b></td>
    <td align="center">Zero downtime</td>
    <td align="center">Duplicação temporária de dados</td>
    <tr>
      <td align="center">Permite testes e validação</td>
      <td align="center">Uso extra de storage</td>
    </tr>
    <tr>
      <td align="center">Rollback simples</td>
      <td align="center">Processo mais longo</td>
    </tr>
  </tr>
</table>

#### Diferenças principais

- **In-place**: Converte a tabela existente diretamente para Iceberg, substituindo o formato Parquet. Ideal para ambientes controlados onde downtime é aceitável.
- **Shadow**: Cria uma nova tabela Iceberg a partir dos dados Parquet, mantendo ambas as versões até a validação completa. Recomendado para ambientes críticos, pois garante continuidade operacional e rollback rápido.

**Recomendação**: Para bancos de grande porte, a abordagem **shadow** é geralmente mais segura, permitindo auditoria, validação e mitigação de riscos durante a transição.

---

## Cloudera Data Engineering (CDE)

O **Cloudera Data Engineering (CDE)** é uma plataforma gerenciada para orquestração e execução de pipelines Spark, facilitando a automação de cargas de trabalho de dados em escala empresarial. Com o CDE, é possível:

- Agendar e monitorar jobs Spark.
- Gerenciar dependências e variáveis de ambiente.
- Integrar com sistemas de storage e metastore Hive/Iceberg.
- Garantir segurança e governança corporativa.

### Roteiros de demonstração

#### Cloudera Data Engineering

- Passo-a-passo para implementar o laboratório com CDE: [Tutorial](tutorials/repositório-do-git.md)

#### Cloudera Data Warehouse e Data Hub

- Avaliação das funcionalidades e migração do Iceberg no Hive: [Tutorial](tutorials/ComandosHQLIcebergHive.md)
- Avaliação das funcionalidades e migração do Iceberg no Impala: [Tutorial](tutorials/ComandosSQLIcebergImpala.md)

---