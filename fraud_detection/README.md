# Detecção de Fraude Financeira com Neo4j

Sistema de detecção de lavagem de dinheiro baseado em grafos, desenvolvido com Neo4j e Python como projeto acadêmico da graduação.

## Motivação

Bancos de dados relacionais têm dificuldade em detectar padrões cíclicos de transações financeiras de forma eficiente. Identificar um ciclo de profundidade N exige N JOINs explícitos, e a consulta precisa ser reescrita para cada tamanho de ciclo. Bancos de dados em grafos tratam esse problema de forma nativa: um ciclo é simplesmente um caminho que começa e termina no mesmo nó, e o Neo4j foi projetado exatamente para encontrar esse padrão.

## O Problema

Um dos esquemas mais comuns de lavagem de dinheiro é o Smurfing. O dinheiro parte de uma conta de origem, percorre diversas contas intermediárias chamadas de laranjas e retorna à conta original, formando um ciclo. A fragmentação do valor e o uso de intermediários dificultam o rastreamento, mas o retorno à origem é o sinal estrutural que torna o esquema detectável.

## A Solução

O sistema modela clientes, contas bancárias e transações como um grafo no Neo4j. Cada transferência é representada como um nó intermediário conectando duas contas, padrão conhecido como relacionamento reificado. Essa decisão permite navegar o caminho de conta em conta, identificando quando ele retorna à origem e caracterizando um ciclo de fraude.

## Arquitetura

O sistema é dividido em quatro módulos com responsabilidades bem definidas e fluxo estritamente unidirecional.

- `data_generator.py` — gera o dataset sintético com clientes, contas e transações, incluindo os ciclos de fraude
- `neo4j_pipeline.py` — persiste os dados no Neo4j com ingestão em batch e garantias ACID
- `fraud_detector.py` — executa as consultas de detecção e gera o relatório
- `main.py` — orquestra os módulos em sequência

## Detecção

Quatro consultas Cypher compõem o sistema de detecção:

1. Identifica ciclos rotulados e consolida volume total e janela temporal de cada um
2. Detecta ciclos estruturalmente, sem depender de rótulos, usando pattern matching quantificado
3. Ranqueia contas suspeitas por volume movimentado em operações fraudulentas
4. Classifica cada ciclo por nível de risco: CRÍTICO, ALTO ou MÉDIO

## Tecnologias

- Python 3.11+
- Neo4j 5.x
- Docker