# Etapas do processo de análise (CRISP-DM) 

## Etapa 3: Preparação dos dados

### 3.1. Visão Geral e Arquitetura

Esta etapa é onde faço o tratamento e roteamento dos dados na transição da camada Bronze (dados brutos/brutos particionados) para a camada Prata (dados confiáveis e higienizados).

A arquitetura de validação foi desenhada de maneira orientada a metadados, o que significa que o código Python não possui regras "chumbadas". Em vez disso, o motor lê as regras de negócio e restrições a partir de um arquivo mestre de configuração chamado  
[cla_omnibox_schema_dados.json](../../config/cla_omnibox_schema_dados.json), garantindo alta escalabilidade e fácil manutenção.

O programa que implementa os tratamentos é o [cla_omnibox_tratamento.py](../../src/cla_omnibox_tratamento.py)

#### Especificações Técnicas do Motor

- **Motor de Processamento:** DuckDB (Processamento Analítico Colunar em Memória)  
- **Otimização de Hardware:** Limite de 6GB de RAM e processamento distribuído em 8 threads para garantir estabilidade operacional  
- **Linguagem Orquestradora:** Python 3  


### 3.2. Fluxo de Execução e Mapeamento
O pipeline opera sob um fluxo contínuo e automatizado que identifica a tipologia da fonte de dados antes de iniciar o processamento:

- **Mapeamento Dinâmico:** O script varre o diretório Bronze e identifica automaticamente se a fonte de dados é um arquivo único (ex: `orders.parquet`) ou um diretório particionado (ex: `google_analytics/*.parquet`)  
- **Criação de Views:** Para otimizar a leitura e não sobrecarregar a memória, são criadas *views* virtuais no DuckDB que apontam diretamente para os arquivos físicos  


### 3.3. Matriz de Validações Aplicadas

Antes de um registro ser promovido para a camada Prata, ele é submetido a uma rigorosa esteira de auditoria dividida em quatro frentes estruturais:

#### 3.3.1. Validações Estruturais e de Formato

- **Schema e Volumetria:** Comparação estrita entre a estrutura física mapeada pelo DuckDB e as colunas esperadas pelo JSON Mestre (detectando colunas faltantes ou excedentes)  
- **Tipagem, Nulos e Brancos:** Garantia de que campos obrigatórios (NOT NULL) não contenham valores nulos ou strings vazias, além da verificação de compatibilidade de tipos (Inteiros, Datas, Strings, etc.)  
- **Regex e Qualidade Numérica:** Validação de padrões de string via Expressões Regulares e bloqueio de anomalias numéricas (ex: preços negativos ou zerados indevidamente)  
- **Domínio de Dados:** Validação contra uma lista estática de categorias aceitas (evitando erros de digitação sistêmica)  

#### 3.3.2. Validações de Integridade Relacional

- **Chaves Primárias (PK):** Teste de unicidade para chaves primárias simples e compostas, garantindo a ausência de registros duplicados absolutos  
- **Chaves Estrangeiras (FK):** Verificação de integridade referencial para garantir que não existam registros "órfãos" (ex: um pedido atrelado a um cliente que não existe na tabela `customers`)  

#### 3.3.3. Validações de Regras de Negócio Customizadas

- **Consistência Financeira:** Aplicação de lógicas em SQL para validações complexas. O pipeline realiza a conciliação automática cruzando o valor total cobrado no cabeçalho do pedido (`gross_amount` na tabela `orders`) com a soma bruta da quantidade multiplicada pelo preço unitário dos itens (`price * quantity` na tabela `order_items`), considerando os descontos aplicados  

### 3.4. Roteamento de Dados e Tolerância a Falhas

O pipeline implementa o padrão arquitetural de **Dead Letter Queue (DLQ)** e **Circuit Breaker** para garantir a confiabilidade do Data Lake:

- **Promoção (Camada Prata):** Registros que passam em 100% das regras de validação são consolidados e gravados no diretório da camada Prata  
- **Quarentena (DLQ):** Registros que violam qualquer regra são isolados e gravados em um diretório de Quarentena (`_dlq.parquet`), impedindo a contaminação da base analítica principal, mas preservando o dado para futura auditoria e correção  
- **Circuit Breaker (Proteção Sistêmica):** O arquivo de configuração mestre define uma taxa de tolerância a erros (em percentual) para cada tabela. Se a proporção de registros defeituosos em relação ao volume lido superar essa tolerância, o pipeline aciona um alarme de falha crítica e interrompe o processamento, impedindo que uma carga massivamente corrompida seja processada  
