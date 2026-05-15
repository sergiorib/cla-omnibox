# Etapas do processo de análise (CRISP-DM) 

## Etapa 2: Coleta e Entendimento dos Dados

### Nessa etapa o objetivo é analisar os dados através de:

- Acesso aos dados e documentações.
- Verificar se os dados cobrem as necessidades das análises
- Entendimento do formato e estrutura dos dados (entidades, campos, chaves, cardinalidade, etc.).
- Verificação da consistência e necessidade de tratamento dos dados.
- Mapeamento dos dados  necessários para atender os objetivos do projeto.

### 2.1 Documentação de coleta

Documentação do dicionário de dados e origens de coleta fornecidas pela equipe de engenharia de dados. 

### 🗎 [CLA_OMNIBOX_Doc_Engenharia_Dados_v2](../CLA_OMNIBOX_Doc_Engenharia_Dados_v2.pdf)

### 2.2 Definição de Requisitos de Dados

Elaboração de mapeamento conceitual dos dados necessários para as análises, com base nas documentações. 

Aqui é feita a ponte entre a documentação fornecida e as necessidades de dados para atender ás expectativas de análises. 

link:  omnibox_cla_mapa_requisitos_dados.pdf  **(gerar link)**

### 2.3 Coleta (ingestão)

Os dados disponibilizados são **multi-origem** e terão 3 formas de coleta diferentes implementadas em programas Python simples: 

1. **omnibox_cla_carga_vendas.py :** 
    
    *Busca os dados das tabelas de vendas no banco de dados PostgreSQL do ERP e salva em formato “parquet” no diretório especificado nas configurações do programa.*  
    
2. **omnibox_cla_carga_navegacao.py**
    
    Lê os dados nos arquivos CSV disponibilizados *e salva em formato “parquet” no diretório especificado nas configurações do programa.*  
    
3. **omnibox_cla_carga_maketing.py**
    
    *Chama métodos da API Rest do sistema de marketing  para buscar os dados e  salvar em formato “parquet” no diretório especificado nas configurações do programa.*
    

**Disponibilização da coleta**

Ao final de todas as ingestões os resultados estarão padronizados no formato ‘**parquet**’ (um para cada arquivo/tabela) e disponibilizados em um diretório único. 

Estes serão os dados brutos (padrão bronze) disponibilizados para tratamento e análise.

### 2.4 Descrição dos dados coletados

Aqui analiso os dados coletados (relacionamentos, PK,  FK, campos requeridos e etc.) , e monto um desenho do modelo de dados.  

Este modelo servirá de base para as análises exploratorias e a configuração dos tratamentos de dados que serão descritos no arquivo [cla_omnibox_schema_dados.json](../../config/cla_omnibox_schema_dados.json)

### 2.5 Análise de adequação e cobertura de dados

Nessa etapa verifico se os dados necessários para atender os objetivos do projeto estão disponíveis nos dados disponibilizados. 

Caso não estejam volto alguns passos para entender se houve algum gap na engenharia, carga ou documentações.

⚠️ **__Em caso de campos faltantes ou dados de baixa qualidade para responder a alguma pergunta, procuro confirmar se a coleta foi feita corretamente e se o “gap” persistir, volto para a Etapa 1 para renegociar o escopo com o cliente e fazer os adendos necessários no TAP (termo de abertura do projeto).__**

### 2.6 Análise exploratória dos dados (EDA)

#### 1. Visão Geral e Objetivo

Nesta etapa tem como objetivo realizar a exploração inicial dos dados, padronização de formatos e execução de análises diagnósticas na camada Bronze do Data Lake.

O processo contempla:

- Inspeção estrutural dos dados
- Validações iniciais de qualidade
- Preparação para etapas posteriores do pipeline (Data Quality)

A abordagem adotada combina processamento analítico em memória com exploração visual orientada a amostras.

O Notebook que implementa a exploração dos dados é o [cla_omnibox_exploracao.ipynb](../../src/cla_omnibox_exploracao.ipynb)

#### 2. Arquitetura e Stack Tecnológica

##### Motor de Processamento
- **DuckDB** (Processamento analítico colunar em memória)

##### Linguagens e Ferramentas
- **Python 3**
- **Pandas** (manipulação e visualização tabular)
- **Jupyter Notebook**

#### 3. Estrutura de Diretórios

O pipeline utiliza uma organização padronizada de diretórios:

- `raw/` → dados brutos
- `bronze/` → dados tratados iniciais
- `prata/` → dados validados (output esperado)
- `config/` → regras e schema (`cla_omnibox_schema_dados.json`)

#### 4. Fluxo de Execução

O notebook segue um fluxo estruturado em etapas sequenciais:

#### 4.1. Setup e Configuração Inicial

Responsável por preparar o ambiente de execução:

- Importação de bibliotecas
- Configuração de visualização (Pandas e Seaborn)
- Inicialização do DuckDB em memória
- Aplicação de limites de hardware
- Definição de caminhos de diretórios

#### 4.2. Carga do Schema (Metadados)

Leitura do arquivo mestre:

- `cla_omnibox_schema_dados.json`

Funções desta etapa:

- Carregar estrutura esperada das tabelas
- Identificar colunas e tipos esperados
- Definir base para validações posteriores

#### 4.3. Mapeamento Dinâmico de Dados (Bronze → DuckDB)

Criação automática de **views no DuckDB** a partir dos arquivos `.parquet`.

#### 4.4. Inspeção de Schema e Volumetria

Análise comparativa entre:
- Estrutura física (DuckDB)
- Estrutura lógica (JSON)
  
Validações realizadas:
- Contagem de registros por tabela
- Quantidade de colunas
- Comparação de colunas esperadas vs reais

Possíveis status: 
✅ OK → Estrutura consistente
⚠️ Divergente → Diferenças detectadas
❌ Pendente → Dados não encontrados

#### 4.5. Validação de Nulos e Campos Obrigatórios

Verificação de integridade básica:
- Campos obrigatórios (NOT NULL)
- Strings vazias
- Dados inconsistentes

*Objetivo* :
Identificar falhas críticas logo na exploração inicial

#### 4.6. Exploração e Análise dos Dados

Uso de:
Pandas → agregações e amostras
Tipos de análise:
- Distribuição de dados
- Outliers
- Padrões de comportamento
- Estatísticas descritivas
