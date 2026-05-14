# Etapas do processo de análise (CRISP-DM) 

## Etapa 1: Entendimento do Negócio

### 📋**Levantamentos** 

#### Informações levantados em e-mails, chats e reuniões de entendimento do negócio com a área solicitante e/ou demais áreas relacionadas.

- **Pontos chave para o sucesso da entrega.**

Ter certeza de que os dados do ERP e os Logs de Navegação estão "falando a mesma língua”.  
Saber qual canal de marketing (Google, Meta, etc.) traz o cliente que se paga mais rápido. 
Lista clara de "Clientes em Risco" para passarmos ao time de CRM. 
Dashboard deve mostrar claramente onde está o gargalo do funil para priorizarmos o investimento

- **Detalhes do negócio, termos técnicos, indicadores e fórmulas de cálculo.**

  - Consideramos como "venda" apenas pedidos com status **"Faturado"** (ignorar cancelados/boletos expirados).
  - **Churn:** Definimos como um cliente que não realiza uma nova compra há mais de **90 dias**.
  -**LTV (Lifetime Value):** Soma da receita líquida gerada pelo cliente num período histórico de **12 meses**.
  - **CAC (Custo de Aquisição):** Total gasto em anúncios (Ads) / Total de novos clientes (Primeira compra).
  - **RFM:** Queremos uma pontuação de **1 a 5** para cada pilar. O "Valor Monetário" deve ser baseado no Ticket Médio acumulado.
  - **Etapas do Funil:** Visualização de Produto → Adição ao Carrinho → Inicio do checkout → Compra finalizada.

- **Quais perguntas o solicitante espera que os dados respondam.**

    - **Eficiência de Marketing:** "Para cada R$ 1,00 investido no Canal X, quanto de lucro (não apenas receita) retorna em 6 meses?"

    - **Gargalo de Conversão:** "Onde está a maior 'fuga' de dinheiro: no carregamento da página, na visualização do produto ou no checkout de pagamento?"

    - **Saúde da Base:** "Nossos clientes antigos estão comprando mais ou estamos sobrevivendo apenas de 'sangue novo' (novos clientes)?"

    - **Segmentação de Ouro:** "Quem são os 10% de clientes que geram 50% do nosso faturamento e quais produtos eles costumam comprar?"

    - **Previsão de Risco:** "Quais comportamentos de navegação indicam que um cliente fiel está prestes a se tornar Churn (parar de comprar)?" ****

- **Fontes importantes para entendimento dos dados (pessoas,  documentos, sites, etc.).**
- **Alinhamentos sobre critérios de aceite e forma de entrega. (DoD)**

### 🖋️ Formalização da entrega

__Fica alinhado que a entrega consistirá em uma Auditoria Estratégica Retrospectiva referente a 24 meses de operação.  
O escopo contempla a construção de um pipeline de dados estruturado em arquitetura Medallion (camadas Bronze a Gold), a disponibilização de um painel focado em Customer Lifecycle Analysis (CLA) e a apresentação de insights direcionados ao planejamento macro e à saúde a longo prazo do negócio. __ 

## Etapa 2: Coleta e Entendimento dos Dados

### Nessa etapa o objetivo é analisar os dados através de:

- Acesso aos dados e documentações.
- Verificar se os dados cobrem as necessidades das análises
- Entendimento do formato e estrutura dos dados (entidades, campos, chaves, cardinalidade, etc.).
- Verificação da consistência e necessidade de tratamento dos dados.
- Mapeamento dos dados  necessários para atender os objetivos do projeto.

### 2.1 Documentação de coleta

Documentação do dicionário de dados e origens de coleta fornecidas pela equipe de engenharia de dados. 

### 🗎 [CLA_OMNIBOX_Doc_Engenharia_Dados_v2](./docs/CLA_OMNIBOX_Doc_Engenharia_Dados_v2.pdf)

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

Este modelo servirá de base para as análises exploratorias e a configuração dos tratamentos de dados que serão descritos no arquivo [cla_omnibox_schema_dados.json](./config/cla_omnibox_schema_dados.json)

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

O Notebook que implementa a exploração dos dados é o [cla_omnibox_exploracao.ipynb](./src/cla_omnibox_exploracao.ipynb)

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

## Etapa 3: Preparação dos dados

### 1. Visão Geral e Arquitetura

Esta etapa é onde faço o tratamento e roteamento dos dados na transição da camada Bronze (dados brutos/brutos particionados) para a camada Prata (dados confiáveis e higienizados).

A arquitetura de validação foi desenhada de maneira orientada a metadados, o que significa que o código Python não possui regras "chumbadas". Em vez disso, o motor lê as regras de negócio e restrições a partir de um arquivo mestre de configuração chamado  
[cla_omnibox_schema_dados.json](./config/cla_omnibox_schema_dados.json), garantindo alta escalabilidade e fácil manutenção.

O programa que implementa os tratamentos é o [cla_omnibox_tratamento.py](./src/cla_omnibox_tratamento.py)

#### Especificações Técnicas do Motor

- **Motor de Processamento:** DuckDB (Processamento Analítico Colunar em Memória)  
- **Otimização de Hardware:** Limite de 6GB de RAM e processamento distribuído em 8 threads para garantir estabilidade operacional  
- **Linguagem Orquestradora:** Python 3  


### 2. Fluxo de Execução e Mapeamento
O pipeline opera sob um fluxo contínuo e automatizado que identifica a tipologia da fonte de dados antes de iniciar o processamento:

- **Mapeamento Dinâmico:** O script varre o diretório Bronze e identifica automaticamente se a fonte de dados é um arquivo único (ex: `orders.parquet`) ou um diretório particionado (ex: `google_analytics/*.parquet`)  
- **Criação de Views:** Para otimizar a leitura e não sobrecarregar a memória, são criadas *views* virtuais no DuckDB que apontam diretamente para os arquivos físicos  


### 3. Matriz de Validações Aplicadas

Antes de um registro ser promovido para a camada Prata, ele é submetido a uma rigorosa esteira de auditoria dividida em quatro frentes estruturais:

#### 3.1. Validações Estruturais e de Formato

- **Schema e Volumetria:** Comparação estrita entre a estrutura física mapeada pelo DuckDB e as colunas esperadas pelo JSON Mestre (detectando colunas faltantes ou excedentes)  
- **Tipagem, Nulos e Brancos:** Garantia de que campos obrigatórios (NOT NULL) não contenham valores nulos ou strings vazias, além da verificação de compatibilidade de tipos (Inteiros, Datas, Strings, etc.)  
- **Regex e Qualidade Numérica:** Validação de padrões de string via Expressões Regulares e bloqueio de anomalias numéricas (ex: preços negativos ou zerados indevidamente)  
- **Domínio de Dados:** Validação contra uma lista estática de categorias aceitas (evitando erros de digitação sistêmica)  

#### 3.2. Validações de Integridade Relacional

- **Chaves Primárias (PK):** Teste de unicidade para chaves primárias simples e compostas, garantindo a ausência de registros duplicados absolutos  
- **Chaves Estrangeiras (FK):** Verificação de integridade referencial para garantir que não existam registros "órfãos" (ex: um pedido atrelado a um cliente que não existe na tabela `customers`)  

#### 3.3. Validações de Regras de Negócio Customizadas

- **Consistência Financeira:** Aplicação de lógicas em SQL para validações complexas. O pipeline realiza a conciliação automática cruzando o valor total cobrado no cabeçalho do pedido (`gross_amount` na tabela `orders`) com a soma bruta da quantidade multiplicada pelo preço unitário dos itens (`price * quantity` na tabela `order_items`), considerando os descontos aplicados  

### 4. Roteamento de Dados e Tolerância a Falhas

O pipeline implementa o padrão arquitetural de **Dead Letter Queue (DLQ)** e **Circuit Breaker** para garantir a confiabilidade do Data Lake:

- **Promoção (Camada Prata):** Registros que passam em 100% das regras de validação são consolidados e gravados no diretório da camada Prata  
- **Quarentena (DLQ):** Registros que violam qualquer regra são isolados e gravados em um diretório de Quarentena (`_dlq.parquet`), impedindo a contaminação da base analítica principal, mas preservando o dado para futura auditoria e correção  
- **Circuit Breaker (Proteção Sistêmica):** O arquivo de configuração mestre define uma taxa de tolerância a erros (em percentual) para cada tabela. Se a proporção de registros defeituosos em relação ao volume lido superar essa tolerância, o pipeline aciona um alarme de falha crítica e interrompe o processamento, impedindo que uma carga massivamente corrompida seja processada  

## Etapa 4: Modelagem dimensional

O módulo de transformação ([cla_omnibox_transformacao.py](./src/cla_omnibox_transformacao.py)) é o motor analítico do projeto.  
Ele é responsável por consumir os dados higienizados da **Camada Prata** e aplicar regras complexas de negócio para gerar a **Camada Ouro** (Modelagem Dimensional e Agregações Analíticas), que alimenta diretamente os dashboards no Power BI.

#### 🛠️ Stack Tecnológico e Otimizações
* **Motor de Processamento:** DuckDB executando consultas analíticas em memória.
* **Formato de Armazenamento:** Apache Parquet, garantindo alta compressão e leitura colunar ultra-rápida.
* **Controle de Recursos:** O limite de memória do DuckDB é explicitamente travado em `4GB` (`PRAGMA memory_limit='4GB'`) para garantir estabilidade operacional em ambientes com recursos restritos.

### 🔄 Fluxo de Processamento e Dependências (DAG)

A execução das rotinas obedece a uma ordem de dependência estrita, pois diversas tabelas da Camada Ouro são utilizadas como insumo (`JOINs` e `CTEs`) para a construção de modelos subsequentes. 

O orquestrador processa os dados na seguinte sequência:

1. **Dimensões Base:** Calendário, Produtos, Marketing Ações.
2. **Dimensão Avançada:** Clientes (depende do cruzamento com web analytics e campanhas).
3. **Tabelas Fato:** Vendas e Marketing (dependem das dimensões base e clientes).
4. **Modelos Analíticos (Agregações):** RFM Snapshot, Estatísticas Mensais, Navegação e Estatísticas de Marketing (dependem das Fatos geradas).

### 📊 Modelagem de Dados e Regras de Negócio

Abaixo estão detalhados os modelos gerados pelo pipeline:

#### 1. Dimensões (Tabelas de Busca)
As dimensões fornecem o contexto necessário para as métricas, permitindo filtros e segmentações detalhadas.

| Tabela | Descrição e Regras Aplicadas |
| :--- | :--- |
| **Calendário** | Tabela de referência temporal contendo atributos de data, ano, mês e chaves compostas para suporte a filtros cronológicos e cálculos de inteligência de tempo. |
| **Produtos** | Dimensão desnormalizada que consolida informações de categoria, subcategoria e o nome do produto em uma única estrutura, facilitando a navegação e hierarquia no dashboard. |
| **Clientes** | Base enriquecida que identifica o canal de origem (tráfego), a ação de marketing vinculada e a data da primeira compra, permitindo o rastreamento do ciclo de vida desde a aquisição. |
| **RFM Legenda** | Tabela auxiliar que armazena a classificação dos segmentos (ex: Campeões, Em Risco), descrições de comportamento e recomendações acionáveis para cada perfil. |
| **Marketing Ações** | Consolida as características das campanhas, incluindo objetivos, canais de mídia utilizados e os períodos de vigência das ações promocionais. |

---

#### 2. Fatos (Tabelas Transacionais)
Tabelas que registram os eventos quantitativos da operação em seu menor nível de detalhe.

| Tabela | Descrição e Regras Aplicadas |
| :--- | :--- |
| **Vendas** | Fato consolidada a nível de item, contendo o status de venda para filtragem de pedidos concluídos. Calcula métricas financeiras críticas como receita líquida, lucro, rateios de frete e o tempo decorrido desde a primeira compra. |

---

#### 3. Agregações e Snapshots (Modelos Analíticos)
Modelos pré-processados e otimizados para garantir rapidez nas visualizações e nos cálculos complexos.

| Tabela | Descrição e Regras Aplicadas |
| :--- | :--- |
| **RFM Snapshot** | Snapshot histórico mensal que armazena os scores individuais de Recência, Frequência e Valor, além da classificação final do cliente baseada no tempo de inatividade. |
| **Estatísticas Mensais** | Tabela de alta performance que agrega volumetria de clientes (novos, ativos e recorrentes) cruzada com faturamento e lucro total por referência mensal. |
| **Estatísticas Marketing** | Modelo focado em ROI que correlaciona faturamento e lucro com o investimento (gasto) por campanha, calculando automaticamente o payback em dias e meses. |
| **Navegação** | Agrega o comportamento do usuário no funil de vendas, registrando a etapa alcançada, a quantidade de sessões únicas e a taxa de abandono em cada ponto do fluxo. |

### 🚦 Orquestração e Telemetria

O pipeline é encapsulado na função `executar_transformacao_ouro()`, que atua como um orquestrador resiliente:
* Cada etapa é cronometrada individualmente.
* Falhas em uma tabela específica são capturadas via bloco `try-except`, evitando a quebra abrupta de todo o pipeline.
* Ao final da execução, um log estruturado em formato JSON é retornado e impresso, contendo o status geral, contagem de tabelas processadas e o tempo gasto (em segundos) por rotina.


## Etapa 5: Construção e validação

### Design e Interface (UI/UX)

O projeto **Omnibox Cosméticos** foi desenvolvido com foco em uma experiência de usuário (UX) intuitiva e uma interface (UI) moderna, utilizando o conceito de **Neumorfismo (Soft UI)**.

#### Conceito Visual
* **Estilo Neumórfico:** Utilização de sombras suaves e camadas para criar profundidade, dando aos elementos uma aparência de "baixo-relevo" que se integra organicamente ao fundo.
* **Paleta de Cores:** Baseada em tons de roxo profundo e azul marinho, transmitindo sofisticação e elegância (alinhado ao nicho de cosméticos), com acentos em dourado e creme para facilitar a leitura e destacar KPIs críticos.
* **Tipografia e Hierarquia:** Uso de fontes sem serifa e contrastes de peso para guiar o olhar do tomador de decisão, priorizando métricas de variação percentual (Δ Ano anterior).
  
*Ferramenta utilizada:*  
Para desenho do visual 'premium' das telas do dashboard foi utilizada a ferramenta gratuita FIGMA.

#### Arquitetura de Informação
O dashboard está estruturado em quatro visões estratégicas que permitem uma análise completa do funil de vendas e saúde do cliente:

1.  **Visão Executiva (Executive Summary):** Concentra os KPIs de alto nível (Faturamento, ROAS, Churn, CAC). Ideal para uma leitura rápida da saúde financeira e operacional.
2.  **Marketing & Performance:** Focado na alocação de recursos por canal (Google, Facebook, Instagram, TikTok). Inclui métricas de eficiência como Payback e Retorno por Real Investido.
3.  **Retenção & CRM:** Utiliza segmentação RFM (Recência, Frequência e Valor) para classificar a base de clientes em perfis (Fiéis, Em Risco, Hibernando), permitindo estratégias de reativação direcionadas.
4.  **Funil de Conversão:** Analisa o comportamento do usuário desde a sessão inicial até o checkout final, identificando gargalos técnicos e de experiência de compra por dispositivo e região.

### Medidas

AS medidas utilizadas no dashboard estão no documento [CLA_OMNIBOX_Doc_Analise_Medidas_DAX.pdf](./docs/CLA_OMNIBOX_Doc_Analise_Medidas_DAX.pdf)

## Etapa 6: Publicação 

#### 📊 [Publicação do dashboard (Power BI Service)](https://app.powerbi.com/view?r=eyJrIjoiNGJhMGJjNWYtM2U1YS00NWJlLTkyZmYtYzQ5YjAwODg4MjY2IiwidCI6IjNmZDRlZDcxLWNmMDUtNDJmMS05Y2ZjLWQyNGI5ZGFjZjA3MyJ9)

