# Etapas do processo de análise (CRISP-DM) 

## Etapa 4: Modelagem dimensional

O módulo de transformação ([cla_omnibox_transformacao.py](../../src/cla_omnibox_transformacao.py)) é o motor analítico do projeto.  
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

#### 4.1. Dimensões (Tabelas de Busca)
As dimensões fornecem o contexto necessário para as métricas, permitindo filtros e segmentações detalhadas.

| Tabela | Descrição e Regras Aplicadas |
| :--- | :--- |
| **Calendário** | Tabela de referência temporal contendo atributos de data, ano, mês e chaves compostas para suporte a filtros cronológicos e cálculos de inteligência de tempo. |
| **Produtos** | Dimensão desnormalizada que consolida informações de categoria, subcategoria e o nome do produto em uma única estrutura, facilitando a navegação e hierarquia no dashboard. |
| **Clientes** | Base enriquecida que identifica o canal de origem (tráfego), a ação de marketing vinculada e a data da primeira compra, permitindo o rastreamento do ciclo de vida desde a aquisição. |
| **RFM Legenda** | Tabela auxiliar que armazena a classificação dos segmentos (ex: Campeões, Em Risco), descrições de comportamento e recomendações acionáveis para cada perfil. |
| **Marketing Ações** | Consolida as características das campanhas, incluindo objetivos, canais de mídia utilizados e os períodos de vigência das ações promocionais. |

---

#### 4.2. Fatos (Tabelas Transacionais)
Tabelas que registram os eventos quantitativos da operação em seu menor nível de detalhe.

| Tabela | Descrição e Regras Aplicadas |
| :--- | :--- |
| **Vendas** | Fato consolidada a nível de item, contendo o status de venda para filtragem de pedidos concluídos. Calcula métricas financeiras críticas como receita líquida, lucro, rateios de frete e o tempo decorrido desde a primeira compra. |

---

#### 4.3. Agregações e Snapshots (Modelos Analíticos)
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

