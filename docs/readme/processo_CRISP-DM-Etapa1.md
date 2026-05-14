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

Fica alinhado que a entrega consistirá em uma Auditoria Estratégica Retrospectiva referente a 24 meses de operação.  
O escopo contempla a construção de um pipeline de dados estruturado em arquitetura Medallion (camadas Bronze a Gold), a disponibilização de um painel focado em Customer Lifecycle Analysis (CLA) e a apresentação de insights direcionados ao planejamento macro e à saúde a longo prazo do negócio.