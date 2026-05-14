# Contextualização da solicitação e planejamento do projeto

Solicitação da área usuária e planejamento prévio dos trabalhos de análise.

## 👨‍💼 Solicitação 

**De:** Ricardo Souza – Diretor de E-commerce (OmniBox S.A.)  
**Para:** Equipe de Análise de Dados  
**Assunto:** Urgente: Diagnóstico de Eficiência de Vendas e Retenção (Projeto CLA)

Olá,
Nossa operação de e-commerce cresceu 25% no último ano, **mas o lucro líquido não acompanhou esse ritmo**.
Tenho a sensação de que estamos gastando muito para atrair **clientes que compram apenas uma vez** e nunca mais voltam.

Além disso, nossa equipe de marketing não consegue dizer com clareza **qual canal traz o cliente mais valioso a longo prazo**.

Hoje, nossos **dados estão espalhados**: as vendas estão no ERP (PostgreSQL), o comportamento de navegação em arquivos de log e o investimento em marketing em planilhas à parte.

Para o planejamento do próximo semestre, preciso de um diagnóstico profundo do nosso Ciclo de Vida do Cliente (CLA).

Especificamente, preciso que respondam:

**Saúde do Funil**: Onde estamos perdendo mais usuários? Desde a navegação até o checkout, **qual o nosso maior gargalo?**

**Qualidade da Aquisição:** Quanto estamos pagando por cliente (CAC) e em quanto tempo esse investimento se paga?

**Fidelização e Churn:** Qual o comportamento de recompra? Estamos perdendo clientes ativos? Quero ver isso por grupos de entrada (Cohort).

**Valor do Cliente:** Qual o LTV médio e quem são nossos "**clientes de ouro**" vs. os que só compram em promoção? (**Análise RFM**).

O objetivo final é parar de "atirar para todos os lados" e **focar os investimentos onde há maior retorno**.

Aguardo uma proposta de como vocês pretendem estruturar essa análise.

Atenciosamente,
Ricardo Souza

## 📅 Planejamento

**De:** Sergio Ribeiro Cerqueira - Equipe de Análise de Dados

**Para:** Ricardo Souza – Diretor de E-commerce

**Assunto**: RE: Urgente: Diagnóstico de Eficiência de Vendas e Retenção (Projeto CLA)

Olá, Ricardo. Recebemos sua solicitação.

Segue a estruturação das etapas do plano de ação para o projeto de **CLA da Omnibox**:

1.  **Entendimento e Alinhamentos Gerais:** Realizaremos o *kick-off*  e as comunicações necessárias para definir KPIs, fórmulas de cálculo (ex: CAC, LTV), perguntas de negócio, critérios de aceite (DoD) e forma de entrega com o objetivo de elaborar e aprovar o “**Termo de abertura do projeto” (TAP)**.
2.  **Mapeamento e Compreensão de Dados:**  Localização e exploração das fontes (ERP, Logs e API) para entender como se conectam.
3.  **Centralização e Tratamento:** Criação de uma base única e confiável, tratando inconsistências para garantir um diagnóstico preciso.
4.  **Análise de Dados:** Execução das análises com foco nas respostas às perguntas levantadas no briefing.
5.  **Camadas de Apresentação:** Desenvolvimento de dashboards no Power BI e relatórios executivos.
6.  **Testes e Aprovações:** Validação técnica e de negócio.
7.  **Deploy:** Entrega final e apresentação dos insights.

**Nota 1:** A execução será **cíclica**. Conforme o método CRISP-DM, poderemos retornar a etapas anteriores para correções de rota sempre que novos insights surgirem.

**Nota 2:** Utilizaremos o método **SCRUM** para organização e controle das atividades, garantindo entregas incrementais e alinhamento constante com as prioridades do negócio.