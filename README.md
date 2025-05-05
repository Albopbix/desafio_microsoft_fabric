Microsoft Fabric Challenge: Monitoramento de Vagas de Emprego

Objetivo do Desafio

Criar uma solução de ingestão, processamento e visualização de dados de vagas de emprego em tempo real ou em lote, com foco em:

Localidade
Cargo
Empresa
Salário médio

A solução será mantida com CI/CD utilizando GitHub ou Azure DevOps, garantindo uma entrega contínua e confiável.

Etapas do Projeto

1. Ingestão de Dados

Fonte: API do Indeed via RapidAPI
Método: GET
Endpoint exemplo: https://rapidapi.com/mantiks-mantiks-default/api/indeed12

2. Tratamento, Transformações e Armazenamento

Ferramenta: A critério de cada participante
Exemplos de transformações:
Normalização de colunas: título, empresa, localização, salário, descrição
Tratamento de valores nulos
Conversão de datas
Criação de colunas derivadas (ex: tipo de vaga, nível de senioridade)

3. Consultas SQL

Quais são os cargos mais ofertados no mercado?
Quais empresas estão com mais vagas abertas?
Qual é o salário médio por cargo?
Em quais cidades ou estados há mais vagas disponíveis?
Como está a tendência de publicação de vagas ao longo do tempo?
Quais cargos oferecem salários acima da média geral?
Quais vagas exigem experiência prévia e em qual nível?
Quantas vagas são para trabalho remoto, híbrido ou presencial?
Quais empresas oferecem os maiores salários?
Quantas vagas foram publicadas na última semana?
Qual a distribuição de vagas por setor ou área de atuação?
Existem padrões de vagas por dia da semana?
Qual a faixa salarial mais comum nas vagas?
Há mais vagas para júnior, pleno ou sênior?
Quais as tecnologias ou habilidades mais pedidas nas vagas?

4. Visualização no Power BI

📊 Dashboards:

Total de vagas por localidade
Salário médio por função
Empresas que mais contratam
Tendência de vagas ao longo do tempo