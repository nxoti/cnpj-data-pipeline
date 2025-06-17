# Visão Geral dos Dados CNPJ

O maior desafio em trabalhar com dados governamentais não é o volume - é entender a lógica por trás da estrutura.

## O que são os Dados CNPJ?

O Cadastro Nacional da Pessoa Jurídica é o registro oficial de todas as empresas brasileiras. Este conjunto de dados abertos contém:

- **50+ milhões** de empresas cadastradas
- **60+ milhões** de estabelecimentos físicos
- **20+ milhões** de relações societárias
- Atualização **mensal** pela Receita Federal

## Estrutura dos Dados

Os dados seguem uma hierarquia clara:

### Tabelas Principais
```
EMPRESAS (1) ──┬── (N) ESTABELECIMENTOS
               ├── (N) SOCIOS
               └── (1) SIMPLES
```

**EMPRESAS**: Entidade jurídica principal
- Identificada pelo CNPJ básico (8 dígitos)
- Contém razão social, natureza jurídica, capital

**ESTABELECIMENTOS**: Localizações físicas
- Cada filial tem seu próprio registro
- Matriz identificada por tipo = 1
- Endereço completo e atividade econômica

**SOCIOS**: Quadro societário
- Pessoas físicas e jurídicas
- CPF mascarado por privacidade
- Data de entrada na sociedade

**SIMPLES**: Regime tributário
- Opção pelo Simples Nacional
- Histórico de entrada/saída MEI

### Tabelas de Referência

Dados que raramente mudam, usados para lookups:

- **CNAES**: Classificação de atividades econômicas
- **MUNICIPIOS**: Códigos IBGE dos municípios
- **NATUREZAS_JURIDICAS**: Tipos de empresa (LTDA, SA, etc)
- **PAISES**: Códigos de países
- **QUALIFICACOES_SOCIOS**: Papéis dos sócios
- **MOTIVOS**: Razões para mudança de status

## Características Técnicas

### Formato
- **Encoding**: ISO-8859-1 (não UTF-8!)
- **Separador**: Ponto e vírgula (;)
- **Tamanho**: ~15GB comprimido, ~85GB processado

### Peculiaridades
1. **Datas zeradas**: "00000000" significa NULL
2. **Decimais brasileiros**: Vírgula como separador
3. **CPF mascarado**: Formato ***XXXXXX**
4. **Múltiplos arquivos**: Divididos em partes numeradas

## Desafios de Processamento

### Volume
Com 50 milhões de registros, uma abordagem ingênua de carregar tudo em memória simplesmente não funciona. É necessário:
- Processamento em chunks
- Estratégia de upsert eficiente
- Índices bem planejados

### Qualidade
Dados governamentais têm suas peculiaridades:
- Inconsistências de encoding
- Chaves estrangeiras inválidas
- Registros duplicados
- Campos com espaços extras
- **Referências incompletas**: Arquivos oficiais omitem alguns códigos usados nos dados

**Solução**: Pipeline detecta e corrige automaticamente, buscando dados complementares do SERPRO quando necessário.

### Performance
O gargalo raramente é o processamento - é a carga no banco:
- COPY é 10x mais rápido que INSERT
- Upsert em staging tables para conflitos
- Batch size importa (50k é um sweet spot)

## Insights de Implementação

Após processar bilhões de registros, algumas lições:

1. **Não confie no encoding**: Sempre converta ISO-8859-1 → UTF-8
2. **Memória é finita**: Use lazy evaluation e streaming
3. **Banco de dados é o gargalo**: Otimize as operações de escrita
4. **Rastreie o progresso**: Processamento incremental é essencial

A beleza destes dados está na completude - cada empresa brasileira está aqui. O desafio está em transformar este tesouro bruto em insights acionáveis.

---

# CNPJ Data Overview (English)

Understanding Brazilian company registry data - the complete picture of 50+ million businesses.

## What is CNPJ Data?

Brazil's official company registry containing:
- **50+ million** companies
- **60+ million** establishments
- **20+ million** partnership records
- **Monthly** updates from Federal Revenue

## Structure

Main tables connected through CNPJ:
- **EMPRESAS**: Parent company entity
- **ESTABELECIMENTOS**: Physical locations
- **SOCIOS**: Partners/shareholders
- **SIMPLES**: Tax regime options

Reference tables for lookups:
- Economic activity codes (CNAE)
- Municipality codes
- Legal entity types
- Partner roles

## Technical Details

- Format: ISO-8859-1, semicolon-separated
- Size: ~15GB compressed, ~85GB processed
- Dates: YYYYMMDD format (zeros = null)
- Privacy: CPF numbers partially masked

## Processing Challenges

Volume requires chunked processing. Quality issues include encoding problems and invalid foreign keys. Performance bottleneck is database writes, not parsing.

The key insight: this isn't just data - it's the complete economic map of Brazil.
