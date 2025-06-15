# CNPJ Data Pipeline

Um carregador modular e configurável para arquivos CNPJ da Receita Federal do Brasil. Processamento inteligente de 50+ milhões de empresas com suporte a múltiplos bancos de dados.

## Características Principais

- **Arquitetura Modular**: Separação clara de responsabilidades com camada de abstração de banco de dados
- **Multi-Banco**: PostgreSQL totalmente suportado, com placeholders para MySQL, BigQuery e SQLite
- **Processamento Inteligente**: Adaptação automática da estratégia baseada em recursos disponíveis
- **Processamento Incremental**: Rastreamento de arquivos processados para evitar duplicações
- **Performance Otimizada**: Operações bulk eficientes com tratamento de conflitos
- **Configuração Simples**: Setup interativo + variáveis de ambiente

## Início Rápido

### Opção 1: Setup Interativo (Recomendado)

```bash
# Clone o repositório
git clone https://github.com/cnpj-chat/cnpj-data-pipeline
cd cnpj-data-pipeline

# Execute o assistente de configuração
python setup.py
```

O assistente irá:
- Detectar recursos do sistema
- Configurar conexão com banco de dados
- Instalar dependências necessárias
- Criar configuração otimizada

### Opção 2: Configuração Manual

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar ambiente
cp env.example .env
# Editar .env com suas configurações

# Executar
python main.py
```

### Docker

```bash
# PostgreSQL (padrão)
docker-compose --profile postgres up --build

# Com configurações customizadas
DATABASE_BACKEND=postgresql BATCH_SIZE=100000 docker-compose --profile postgres up
```

## Configuração

### Seleção de Backend

```bash
# PostgreSQL (padrão e recomendado)
DATABASE_BACKEND=postgresql

# Suporte futuro
# DATABASE_BACKEND=mysql
# DATABASE_BACKEND=bigquery
# DATABASE_BACKEND=sqlite
```

### Estratégias de Processamento

O sistema detecta automaticamente a estratégia ideal:

| Memória | Estratégia | Descrição |
|---------|------------|-----------|
| <8GB | `memory_constrained` | Processamento em chunks pequenos |
| 8-32GB | `high_memory` | Batches maiores, cache otimizado |
| >32GB | `distributed` | Processamento paralelo máximo |

### Variáveis de Configuração

| Variável | Padrão | Descrição |
|----------|---------|-----------|
| `BATCH_SIZE` | `50000` | Tamanho do lote para operações |
| `MAX_MEMORY_PERCENT` | `80` | Uso máximo de memória |
| `TEMP_DIR` | `./temp` | Diretório temporário |
| `DB_HOST` | `localhost` | Host PostgreSQL |
| `DB_PORT` | `5432` | Porta PostgreSQL |
| `DB_NAME` | `cnpj` | Nome do banco |

## Arquitetura

```
cnpj-data-pipeline/
├── src/
│   ├── config.py          # Configuração com auto-detecção
│   ├── downloader.py      # Download e extração
│   ├── processor.py       # Parsing e transformação
│   └── database/          # Abstração de banco de dados
│       ├── base.py        # Interface abstrata
│       ├── factory.py     # Factory pattern
│       └── postgres.py    # Implementação PostgreSQL
├── main.py                # Ponto de entrada
└── setup.py               # Assistente de configuração
```

## Fluxo de Processamento

1. **Descoberta**: Localiza diretório mais recente de dados CNPJ
2. **Download**: Baixa e extrai arquivos ZIP com retry automático
3. **Processamento**: Parse dos CSVs com estratégia adaptativa
4. **Carga**: Bulk upsert otimizado no banco de dados
5. **Rastreamento**: Marca arquivos como processados

## Tipos de Arquivo Suportados

| Arquivo | Tabela | Descrição |
|---------|--------|-----------|
| `CNAECSV` | `cnaes` | Atividades econômicas |
| `EMPRECSV` | `empresas` | Dados das empresas |
| `ESTABELE` | `estabelecimentos` | Estabelecimentos físicos |
| `SOCIOCSV` | `socios` | Quadro societário |
| `SIMPLECSV` | `dados_simples` | Regime tributário |

## Performance

Tempos típicos de processamento:

| Sistema | Memória | Tempo (50M empresas) |
|---------|---------|---------------------|
| VPS básico | 4GB | ~12 horas |
| Servidor padrão | 16GB | ~3 horas |
| Servidor high-end | 64GB+ | ~1 hora |

## Desenvolvimento

### Princípios de Design

- **Modular**: Cada componente com responsabilidade única
- **Resiliente**: Tratamento de erros e retry automático
- **Eficiente**: Uso otimizado de memória e operações bulk
- **Adaptativo**: Ajuste automático aos recursos disponíveis

### Adicionando Novo Backend

1. Criar adapter em `src/database/seu_banco.py`
2. Implementar métodos abstratos de `DatabaseAdapter`
3. Registrar no factory em `src/database/factory.py`
4. Criar arquivo de requirements em `requirements/seu_banco.txt`

---

# CNPJ Data Pipeline (English)

A configurable, modular data pipeline for Brazilian CNPJ registry files. Smart processing of 50+ million companies with multi-database support.

## Key Features

- **Modular Architecture**: Clean separation with database abstraction
- **Multi-Database**: Full PostgreSQL support, placeholders for others
- **Smart Processing**: Auto-adapts to available resources
- **Incremental**: Tracks processed files
- **Optimized**: Efficient bulk operations
- **Easy Config**: Interactive setup + env vars

## Quick Start

### Interactive Setup

```bash
python setup.py
```

### Manual Setup

```bash
pip install -r requirements.txt
cp env.example .env
python main.py
```

### Docker

```bash
docker-compose --profile postgres up --build
```

## Configuration

Set `DATABASE_BACKEND` and `PROCESSING_STRATEGY` in `.env` file.

## Architecture

Factory pattern for database adapters, intelligent resource detection, chunked processing for large files.

## Performance

Processes 50M records in 1-12 hours depending on system resources.

Made with engineering excellence for the Brazilian tech community.