# 🇧🇷 CNPJ Data Pipeline

Um script modular e configurável para processar arquivos CNPJ da Receita Federal do Brasil. Processamento inteligente de 50+ milhões de empresas com suporte a múltiplos bancos de dados.

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

## Deployment

Este é um job batch que processa dados CNPJ e finaliza. A Receita Federal atualiza os dados mensalmente, então agende a execução mensal.

### Execução Manual

```bash
# Executar uma vez
docker-compose up

# Ou sem Docker
python main.py
```

### Execução Agendada (Mensal)

**Linux/Mac (cron):**
```bash
# Executar no dia 5 de cada mês às 2h da manhã
crontab -e
# Adicionar:
0 2 5 * * cd /caminho/para/cnpj-data-pipeline && docker-compose up >> /var/log/cnpj-pipeline.log 2>&1
```

**Windows (Task Scheduler):**
- Criar tarefa agendada mensal
- Comando: `docker-compose up`

**Kubernetes:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cnpj-pipeline
spec:
  schedule: "0 2 5 * *"  # Dia 5 às 2h
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: cnpj-pipeline
            image: sua-imagem
          restartPolicy: OnFailure
```

**GitHub Actions:**
```yaml
on:
  schedule:
    - cron: '0 2 5 * *'  # Dia 5 às 2h UTC
```

### Plataformas que Requerem Containers Ativos

Algumas plataformas (PaaS) esperam que containers permaneçam em execução. Se necessário:

```bash
# Manter container ativo
docker run -d --name cnpj sua-imagem tail -f /dev/null

# Agendar execução mensal do comando:
docker exec cnpj python main.py
```

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
| `CNAECSV` | `cnaes` | Classificações de atividade econômica |
| `EMPRECSV` | `empresas` | Registros de empresas |
| `ESTABELECSV` | `estabelecimentos` | Dados de estabelecimentos |
| `MOTICSV` | `motivos_situacao_cadastral` | Motivos de situação cadastral |
| `MUNICCSV` | `municipios` | Códigos de municípios |
| `NATJUCSV` | `naturezas_juridicas` | Naturezas jurídicas |
| `PAISCSV` | `paises` | Códigos de países |
| `QUALSCSV` | `qualificacoes_socios` | Qualificações de sócios |
| `SIMPLECSV` | `dados_simples` | Dados do Simples Nacional |
| `SOCIOCSV` | `socios` | Quadro societário |

## Performance

Tempos típicos de processamento:

| Sistema | Memória | Tempo (60M+ empresas) |
|---------|---------|---------------------|
| VPS básico | 4GB | ~8 horas |
| Servidor padrão | 16GB | ~2 horas |
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

# 🇧🇷 CNPJ Data Pipeline (English)

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

## Deployment

This is a batch job that processes CNPJ data and exits. Schedule it to run monthly.

### Manual Execution

```bash
# Run once
docker-compose up
```

### Scheduled Execution (Monthly)

**Linux/Mac (cron):**
```bash
# Run on the 5th of each month at 2 AM
0 2 5 * * cd /path/to/cnpj-pipeline && docker-compose up
```

**Other platforms:** Use your platform's scheduler (Task Scheduler, Kubernetes CronJob, GitHub Actions, etc.)

### Note for PaaS Platforms

If your platform requires containers to stay running:

```bash
# Keep container alive
docker run -d --name cnpj your-image tail -f /dev/null

# Schedule this command monthly:
docker exec cnpj python main.py
```

## Configuration

Set `DATABASE_BACKEND` and `PROCESSING_STRATEGY` in `.env` file.

## Architecture

Factory pattern for database adapters, intelligent resource detection, chunked processing for large files.

## Performance

Processes 60M+ records in 1-12 hours depending on system resources.

Made with engineering excellence for the Brazilian tech community.
