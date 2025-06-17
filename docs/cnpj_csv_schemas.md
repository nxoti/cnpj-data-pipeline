# Schemas dos Arquivos CSV - Dados Abertos CNPJ

Especificação técnica dos campos de cada arquivo CSV. Todos utilizam ponto e vírgula (;) como separador.

## 1. EMPRESAS

Dados cadastrais básicos das empresas.

```
CNPJ_BASICO;RAZAO_SOCIAL;NATUREZA_JURIDICA;QUALIFICACAO_RESPONSAVEL;CAPITAL_SOCIAL;PORTE_EMPRESA;ENTE_FEDERATIVO_RESPONSAVEL
```

| Campo | Tipo | Descrição |
|-------|------|-----------|
| CNPJ_BASICO | CHAR(8) | Primeiros 8 dígitos do CNPJ |
| RAZAO_SOCIAL | VARCHAR | Nome empresarial |
| NATUREZA_JURIDICA | CHAR(4) | Código da natureza jurídica |
| QUALIFICACAO_RESPONSAVEL | CHAR(2) | Qualificação do responsável |
| CAPITAL_SOCIAL | DECIMAL | Capital social (vírgula como separador) |
| PORTE_EMPRESA | CHAR(2) | 00=Não informado, 01=ME, 03=EPP, 05=Demais |
| ENTE_FEDERATIVO_RESPONSAVEL | VARCHAR | Para órgãos públicos (natureza 1xxx) |

## 2. ESTABELECIMENTOS

Dados dos estabelecimentos (matriz e filiais).

```
CNPJ_BASICO;CNPJ_ORDEM;CNPJ_DV;IDENTIFICADOR_MATRIZ_FILIAL;NOME_FANTASIA;SITUACAO_CADASTRAL;DATA_SITUACAO_CADASTRAL;MOTIVO_SITUACAO_CADASTRAL;NOME_CIDADE_EXTERIOR;PAIS;DATA_INICIO_ATIVIDADE;CNAE_FISCAL_PRINCIPAL;CNAE_FISCAL_SECUNDARIA;TIPO_LOGRADOURO;LOGRADOURO;NUMERO;COMPLEMENTO;BAIRRO;CEP;UF;MUNICIPIO;DDD_1;TELEFONE_1;DDD_2;TELEFONE_2;DDD_FAX;FAX;CORREIO_ELETRONICO;SITUACAO_ESPECIAL;DATA_SITUACAO_ESPECIAL
```

Campos principais:
- **IDENTIFICADOR_MATRIZ_FILIAL**: 1=Matriz, 2=Filial
- **SITUACAO_CADASTRAL**: 01=Nula, 02=Ativa, 03=Suspensa, 04=Inapta, 08=Baixada
- **CNAE_FISCAL_SECUNDARIA**: Múltiplos CNAEs separados por vírgula
- **DATA_***: Formato YYYYMMDD (00000000 = nulo)

## 3. SOCIOS

Quadro societário das empresas.

```
CNPJ_BASICO;IDENTIFICADOR_SOCIO;NOME_SOCIO;CNPJ_CPF_SOCIO;QUALIFICACAO_SOCIO;DATA_ENTRADA_SOCIEDADE;PAIS;REPRESENTANTE_LEGAL;NOME_REPRESENTANTE;QUALIFICACAO_REPRESENTANTE_LEGAL;FAIXA_ETARIA
```

| Campo | Tipo | Descrição |
|-------|------|-----------|
| IDENTIFICADOR_SOCIO | CHAR(1) | 1=PJ, 2=PF, 3=Estrangeiro |
| CNPJ_CPF_SOCIO | CHAR(14) | CPF mascarado (***XXXXXX**) |
| FAIXA_ETARIA | CHAR(1) | 1=0-12, 2=13-20... 9=>80, 0=N/A |

## 4. SIMPLES

Dados do regime tributário Simples Nacional e MEI.

```
CNPJ_BASICO;OPCAO_PELO_SIMPLES;DATA_OPCAO_PELO_SIMPLES;DATA_EXCLUSAO_SIMPLES;OPCAO_PELO_MEI;DATA_OPCAO_PELO_MEI;DATA_EXCLUSAO_MEI
```

Valores para opções: S=Sim, N=Não, vazio=Não se aplica

## 5. Tabelas de Referência

### CNAES
```
CODIGO;DESCRICAO
```

### MUNICIPIOS
```
CODIGO;DESCRICAO
```

### NATUREZAS_JURIDICAS
```
CODIGO;DESCRICAO
```

### QUALIFICACOES_SOCIOS
```
CODIGO;DESCRICAO
```

### PAISES
```
CODIGO;DESCRICAO
```

### MOTIVOS
```
CODIGO;DESCRICAO
```

## Regras de Formatação

1. **Encoding**: ISO-8859-1 (Latin-1)
2. **Separador**: Ponto e vírgula (;)
3. **Decimais**: Vírgula como separador
4. **Datas nulas**: "00000000" ou "0"
5. **Campos vazios**: String vazia, não NULL

## Observações Técnicas

### Performance
- Arquivos grandes (até 2GB cada)
- Divididos em partes numeradas (Empresas0.zip, Empresas1.zip...)
- Processamento requer estratégia de chunking

### Integridade
- Nem todas as FKs são garantidas
- Possíveis códigos CNAE/município inexistentes
- Duplicatas possíveis em SOCIOS

---

# CSV File Schemas - CNPJ Open Data (English)

Technical specification for each CSV file. All use semicolon (;) as separator.

## Key Tables

1. **EMPRESAS** - Company base data
2. **ESTABELECIMENTOS** - Physical establishments
3. **SOCIOS** - Partners/shareholders
4. **SIMPLES** - Tax regime data

## Format Rules

- Encoding: ISO-8859-1
- Separator: Semicolon (;)
- Decimals: Comma separator
- Null dates: "00000000"
- Empty fields: Empty string

## Technical Notes

- Large files (up to 2GB each)
- Split into numbered parts
- Processing requires chunking strategy
- Not all FKs guaranteed
