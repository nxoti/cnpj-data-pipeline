# Guia de Qualidade de Dados CNPJ

Dados governamentais são como arqueologia digital - cada camada revela uma era diferente de sistemas e decisões. Aqui está o que você realmente encontrará ao processar 50 milhões de registros.

## Problemas Conhecidos e Soluções

### 1. Problemas de Encoding
**Sintoma**: Caracteres estranhos onde deveria haver "ç", "ã", "é"

**Causa**: Arquivos em ISO-8859-1, mas alguns campos já corrompidos na origem

**Solução Implementada** (em `src/processor.py`):
```python
# O sistema já converte encoding automaticamente
def _convert_file_encoding_chunked(self, input_file: Path, output_file: Optional[Path] = None) -> Path:
    """Converte encoding de ISO-8859-1 para UTF-8 usando leitura em chunks."""
    # Implementação real no código
    with open(input_file, "r", encoding="ISO-8859-1", buffering=self.config.encoding_chunk_size) as infile:
        with open(output_file, "w", encoding="UTF-8", buffering=self.config.encoding_chunk_size) as outfile:
            # Processamento em chunks para arquivos grandes
```

**IDEIA para melhorias futuras**:
```python
# TODO: Adicionar detecção automática de encoding corrompido
# def detect_and_fix_double_encoding(text):
#     """
#     SUGESTÃO: Detectar e corrigir encoding duplo.
#     Alguns campos passaram por encoding errado duas vezes.
#     """
#     # Implementar quando encontrar casos específicos
#     pass
```

### 2. Inconsistências de Dados

**Foreign Keys Quebradas**:
- ~2% dos CNAEs não existem na tabela de referência
- ~0.5% dos municípios são códigos extintos
- Alguns sócios referenciam países inexistentes

**Estratégia Atual** (implementada em `src/database/postgres.py`):
```python
# Sistema usa UPSERT com ON CONFLICT
def bulk_upsert(self, df: pl.DataFrame, table_name: str):
    """Bulk upsert com tratamento de conflitos."""
    # Código real trata conflitos automaticamente
    if update_clause:
        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES %s
            ON CONFLICT ({conflict_columns})
            DO UPDATE SET {update_clause}
        """
```

**IDEIAS para validação adicional**:
```python
# TODO: Implementar validação de referências antes da carga
# def validate_foreign_keys(df: pl.DataFrame, table_name: str) -> dict:
#     """
#     SUGESTÃO: Validar FKs antes de inserir.
#     Retorna dicionário com registros problemáticos.
#     """
#     # Implementar quando necessário relatórios de qualidade
#     pass
```

### 3. Problemas Temporais

**Validação Existente** (em `src/processor.py`):
```python
# Sistema já trata datas zeradas
date_cols = DATE_COLUMNS.get(file_type, [])
for col in date_cols:
    if col in df.columns:
        df = df.with_columns(
            pl.when(pl.col(col) == "0")
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
        )
```

**IDEIA para validação mais robusta**:
```python
# TODO: Adicionar validação de datas futuras
# def validate_date_sanity(date_str: str) -> Optional[date]:
#     """
#     SUGESTÃO: Validar datas impossíveis.
#     Ex: empresas "fundadas" em 2027
#     """
#     # if parsed_date > datetime.now():
#     #     logger.warning(f"Data futura detectada: {date_str}")
#     #     return None
#     pass
```

### 4. Problemas de Formatação

**Capital Social com Vírgulas**:
```python
# "120000000000,00" → 120000000000.00
capital = float(capital_str.replace(',', '.'))
```

**Espaços Extras**:
```python
# Comum em razão social e endereços
df = df.apply(lambda x: x.strip() if isinstance(x, str) else x)
```

**Zeros à Esquerda**:
```python
# CEPs às vezes vêm sem zeros
cep = cep.zfill(8)  # "1234567" → "01234567"
```

### 5. Dados Incompletos

**Problema**: Arquivos oficiais incompletos (ex: MOTICSV com 63 códigos vs 71 no SERPRO).

**Solução Implementada**: Busca automática de códigos faltantes durante processamento.

#### Fonte SERPRO
- URL: https://bcadastros.serpro.gov.br/documentacao/dominios/pj/motivo_situacao_cadastral.csv
- Cache: 30 dias
- Códigos adicionados: 8

#### Códigos Faltantes Identificados
| Código | Descrição |
|--------|-----------|
| 32 | DECURSO DE PRAZO DE INTERRUPCAO TEMPORARIA |
| 33 | REGISTRO CANCELADO |
| ... | (6 outros códigos) |

## Validações Essenciais

### 1. Validação de CNPJ
```python
def validate_cnpj_structure(cnpj_parts):
    """Valida estrutura do CNPJ completo."""
    basico, ordem, dv = cnpj_parts

    return (
        len(basico) == 8 and basico.isdigit() and
        len(ordem) == 4 and ordem.isdigit() and
        len(dv) == 2 and dv.isdigit()
    )
```

### 2. Validação de Referências
```python
def validate_references(df, reference_tables):
    """Identifica referências inválidas."""
    issues = {}

    # CNAE principal
    invalid_cnae = df[
        ~df['cnae_fiscal_principal'].isin(reference_tables['cnaes'])
    ]
    if len(invalid_cnae) > 0:
        issues['invalid_cnae'] = invalid_cnae['cnpj_basico'].tolist()

    return issues
```

## Métricas de Qualidade

Após processar o dataset completo, estas são as métricas típicas:

| Métrica | Valor Típico | Observação |
|---------|--------------|------------|
| CNPJ válidos | ~99.5% | Estrutura correta |
| Municípios válidos | ~98% | Alguns códigos extintos |
| CNAEs válidos | ~95% | Códigos desatualizados |
| Endereços completos | ~90% | Muitos sem número |
| Emails válidos | ~85% | Formato correto |
| Telefones válidos | ~70% | Muitos desatualizados |

## Pipeline de Limpeza Recomendado

**Implementação Atual** (baseada no código real):
```python
# Em src/processor.py - transformações aplicadas automaticamente
def _apply_transformations(self, df: pl.DataFrame, file_type: str) -> pl.DataFrame:
    """Pipeline real de transformação implementado."""
    # 1. Renomeação de colunas
    col_mapping = COLUMN_MAPPINGS.get(file_type, {})

    # 2. Conversão de numéricos (vírgula → ponto)
    numeric_cols = NUMERIC_COLUMNS.get(file_type, [])

    # 3. Limpeza de datas (zeros → NULL)
    date_cols = DATE_COLUMNS.get(file_type, [])

    return df
```

**IDEIAS para pipeline estendido**:
```python
# TODO: Adicionar estas validações quando necessário
# def extended_cleaning_pipeline(df: pl.DataFrame) -> pl.DataFrame:
#     """
#     SUGESTÃO: Pipeline completo com validações extras.
#     Implementar conforme necessidade de qualidade de dados.
#     """
#     # 1. Normalização de texto (upper, trim)
#     # text_cols = ['razao_social', 'nome_fantasia']
#     # for col in text_cols:
#     #     df = df.with_columns(pl.col(col).str.strip().str.to_uppercase())
#
#     # 2. Validação de padrões (CNPJ, CEP, email)
#     # 3. Detecção de anomalias (outliers em capital social)
#     # 4. Enriquecimento (adicionar região a partir de UF)
#
#     return df
```

## Armadilhas Não Documentadas

1. **CNPJs de teste**: Existem CNPJs claramente de teste (00.000.000/0001-00)
2. **Empresas fantasma**: Razão social genérica, mesmo endereço, sócios idênticos
3. **Encoding duplo**: Alguns campos passaram por encoding errado DUAS vezes
4. **Timestamps ocultos**: Alguns campos de texto contêm timestamps no final

## Conclusão

Qualidade de dados é um espectro, não um binário. O objetivo não é perfeição - é consistência suficiente para análises confiáveis.

Cada problema de qualidade conta uma história: um sistema legado migrado às pressas, uma validação que nunca existiu, um padrão que mudou mas os dados antigos permaneceram.

Abraçar essas imperfeições, documentá-las, e construir resiliência no seu pipeline - essa é a diferença entre um sistema que quebra a cada atualização e um que processa décadas de história empresarial brasileira sem pestanejar.

---

# CNPJ Data Quality Guide (English)

Government data is digital archaeology - each layer reveals different systems and decisions. Here's what you'll really find processing 50 million records.

## Known Issues

### Encoding Problems
- Files in ISO-8859-1 with corruption
- Characters like ç, ã, é appear broken
- Some fields double-encoded

### Data Inconsistencies
- ~2% invalid CNAE codes
- ~0.5% extinct municipality codes
- Duplicate partner records
- Missing establishments for some companies

### Temporal Issues
- Future dates (companies "founded" in 2027)
- Null dates as "00000000"
- Historical data with discontinued codes

### Formatting Issues
- Decimal comma separator
- Trailing spaces
- Missing leading zeros in CEP

## Essential Validations

```python
# CNPJ structure
len(basico) == 8 and basico.isdigit()

# Date validation
if date.year < 1900 or date > now:
    return None

# Reference validation
invalid_cnae = df[~df['cnae'].isin(valid_cnaes)]
```

## Quality Metrics

- ~99.5% valid CNPJ format
- ~98% valid municipality codes
- ~95% valid CNAE codes
- ~90% complete addresses
- ~85% valid emails

## Recommended Pipeline

1. Fix encoding issues
2. Normalize text (strip, uppercase)
3. Validate dates
4. Parse numeric fields
5. Remove obvious duplicates
6. Log quality metrics

## Hidden Pitfalls

- Test CNPJs (00.000.000/0001-00)
- Shell companies with generic data
- Double-encoded fields
- Hidden timestamps in text fields

Quality is a spectrum, not binary. The goal isn't perfection - it's sufficient consistency for reliable analysis.
