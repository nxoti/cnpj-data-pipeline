# Dados de Exemplo CNPJ

Antes de processar 50 milhões de registros, teste com 10. Estes arquivos são amostras reais dos dados CNPJ, preservando todas as peculiaridades do formato original.

## Arquivos Disponíveis

| Arquivo | Registros | Tamanho | Descrição |
|---------|-----------|---------|-----------|
| CNAECSV | 10 | 1KB | Classificações de atividade econômica |
| EMPRECSV | 10 | 1KB | Registros de empresas |
| ESTABELECSV | 10 | 3KB | Dados de estabelecimentos |
| MOTICSV | 10 | <1KB | Motivos de situação cadastral |
| MUNICCSV | 10 | <1KB | Códigos de municípios |
| NATJUCSV | 10 | <1KB | Naturezas jurídicas |
| PAISCSV | 10 | <1KB | Códigos de países |
| QUALSCSV | 10 | <1KB | Qualificações de sócios |
| SIMPLECSV | 10 | <1KB | Dados do Simples Nacional |
| SOCIOCSV | 10 | 1KB | Quadro societário |

## Casos de Teste Incluídos

### EMPRECSV - Empresas
```
"00000000" → Banco do Brasil (capital: 120 bilhões)
"00000001" → Associação sem fins lucrativos
"00000006" → Micro empresa (ME)
```

Casos cobertos:
- Diferentes portes (ME, EPP, Demais)
- Capital social com vírgula decimal
- Empresas sem capital social

### ESTABELECSV - Estabelecimentos
```
"07396865";"0001";"68" → Matriz em SC
"64904295";"0018";"51" → Filial 18 em AL
"03650261";"0001";"45" → Com nome fantasia
```

Casos cobertos:
- Matriz vs Filial
- Diferentes situações cadastrais
- Endereços completos e incompletos
- Múltiplos CNAEs secundários

### SOCIOCSV - Sócios
```
"***973291**" → CPF mascarado
Faixa etária "6" → 51-60 anos
Qualificação "49" → Sócio administrador
```

Casos cobertos:
- CPF corretamente mascarado
- Diferentes faixas etárias
- Sócios PF, PJ e estrangeiros

## Como Usar

### Teste Básico de Parsing (Código Real)
```python
import polars as pl

# Baseado em src/processor.py - leitura real
df = pl.read_csv(
    "EMPRECSV",
    separator=";",
    encoding="iso-8859-1",
    has_header=False,
    null_values=[""],
    ignore_errors=True,
    infer_schema_length=0
)

# Aplicar mapeamento de colunas (como no processador real)
col_mapping = {
    0: "cnpj_basico", 1: "razao_social", 2: "natureza_juridica",
    3: "qualificacao_responsavel", 4: "capital_social",
    5: "porte", 6: "ente_federativo_responsavel"
}
new_columns = [col_mapping.get(i, f"column_{i}") for i in range(len(df.columns))]
df = df.rename(dict(zip(df.columns, new_columns)))

print(f"Registros lidos: {len(df)}")
print(f"Colunas: {df.columns}")
```

### Teste de Transformações (Baseado no Código Real)
```python
# Transformação implementada em src/processor.py
# Converter capital social (vírgula → ponto)
df = df.with_columns(
    pl.col("capital_social")
    .str.replace(",", ".")
    .cast(pl.Float64, strict=False)
)

# Verificar valores
print(df["capital_social"].describe())
```

### Teste de Encoding
```python
# O sistema converte automaticamente para UTF-8
# Mas você pode testar o arquivo original:
with open("NATJUCSV", "r", encoding="iso-8859-1") as f:
    content = f.read()

# Deve mostrar acentuação em ISO-8859-1
print(content)
```

## Validações Recomendadas

### 1. Estrutura
```python
def validate_structure(df, expected_columns):
    """Valida se DataFrame tem estrutura esperada."""
    assert len(df.columns) == expected_columns
    assert len(df) > 0
    assert not df.is_empty()
```

### 2. Tipos de Dados
```python
def validate_types(df):
    """Valida tipos após transformação."""
    # CNPJ básico sempre 8 dígitos
    assert df["cnpj_basico"].str.lengths().min() == 8

    # Capital social numérico
    assert df["capital_social"].dtype == pl.Float64
```

### 3. Valores Especiais
```python
def validate_special_values(df):
    """Valida tratamento de valores especiais."""
    # Datas zeradas
    date_cols = df.select(pl.col("^data_.*$"))
    assert (date_cols == "00000000").sum() > 0

    # Campos vazios vs NULL
    assert df.null_count().sum() > 0
```

## Casos Extremos Incluídos

1. **Encoding**: Caracteres especiais (ç, ã, õ, é)
2. **Números grandes**: Capital social de 120 bilhões
3. **Campos vazios**: Empresas sem endereço completo
4. **Datas zeradas**: "00000000" e "20231113"
5. **CPF mascarado**: Formato ***XXXXXX**

## IDEIAS para Validações Avançadas

### SUGESTÃO: Framework de Validação
```python
# TODO: Implementar quando necessário testes automatizados
# def validate_sample_data():
#     """
#     IDEIA: Suite completa de validação para dados de exemplo.
#
#     Validações sugeridas:
#     1. Estrutura (número de colunas, tipos)
#     2. Integridade referencial entre arquivos
#     3. Valores especiais (datas zeradas, campos vazios)
#     4. Padrões de dados (CPF mascarado, CNPJ válido)
#     """
#     pass

# EXEMPLO conceitual de validação:
# def test_referential_integrity():
#     # Carregar empresas e estabelecimentos
#     empresas = load_sample("EMPRECSV")
#     estabelecimentos = load_sample("ESTABELECSV")
#
#     # Todo estabelecimento deve ter empresa
#     cnpj_empresas = set(empresas["cnpj_basico"])
#     cnpj_estab = set(estabelecimentos["cnpj_basico"])
#
#     assert cnpj_estab.issubset(cnpj_empresas), "Estabelecimento órfão encontrado"
```

### SUGESTÃO: Pipeline de Teste Completo
```python
# TODO: Criar suite de testes quando implementar CI/CD
# class CNPJTestPipeline:
#     """
#     IDEIA: Pipeline automatizado para validar processamento.
#
#     Etapas sugeridas:
#     1. Ler todos os arquivos de exemplo
#     2. Aplicar transformações
#     3. Validar output
#     4. Gerar relatório de qualidade
#
#     Útil para:
#     - Regression testing
#     - Validar mudanças no processador
#     - Treinar novos desenvolvedores
#     """
#     pass
```

## Notas Importantes

1. **Dados reais**: Extraídos diretamente dos arquivos oficiais
2. **Anonimização**: CPFs já vêm mascarados da fonte
3. **Representatividade**: Cobrem os principais casos de uso
4. **Encoding original**: Mantido ISO-8859-1 para testes realistas

## Conclusão

Estes arquivos de exemplo são seu laboratório. Use-os para:
- Desenvolver e testar transformações
- Validar handling de encoding
- Entender a estrutura antes do processamento em massa
- Treinar novos membros da equipe

Dominar estes 10 registros é o primeiro passo para processar 50 milhões com confiança.

---

# CNPJ Sample Data (English)

Before processing 50 million records, test with 10. These files are real CNPJ data samples, preserving all format quirks.

## Available Files

Each file contains 10 records from actual CNPJ data:
- Company registrations (EMPRECSV)
- Establishments (ESTABELECSV)
- Partners (SOCIOCSV)
- Reference tables (CNAE, municipalities, etc.)

## Test Cases Included

- Different company sizes (ME, EPP, Large)
- Decimal comma separator
- Masked CPF format
- Zero dates
- Special characters
- Empty vs NULL fields

## Usage

```python
# Read with correct encoding
df = pl.read_csv(
    "EMPRECSV",
    separator=";",
    encoding="iso-8859-1",
    has_header=False
)

# Test transformations
df["capital_social"] = df["capital_social"].str.replace(",", ".")
```

## Key Validations

1. Structure validation
2. Type checking after transformation
3. Special value handling
4. Referential integrity

## Edge Cases

- Encoding issues (ç, ã, õ)
- Large numbers (120 billion capital)
- Incomplete addresses
- Zero dates ("00000000")
- Masked CPF (***XXXXXX**)

Master these 10 records before processing 50 million.
