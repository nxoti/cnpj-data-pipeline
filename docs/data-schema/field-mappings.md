# Mapeamento de Campos CNPJ

Transformar dados governamentais em informação útil exige entender cada peculiaridade do formato original. Após processar milhões de registros, estas são as transformações essenciais.

## Tratamento de Campos Especiais

### Campos de Data
Zeros não são datas - são NULLs disfarçados:

```python
# Campos afetados:
date_fields = [
    'data_situacao_cadastral',
    'data_inicio_atividade', 
    'data_situacao_especial',
    'data_entrada_sociedade',
    'data_opcao_pelo_simples',
    'data_exclusao_do_simples',
    'data_opcao_pelo_mei',
    'data_exclusao_do_mei'
]

# Transformação:
if value in ['00000000', '0']:
    return None
else:
    return parse_date(value)  # YYYYMMDD → date
```

### Campos Numéricos
O Brasil usa vírgula como separador decimal:

```python
# Capital social: "120000000000,00" → 120000000000.00
value.replace(',', '.')
```

## Códigos e Suas Descrições

### Porte da Empresa
```
00 → Não informado
01 → Micro empresa (ME)
03 → Empresa de pequeno porte (EPP)
05 → Demais
```

### Situação Cadastral
```
01 → Nula
02 → Ativa
03 → Suspensa
04 → Inapta
08 → Baixada
```

### Identificador Matriz/Filial
```
1 → Matriz (sede)
2 → Filial
```

### Identificador de Sócio
```
1 → Pessoa Jurídica
2 → Pessoa Física
3 → Estrangeiro
```

### Opções Simples/MEI
```
S → Sim
N → Não
(vazio) → Não se aplica
```

## Regras de Processamento

### CNAE Secundário
Múltiplas atividades vêm concatenadas:
```
"4639701,4637199" → ["4639701", "4637199"]
```

### Mascaramento de CPF
Formato obrigatório por lei:
```
Original: 12345678901
Mascarado: ***456789**
```

### Ente Federativo
Preenchido apenas para órgãos públicos (natureza jurídica 1xxx):
```
Se natureza_juridica.startswith('1'):
    ente_federativo = 'UNIÃO' | 'ESTADO' | 'MUNICÍPIO'
```

### Faixa Etária
Calculada a partir da data de nascimento:
```
1 → 0-12 anos
2 → 13-20 anos
3 → 21-30 anos
4 → 31-40 anos
5 → 41-50 anos
6 → 51-60 anos
7 → 61-70 anos
8 → 71-80 anos
9 → Acima de 80 anos
0 → Não se aplica
```

## Implementação Prática

```python
# EXEMPLO DE IMPLEMENTAÇÃO (baseado em src/processor.py):

def _apply_transformations(df: pl.DataFrame, file_type: str) -> pl.DataFrame:
    """Aplica transformações ao dataframe - implementação real."""
    # Renomear colunas
    col_mapping = COLUMN_MAPPINGS.get(file_type, {})
    if col_mapping:
        new_columns = []
        for i in range(len(df.columns)):
            new_columns.append(col_mapping.get(i, f"column_{i}"))
        df = df.rename(dict(zip(df.columns, new_columns)))
    
    # Converter colunas numéricas
    numeric_cols = NUMERIC_COLUMNS.get(file_type, [])
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.replace(",", ".").cast(pl.Float64, strict=False)
            )
    
    # Limpar colunas de data
    date_cols = DATE_COLUMNS.get(file_type, [])
    for col in date_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col) == "0")
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
    
    return df
```

### IDEIAS PARA EXTENSÃO (não implementadas):

```python
# Função sugerida para expandir códigos em descrições
def expand_codes_to_descriptions(df: pl.DataFrame, file_type: str) -> pl.DataFrame:
    """
    IDEIA: Adicionar descrições legíveis aos códigos.
    TODO: Implementar quando necessário lookup de descrições.
    """
    # Exemplo conceitual:
    # if file_type == "EMPRECSV" and "porte" in df.columns:
    #     porte_map = {"01": "Micro Empresa", "03": "EPP", "05": "Demais"}
    #     df = df.with_columns(
    #         pl.col("porte").map_dict(porte_map).alias("porte_descricao")
    #     )
    pass

# Função sugerida para validação de CPF mascarado
def validate_masked_cpf(cpf_str: str) -> bool:
    """
    IDEIA: Validar formato de CPF mascarado.
    TODO: Implementar se necessário validação adicional.
    """
    # Formato esperado: ***XXXXXX**
    # return bool(re.match(r"^\*{3}\d{6}\*{2}$", cpf_str))
    pass
```

## Armadilhas Comuns

1. **Encoding duplo**: Alguns campos já vêm com problemas de encoding da origem
2. **Trailing spaces**: Campos de texto frequentemente têm espaços no final
3. **Códigos inválidos**: Nem sempre os códigos correspondem às tabelas de referência
4. **Nulls inconsistentes**: Às vezes "", às vezes "0", às vezes "00000000"

A lição mais importante? Dados governamentais refletem décadas de sistemas legados. Cada peculiaridade tem uma história - e ignorá-las significa bugs em produção.

---

# CNPJ Field Mappings (English)

Essential transformations for Brazilian company data processing.

## Date Fields
Zeros mean NULL:
- "00000000" → None
- Valid dates → Parse as YYYYMMDD

## Numeric Fields
Comma as decimal separator:
- "120000000000,00" → 120000000000.00

## Code Mappings

**Company Size**: 00=Not informed, 01=Micro, 03=Small, 05=Other

**Status**: 01=Null, 02=Active, 03=Suspended, 04=Inactive, 08=Closed

**Establishment Type**: 1=Headquarters, 2=Branch

**Partner Type**: 1=Company, 2=Individual, 3=Foreign

## Processing Rules

- **Secondary CNAE**: Split comma-separated values
- **CPF Masking**: ***XXXXXX** format required by law
- **Government Entity**: Only for legal nature 1xxx
- **Age Range**: Calculated from birth date (1-9 scale)

## Common Pitfalls

- Double encoding issues
- Trailing spaces in text fields
- Invalid reference codes
- Inconsistent null representations

The key lesson: government data reflects decades of legacy systems. Every quirk has a reason.