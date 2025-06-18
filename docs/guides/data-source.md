# Guia de Fonte de Dados CNPJ

Obter dados CNPJ é simples. O desafio está em fazê-lo de forma confiável, todo mês, sem supervisão humana.

## Fonte Oficial

**Portal**: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj

**URL Base**: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/

**Responsável**: Receita Federal do Brasil

## Cronograma de Atualizações

A Receita Federal segue um padrão previsível:

- **Frequência**: Mensal
- **Data**: Primeira semana do mês (geralmente entre dias 1-7)
- **Horário**: Entre 00:00 e 06:00 BRT
- **Defasagem**: Dados com 30-45 dias de atraso

Exemplo: Em dezembro/2024, você baixa dados de outubro/2024.

## Estrutura de Diretórios

```
/dados_abertos_cnpj/
├── 2024-01/
│   ├── Empresas0.zip
│   ├── Empresas1.zip
│   ├── ...
│   ├── Empresas9.zip
│   ├── Estabelecimentos0.zip
│   ├── ...
│   ├── Estabelecimentos19.zip
│   ├── Socios0.zip
│   ├── ...
│   ├── Simples.zip
│   ├── Cnaes.zip
│   ├── Motivos.zip
│   ├── Municipios.zip
│   ├── Naturezas.zip
│   ├── Paises.zip
│   └── Qualificacoes.zip
├── 2024-02/
└── ...
```

## Estratégia de Download

### Descoberta Automatizada (Implementada)
```python
# Em src/downloader.py - código real
def get_latest_directories(self) -> List[str]:
    """Descobre o diretório mais recente disponível."""
    response = requests.get(self.config.base_url, timeout=(...))
    soup = BeautifulSoup(response.text, "html.parser")

    dirs = []
    for link in soup.find_all("a"):
        href = link.get("href")
        text = link.text.strip()
        # Match directories in YYYY-MM format
        if href and text.strip("/").count("-") == 1:
            dir_name = text.strip("/")
            dirs.append(dir_name)

    return sorted(dirs, reverse=True)  # Mais recente primeiro
```

### Download com Retry (Implementado)
```python
# Em src/downloader.py - código real
def download_and_extract(self, directory: str, filename: str) -> List[Path]:
    """Download com retry automático."""
    for attempt in range(self.config.retry_attempts):
        try:
            response = requests.get(url, stream=True, timeout=(...))
            response.raise_for_status()
            # ... processamento do arquivo
            break
        except Exception as e:
            if attempt < self.config.retry_attempts - 1:
                time.sleep(self.config.retry_delay)
            else:
                raise
```

## Características dos Arquivos

### Tamanhos Típicos
| Tipo | Arquivos | Tamanho Total |
|------|----------|---------------|
| Empresas | 10 arquivos | ~3 GB |
| Estabelecimentos | 20 arquivos | ~8 GB |
| Sócios | 10 arquivos | ~4 GB |
| Simples | 1 arquivo | ~500 MB |
| Referências | 6 arquivos | ~10 MB |

**Total**: ~15.5 GB comprimido

### Nomenclatura
- Dados principais: `[Tipo][Numero].zip`
- Referências: `[Tabela].zip`
- Numeração: Começa em 0

## Otimizações de Performance

### IDEIA: Download Paralelo
```python
# TODO: Implementar quando necessário otimizar downloads
# class ParallelDownloader:
#     """
#     SUGESTÃO: Download paralelo para múltiplos arquivos.
#     Pode reduzir tempo total de download significativamente.
#
#     Considerações:
#     - max_workers = 4 (respeitar servidor público)
#     - Implementar progress tracking
#     - Thread-safe para atualizações de progresso
#     """
#     pass

# Por enquanto, o sistema usa download sequencial (mais estável)
```

### Validação de Integridade (Implementada)
```python
# Em src/downloader.py - validação real de ZIPs
with zipfile.ZipFile(zip_path, "r") as zip_ref:
    # Extrai apenas arquivos CNPJ conhecidos
    for member in zip_ref.namelist():
        member_upper = member.upper()
        is_cnpj_file = any(
            member_upper.endswith(pattern) for pattern in known_patterns
        )
        if is_cnpj_file:
            zip_ref.extract(member, self.temp_path)
```

## Monitoramento Automatizado

### IDEIAS para Implementação Futura:

```python
# TODO: Sistema de notificações quando houver novos dados
# def setup_monitoring():
#     """
#     SUGESTÕES de implementação:
#
#     1. Cron job para verificar novos dados:
#        0 6 * * * /usr/bin/python3 /path/to/check_updates.py
#
#     2. Notificações via:
#        - Email (SMTP)
#        - Slack webhook
#        - Discord bot
#        - SMS (Twilio)
#
#     3. Integração com ferramentas de monitoramento:
#        - Prometheus metrics
#        - Grafana dashboards
#        - CloudWatch alarms
#     """
#     pass

# EXEMPLO conceitual de verificação:
# def check_for_updates():
#     latest = downloader.get_latest_directories()[0]
#     last_processed = load_from_database()
#
#     if latest > last_processed:
#         # Disparar processamento ou notificação
#         pass
```

## Problemas Comuns e Soluções

### Servidor Instável
A Receita Federal ocasionalmente fica sobrecarregada:
- Use timeouts generosos (300s+)
- Implemente retry com backoff
- Considere downloads noturnos

### Arquivos Corrompidos
Raramente, arquivos vêm corrompidos:
- Sempre valide ZIPs após download
- Mantenha checksums dos arquivos
- Re-baixe se necessário

### Mudanças de Estrutura
O formato ocasionalmente muda:
- Monitore logs para erros de parsing
- Mantenha mapeamentos flexíveis
- Versione seu schema

## Melhores Práticas

1. **Processamento Incremental**: Rastreie arquivos já processados
2. **Download Resumível**: Suporte para retomar downloads interrompidos
3. **Downloads Paralelos**: Configure DOWNLOAD_STRATEGY=parallel para melhor performance
4. **Verificação de Espaço**: 15GB download + 85GB processado = 100GB necessários
5. **Bandwidth Throttling**: Seja respeitoso com servidores públicos

## Conclusão

Baixar dados é a parte fácil. O verdadeiro desafio está em construir um pipeline que rode todo mês, sem falhar, adaptando-se a mudanças sutis, recuperando-se de erros, e avisando quando algo realmente importante mudou.

A Receita Federal fornece um serviço público valioso. Nossa responsabilidade como engenheiros é consumi-lo de forma eficiente e respeitosa, transformando dados brutos em valor para a sociedade.

---

# CNPJ Data Source Guide (English)

Getting CNPJ data is simple. The challenge is doing it reliably, every month, without human supervision.

## Official Source

**Portal**: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj

**Base URL**: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/

## Update Schedule

- **Frequency**: Monthly
- **Date**: First week (days 1-7)
- **Time**: 00:00-06:00 BRT
- **Delay**: 30-45 days behind

## File Structure

```
/2024-01/
├── Empresas[0-9].zip      (~3GB total)
├── Estabelecimentos[0-19].zip  (~8GB total)
├── Socios[0-9].zip        (~4GB total)
└── [Reference tables].zip  (~10MB)
```

Total: ~15.5GB compressed

## Download Strategy

- Parallel downloads (4 workers optimal)
- Retry with exponential backoff
- ZIP integrity validation
- Progress tracking

## Common Issues

1. **Unstable server**: Use generous timeouts
2. **Corrupted files**: Always validate ZIPs
3. **Structure changes**: Keep mappings flexible

## Best Practices

- Track processed files
- Support resumable downloads
- Configure parallel downloads for better performance
- Check disk space (need 100GB)
- Throttle bandwidth respectfully

Building a reliable monthly pipeline is the real engineering challenge.
