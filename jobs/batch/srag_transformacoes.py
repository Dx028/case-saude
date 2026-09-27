"""
Transformações do SRAG (SIVEP-Gripe / OpenDataSUS): bronze -> silver -> gold.

A bronze guarda as 194 colunas originais como texto. A silver seleciona o que
é necessário para a análise (minimização, LGPD), tipa, traduz os códigos do
dicionário de dados e protege quase-identificadores:
  * NU_NOTIFIC (número da notificação) -> pseudônimo HMAC (permite deduplicar)
  * DT_NASC (data de nascimento)       -> descartada (usa-se a faixa etária)
  * ocupação, nome da unidade, lotes de vacina e texto livre -> descartados

Todas as funções são puras (DataFrame -> DataFrame) e testadas em tests/.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import mascaramento as m

# ---------------------------------------------------------------- dicionário de dados
SEXO = {"M": "Masculino", "F": "Feminino", "I": "Ignorado"}
RACA = {"1": "Branca", "2": "Preta", "3": "Amarela", "4": "Parda", "5": "Indígena", "9": "Ignorado"}
SIM_NAO = {"1": "Sim", "2": "Não", "9": "Ignorado"}
SUPORTE_VENTILATORIO = {"1": "Invasivo", "2": "Não invasivo", "3": "Não", "9": "Ignorado"}
CLASSIFICACAO = {
    "1": "SRAG por influenza",
    "2": "SRAG por outro vírus respiratório",
    "3": "SRAG por outro agente etiológico",
    "4": "SRAG não especificado",
    "5": "SRAG por covid-19",
}
EVOLUCAO = {"1": "Cura", "2": "Óbito", "3": "Óbito por outras causas", "9": "Ignorado"}


def _mapear(coluna, dicionario, padrao):
    mapa = F.create_map(*[F.lit(x) for kv in dicionario.items() for x in kv])
    return F.coalesce(F.try_element_at(mapa, F.trim(F.col(coluna))), F.lit(padrao))


def _data(coluna):
    """As datas vêm como '2024-08-29T00:00:00.000Z'; valores inválidos viram nulo."""
    return F.try_to_timestamp(F.substring(F.col(coluna), 1, 10), F.lit("yyyy-MM-dd")).cast("date")


def municipio_ibge6(coluna):
    """
    Padroniza o código de município (6 dígitos) do SIVEP-Gripe para o cadastro do IBGE.
    O Distrito Federal tem um único município no IBGE (Brasília, 530010), mas o SIVEP
    registra as Regiões Administrativas (Ceilândia, Taguatinga...) com códigos próprios
    iniciados em 53: todos são mapeados para Brasília.
    """
    codigo = F.nullif(F.trim(F.col(coluna)), F.lit(""))
    return F.when(codigo.startswith("53"), F.lit("530010")).otherwise(codigo)


def _inteiro(coluna):
    return F.nullif(F.trim(F.col(coluna)), F.lit("")).try_cast("int")


def idade_em_anos():
    """NU_IDADE_N é expressa na unidade de TP_IDADE: 1 = dias, 2 = meses, 3 = anos."""
    n = _inteiro("NU_IDADE_N")
    tipo = F.trim(F.col("TP_IDADE"))
    return (F.when(tipo == "3", n)
             .when(tipo == "2", F.floor(n / 12))
             .when(tipo == "1", F.floor(n / 365))
             .cast("int"))


def faixa_etaria(idade):
    inicio = (F.floor(idade / 10) * 10).cast("int")
    return (F.when(idade.isNull() | (idade < 0) | (idade > 120), F.lit("não informado"))
             .when(idade >= 80, F.lit("80+"))
             .otherwise(F.concat(inicio.cast("string"), F.lit("-"), (inicio + 9).cast("string"))))


# ---------------------------------------------------------------- bronze -> silver
def para_silver(bronze: DataFrame) -> DataFrame:
    idade = idade_em_anos()
    silver = bronze.select(
        m.pseudonimizar(F.col("NU_NOTIFIC")).alias("notificacao_id"),
        F.col("ano").cast("int").alias("ano"),
        _data("DT_NOTIFIC").alias("dt_notificacao"),
        _inteiro("SEM_NOT").alias("semana_notificacao"),
        _data("DT_SIN_PRI").alias("dt_primeiros_sintomas"),
        _inteiro("SEM_PRI").alias("semana_sintomas"),
        F.upper(F.trim("SG_UF_NOT")).alias("uf_notificacao"),
        municipio_ibge6("CO_MUN_NOT").alias("municipio_notificacao_ibge6"),
        F.upper(F.trim("SG_UF")).alias("uf_residencia"),
        municipio_ibge6("CO_MUN_RES").alias("municipio_residencia_ibge6"),
        F.nullif(F.trim("CO_MUN_RES"), F.lit("")).alias("municipio_residencia_sivep"),  # código original (RAs do DF)
        _mapear("CS_SEXO", SEXO, "Ignorado").alias("sexo"),
        idade.alias("idade"),
        faixa_etaria(idade).alias("faixa_etaria"),
        _mapear("CS_RACA", RACA, "Ignorado").alias("raca_cor"),
        _mapear("FATOR_RISC", SIM_NAO, "Ignorado").alias("fator_risco"),
        _mapear("HOSPITAL", SIM_NAO, "Ignorado").alias("hospitalizado"),
        _data("DT_INTERNA").alias("dt_internacao"),
        _mapear("UTI", SIM_NAO, "Ignorado").alias("internado_uti"),
        _data("DT_ENTUTI").alias("dt_entrada_uti"),
        _data("DT_SAIDUTI").alias("dt_saida_uti"),
        _mapear("SUPORT_VEN", SUPORTE_VENTILATORIO, "Ignorado").alias("suporte_ventilatorio"),
        _mapear("VACINA_COV", SIM_NAO, "Ignorado").alias("vacinado_covid"),
        _mapear("VACINA", SIM_NAO, "Ignorado").alias("vacinado_gripe"),
        _mapear("CLASSI_FIN", CLASSIFICACAO, "Em investigação").alias("classificacao_final"),
        _mapear("EVOLUCAO", EVOLUCAO, "Sem desfecho informado").alias("evolucao"),
        _data("DT_EVOLUCA").alias("dt_evolucao"),
        _data("DT_ENCERRA").alias("dt_encerramento"),
        _data("DT_DIGITA").alias("dt_digitacao"),
        F.col("_arquivo").alias("arquivo_origem"),
    )
    # Deduplicação: a mesma notificação pode aparecer mais de uma vez; fica a digitação mais recente
    janela = Window.partitionBy("ano", "notificacao_id").orderBy(F.col("dt_digitacao").desc_nulls_last())
    return (silver.withColumn("_ordem", F.row_number().over(janela))
                  .filter("_ordem = 1").drop("_ordem"))


# ---------------------------------------------------------------- qualidade
def verificar_silver(bronze: DataFrame, silver: DataFrame, ano: int):
    """Regras de qualidade: críticas bloqueiam a publicação; alertas apenas registram."""
    b = bronze.agg(F.count("*").alias("linhas"),
                   F.sum(F.when(F.nullif(F.trim("NU_NOTIFIC"), F.lit("")).isNull(), 1).otherwise(0)).alias("sem_id")
                   ).first()
    s = silver.agg(
        F.count("*").alias("linhas"),
        F.sum(F.when(F.col("uf_residencia").isNull() | (F.col("uf_residencia") == ""), 1).otherwise(0)).alias("sem_uf"),
        F.sum(F.when(F.col("faixa_etaria") == "não informado", 1).otherwise(0)).alias("sem_idade"),
        F.sum(F.when(F.col("dt_primeiros_sintomas") > F.col("dt_notificacao"), 1).otherwise(0)).alias("sintoma_apos_notif"),
        F.sum(F.when(F.col("dt_primeiros_sintomas").isNull(), 1).otherwise(0)).alias("sem_dt_sintomas"),
        F.sum(F.when(F.year("dt_primeiros_sintomas").between(ano - 1, ano), 0).otherwise(1)).alias("sintoma_fora_ano"),
    ).first()
    pct = lambda parte, total: round(100.0 * (parte or 0) / total, 3) if total else 0.0  # noqa: E731
    return [
        # (regra, severidade, valor, limite, aprovado)
        ("bronze: arquivo com registros", "critica", b.linhas, 1, b.linhas >= 1),
        ("bronze: % de notificações sem número", "critica", pct(b.sem_id, b.linhas), 1.0, pct(b.sem_id, b.linhas) <= 1.0),
        ("silver: % de duplicidades removidas", "alerta", pct(b.linhas - s.linhas, b.linhas), 2.0, pct(b.linhas - s.linhas, b.linhas) <= 2.0),
        ("silver: % sem UF de residência", "alerta", pct(s.sem_uf, s.linhas), 5.0, pct(s.sem_uf, s.linhas) <= 5.0),
        ("silver: % sem idade válida", "alerta", pct(s.sem_idade, s.linhas), 5.0, pct(s.sem_idade, s.linhas) <= 5.0),
        ("silver: % sem data de primeiros sintomas", "critica", pct(s.sem_dt_sintomas, s.linhas), 5.0, pct(s.sem_dt_sintomas, s.linhas) <= 5.0),
        ("silver: % sintomas após a notificação", "alerta", pct(s.sintoma_apos_notif, s.linhas), 1.0, pct(s.sintoma_apos_notif, s.linhas) <= 1.0),
        ("silver: % sintomas fora do ano da base", "alerta", pct(s.sintoma_fora_ano, s.linhas), 5.0, pct(s.sintoma_fora_ano, s.linhas) <= 5.0),
    ]


# ---------------------------------------------------------------- silver -> gold
def para_fato_semanal(silver: DataFrame) -> DataFrame:
    """Fato agregado por semana epidemiológica de início dos sintomas e perfil do caso."""
    sim = lambda c: F.sum(F.when(F.col(c) == "Sim", 1).otherwise(0))  # noqa: E731
    return (silver.groupBy(
                "ano", F.col("semana_sintomas").alias("semana_epidemiologica"),
                "uf_residencia", "municipio_residencia_ibge6",
                "faixa_etaria", "sexo", "classificacao_final", "evolucao")
            .agg(F.count("*").alias("casos"),
                 sim("hospitalizado").alias("hospitalizados"),
                 sim("internado_uti").alias("internados_uti"),
                 F.sum(F.when(F.col("suporte_ventilatorio") == "Invasivo", 1).otherwise(0)).alias("ventilacao_invasiva"),
                 F.sum(F.when(F.col("evolucao") == "Óbito", 1).otherwise(0)).alias("obitos"),
                 sim("vacinado_covid").alias("vacinados_covid"))
            .withColumn("atualizado_em", F.current_timestamp()))
