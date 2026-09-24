"""
Técnicas de mascaramento e anonimização de dados pessoais (LGPD).

Cada técnica tem um propósito diferente:

| Técnica            | Reversível? | Permite join? | Uso típico                          |
|--------------------|-------------|---------------|-------------------------------------|
| Pseudonimização    | Não*        | Sim           | Chave técnica na silver             |
| Mascaramento       | Não         | Não           | Exibição parcial (atendimento, BI)  |
| Generalização      | Não         | Não           | Análises estatísticas na gold       |
| Supressão          | Não         | Não           | Remover o que não é necessário      |

* HMAC com chave secreta: sem a chave, não é possível recalcular nem
  testar CPFs por força bruta; com a chave (restrita), o mesmo CPF gera
  sempre o mesmo pseudônimo, permitindo cruzar bases sem expor o dado.

As funções "puras" (sufixo _py) contêm a regra e são testáveis sem Spark;
as versões para DataFrame são aplicadas coluna a coluna.
"""
import hashlib
import hmac
import os
import re
from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

_SO_DIGITOS = re.compile(r"\D")


# ---------------------------------------------------------------------------
# Regras em Python puro
# ---------------------------------------------------------------------------
def pseudonimizar_py(valor, chave):
    """HMAC-SHA256 do valor normalizado (apenas dígitos, se houver)."""
    if valor is None:
        return None
    normalizado = _SO_DIGITOS.sub("", valor) or valor.strip().lower()
    return hmac.new(chave.encode(), normalizado.encode(), hashlib.sha256).hexdigest()


def mascarar_cpf_py(cpf):
    """Padrão do Portal da Transparência: ***.456.789-**"""
    if cpf is None:
        return None
    d = _SO_DIGITOS.sub("", cpf)
    return f"***.{d[3:6]}.{d[6:9]}-**" if len(d) == 11 else None


def mascarar_nome_py(nome):
    """Mantém a inicial de cada parte: 'Maria Souza' -> 'M**** S****'"""
    if nome is None:
        return None
    return " ".join(p[0] + "*" * (len(p) - 1) for p in nome.split())


def mascarar_email_py(email):
    """'maria.souza@email.com' -> 'm**********@email.com'"""
    if email is None or "@" not in email:
        return None
    usuario, dominio = email.split("@", 1)
    return usuario[0] + "*" * (len(usuario) - 1) + "@" + dominio


def faixa_etaria_py(nascimento, referencia=None):
    """Generalização da idade em faixas de 10 anos (80+ agrupado)."""
    if nascimento is None:
        return "não informado"
    ref = referencia or date.today()
    idade = ref.year - nascimento.year - ((ref.month, ref.day) < (nascimento.month, nascimento.day))
    if idade >= 80:
        return "80+"
    inicio = (idade // 10) * 10
    return f"{inicio}-{inicio + 9}"


# ---------------------------------------------------------------------------
# Versões para DataFrame
# ---------------------------------------------------------------------------
def _chave_hmac():
    chave = os.environ.get("PII_HMAC_KEY")
    if not chave:
        raise RuntimeError("PII_HMAC_KEY não definida: a pseudonimização exige a chave secreta")
    return chave


def pseudonimizar(coluna: Column) -> Column:
    chave = _chave_hmac()
    udf = F.udf(lambda v: pseudonimizar_py(v, chave), StringType())
    return udf(coluna)


mascarar_nome = F.udf(mascarar_nome_py, StringType())
mascarar_email = F.udf(mascarar_email_py, StringType())


def mascarar_cpf(coluna: Column) -> Column:
    d = F.regexp_replace(coluna, r"\D", "")
    return F.when(F.length(d) == 11,
                  F.concat(F.lit("***."), F.substring(d, 4, 3), F.lit("."),
                           F.substring(d, 7, 3), F.lit("-**")))


def mascarar_telefone(coluna: Column) -> Column:
    """Mantém DDD e 4 últimos dígitos: '(11) *****-4321'"""
    d = F.regexp_replace(coluna, r"\D", "")
    return F.concat(F.lit("("), F.substring(d, 1, 2), F.lit(") *****-"), F.substring(d, -4, 4))


def generalizar_cep(coluna: Column) -> Column:
    return F.concat(F.substring(F.regexp_replace(coluna, r"\D", ""), 1, 5), F.lit("-***"))


def faixa_etaria(coluna_nascimento: Column) -> Column:
    idade = F.floor(F.months_between(F.current_date(), coluna_nascimento) / 12)
    inicio = (F.floor(idade / 10) * 10).cast("int")
    return (F.when(coluna_nascimento.isNull(), F.lit("não informado"))
             .when(idade >= 80, F.lit("80+"))
             .otherwise(F.concat(inicio.cast("string"), F.lit("-"), (inicio + 9).cast("string"))))


def verificar_k_anonimato(df: DataFrame, quase_identificadores, k=5) -> DataFrame:
    """
    Retorna as combinações de quase-identificadores com menos de k registros.
    Grupos pequenos permitem reidentificação (ex.: única paciente de 80+
    anos num CEP) e devem ser suprimidos ou generalizados antes de publicar.
    """
    return (df.groupBy(*quase_identificadores).count()
              .filter(F.col("count") < k)
              .orderBy("count"))
