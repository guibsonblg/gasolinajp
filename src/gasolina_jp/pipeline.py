from __future__ import annotations

import html
import io
import os
import re
import unicodedata
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

ANP_WEEKLY_PAGE_URL = (
    "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/"
    "precos/levantamento-de-precos-de-combustiveis-ultimas-semanas-pesquisadas"
)
USER_AGENT = "gasolina-jp/1.0 (+https://github.com/)"
TARGET_FUELS = {"gasolina comum", "gasolina aditivada", "etanol", "diesel comum"}


def _normalizar(texto: object) -> str:
    # Remove acentos e coloca em minusculo para comparacoes seguras.
    t = "" if texto is None else str(texto).strip()
    sem_acento = unicodedata.normalize("NFKD", t)
    return "".join(c for c in sem_acento if not unicodedata.combining(c)).lower()


def _extract_revendas_links(page_html: str) -> list[str]:
    # Varre o HTML da pagina ANP e coleta links de planilhas semanais (revendas_lpc*.xlsx).
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', page_html, flags=re.IGNORECASE)
    links = []
    for href in hrefs:
        normalizado = html.unescape(href).strip()
        if "revendas_lpc" in normalizado.lower() and normalizado.lower().endswith(".xlsx"):
            links.append(urljoin(ANP_WEEKLY_PAGE_URL, normalizado))
    return links


def resolve_latest_anp_file_url(session: requests.Session | None = None) -> str:
    # Acessa a pagina oficial da ANP e retorna o link do arquivo semanal mais recente.
    sessao = session or requests.Session()
    resposta = sessao.get(ANP_WEEKLY_PAGE_URL, timeout=45, headers={"User-Agent": USER_AGENT})
    resposta.raise_for_status()
    links = _extract_revendas_links(resposta.text)
    if not links:
        raise RuntimeError("Nenhum arquivo semanal encontrado na pagina da ANP.")
    return links[0]


def prepare_top3_joao_pessoa(df: pd.DataFrame, limit: int = 3) -> pd.DataFrame:
    """Recebe o DataFrame bruto da ANP e retorna os menores precos por bairro e combustivel em Joao Pessoa."""
    if df.empty:
        return pd.DataFrame(columns=["bairro", "combustivel", "preco", "revenda", "endereco", "data_coleta"])

    # Cria um mapa de nome-normalizado -> nome-real para encontrar colunas sem depender de acento ou case.
    col_map = {_normalizar(col).replace("_", " "): col for col in df.columns}

    def achar_col(*aliases):
        # Retorna o nome real da coluna a partir de possiveis aliases.
        for alias in aliases:
            chave = _normalizar(alias).replace("_", " ")
            if chave in col_map:
                return col_map[chave]
        return None

    municipio_col = achar_col("municipio", "município")
    estado_col    = achar_col("estado", "uf")
    bairro_col    = achar_col("bairro")
    produto_col   = achar_col("produto", "combustivel", "combustível")
    preco_col     = achar_col("preco de revenda", "preco revenda", "preco", "preço", "valor de venda")
    revenda_col   = achar_col("revenda", "posto")
    fantasia_col  = achar_col("fantasia", "nome fantasia")
    razao_col     = achar_col("razao", "razão", "razao social", "razão social")
    endereco_col  = achar_col("endereco", "endereço", "logradouro")
    data_col      = achar_col("data da coleta", "data de coleta", "data", "data_coleta")

    if municipio_col is None or produto_col is None or preco_col is None:
        raise RuntimeError("Colunas essenciais ausentes no arquivo ANP (municipio, produto, preco).")

    # Nome do posto: tenta revenda, cai para fantasia, cai para razao social.
    colunas_nome = [c for c in [revenda_col, fantasia_col, razao_col] if c]
    nome_posto = (
        df[colunas_nome].replace("", pd.NA).bfill(axis=1).iloc[:, 0]
        if colunas_nome else pd.Series("", index=df.index)
    )

    # Monta DataFrame com as colunas que interessam, ja com nomes padronizados.
    dados = pd.DataFrame({
        "municipio":   df[municipio_col],
        "estado":      df[estado_col]   if estado_col   else "",
        "bairro":      df[bairro_col]   if bairro_col   else "Nao informado",
        "combustivel": df[produto_col],
        "preco":       df[preco_col],
        "revenda":     nome_posto,
        "endereco":    df[endereco_col] if endereco_col else "",
        "data_coleta": df[data_col]     if data_col     else "",
    })

    # Filtra somente registros de Joao Pessoa/PB.
    filtro_cidade = dados["municipio"].map(_normalizar).eq("joao pessoa")
    filtro_estado = dados["estado"].map(_normalizar).isin({"pb", "paraiba"}) if estado_col else True
    dados = dados[filtro_cidade & filtro_estado].copy()

    # Bairro: remove nulos e strings vazias.
    dados["bairro"] = dados["bairro"].fillna("Nao informado").astype(str).str.strip().replace("", "Nao informado")

    # Classifica combustivel por palavras-chave (ordem importa: aditivada sobrescreve comum).
    texto_comb = dados["combustivel"].fillna("").astype(str).map(_normalizar)
    dados["combustivel"] = "outro"
    dados.loc[texto_comb.str.contains("diesel",   regex=False), "combustivel"] = "diesel comum"
    dados.loc[texto_comb.str.contains("etanol",   regex=False), "combustivel"] = "etanol"
    dados.loc[texto_comb.str.contains("gasolina", regex=False), "combustivel"] = "gasolina comum"
    dados.loc[
        texto_comb.str.contains("gasolina", regex=False) & texto_comb.str.contains("aditiv", regex=False),
        "combustivel",
    ] = "gasolina aditivada"

    # Converte preco: aceita numero puro ou formato brasileiro com virgula (ex: "4,759").
    preco_puro = pd.to_numeric(dados["preco"], errors="coerce")
    preco_br = pd.to_numeric(
        dados["preco"].astype(str)
            .str.replace("R$", "", regex=False).str.strip()
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False),
        errors="coerce",
    )
    dados["preco"] = preco_puro.where(preco_puro.notna(), preco_br)

    # Remove combustiveis nao reconhecidos e precos invalidos.
    dados = dados[dados["combustivel"].isin(TARGET_FUELS)].dropna(subset=["preco"])

    # Ordena por menor preco e retorna os primeiros por bairro + combustivel.
    return (
        dados.sort_values(["bairro", "combustivel", "preco", "revenda", "endereco"])
             .groupby(["bairro", "combustivel"], as_index=False)
             .head(limit)
             .reset_index(drop=True)
    )


def update_dataset(
    raw_dir: Path,
    processed_file: Path,
    source_url: str | None = None,
    limit: int = 3,
) -> dict[str, object]:
    """Fluxo completo: descobre o arquivo da semana, baixa, processa e salva o CSV final."""
    raw_dir.mkdir(parents=True, exist_ok=True)

    file_url = source_url or os.getenv("ANP_SOURCE_URL") or resolve_latest_anp_file_url()

    # Baixa o arquivo da ANP.
    resposta = requests.get(file_url, timeout=90, headers={"User-Agent": USER_AGENT})
    resposta.raise_for_status()
    conteudo = resposta.content

    # Carrega o conteudo como DataFrame de acordo com o formato do arquivo.
    if file_url.lower().endswith(".xlsx"):
        # A planilha da ANP tem linhas institucionais no topo; precisa encontrar o cabecalho real.
        preview = pd.read_excel(io.BytesIO(conteudo), header=None, engine="openpyxl")
        linha_cabecalho = next(
            (
                i for i in range(min(20, len(preview)))
                if {"municipio", "produto", "preco de revenda"}.issubset(
                    {_normalizar(v) for v in preview.iloc[i] if pd.notna(v)}
                )
            ),
            None,
        )
        if linha_cabecalho is None:
            raise RuntimeError("Cabecalho da planilha ANP nao encontrado nas primeiras 20 linhas.")
        df = pd.read_excel(io.BytesIO(conteudo), header=linha_cabecalho, engine="openpyxl")

    elif file_url.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError("Arquivo ZIP nao contem CSV.")
            with zf.open(csv_names[0]) as csv_file:
                df = pd.read_csv(csv_file, sep=None, engine="python", encoding="latin-1")

    else:
        df = pd.read_csv(io.BytesIO(conteudo), sep=None, engine="python", encoding="latin-1")

    # Salva o arquivo bruto localmente.
    sufixo = Path(file_url).suffix.lower() or ".csv"
    raw_file = raw_dir / f"anp_latest_raw{sufixo}"
    if sufixo == ".xlsx":
        raw_file.write_bytes(conteudo)
    else:
        df.to_csv(raw_file, index=False)

    # Processa e salva o resultado final.
    resultado = prepare_top3_joao_pessoa(df, limit=limit)
    processed_file.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(processed_file, index=False)

    return {
        "source_url": file_url,
        "raw_rows": len(df),
        "result_rows": len(resultado),
        "raw_file": raw_file,
        "processed_file": processed_file,
    }
