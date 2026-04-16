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


def _normalize_text(value: object) -> str:
    # Normaliza textos para comparacao segura (sem acento e em minusculo).
    texto = "" if value is None else str(value).strip()
    if not texto:
        return ""
    texto_normalizado = unicodedata.normalize("NFKD", texto)
    return "".join(char for char in texto_normalizado if not unicodedata.combining(char)).lower()


def _request(url: str, timeout: int = 90, session: requests.Session | None = None) -> requests.Response:
    # Centraliza requisicoes HTTP com timeout e User-Agent padronizados.
    sessao = session or requests.Session()
    resposta = sessao.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    resposta.raise_for_status()
    return resposta


def _extract_revendas_links(page_html: str) -> list[str]:
    # Extrai links da pagina da ANP que apontam para planilhas semanais revendas_lpc.
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', page_html, flags=re.IGNORECASE)
    links: list[str] = []
    for href in hrefs:
        normalized = html.unescape(href).strip()
        lower = normalized.lower()
        if "revendas_lpc" in lower and lower.endswith(".xlsx"):
            links.append(urljoin(ANP_WEEKLY_PAGE_URL, normalized))
    return links


def resolve_latest_anp_file_url(session: requests.Session | None = None) -> str:
    # Resolve o arquivo semanal mais recente direto da pagina oficial da ANP.
    resposta = _request(ANP_WEEKLY_PAGE_URL, timeout=45, session=session)
    links = _extract_revendas_links(resposta.text)
    if not links:
        raise RuntimeError("Nao foi possivel encontrar links semanais da ANP na pagina oficial.")
    return links[0]


def _load_anp_xlsx(content: bytes) -> pd.DataFrame:
    # Procura automaticamente a linha de cabecalho util e le a planilha com pandas.
    preview = pd.read_excel(io.BytesIO(content), header=None, engine="openpyxl")

    for row_index in range(min(20, len(preview))):
        values = {_normalize_text(value) for value in preview.iloc[row_index].tolist() if pd.notna(value)}
        if {"municipio", "produto", "preco de revenda"}.issubset(values):
            return pd.read_excel(io.BytesIO(content), header=row_index, engine="openpyxl")

    raise RuntimeError("Nao foi possivel localizar o cabecalho util na planilha da ANP.")


def _load_table_from_bytes(content: bytes, file_url: str) -> pd.DataFrame:
    # Carrega tabela de acordo com o tipo de arquivo recebido (xlsx, zip ou csv).
    lower = file_url.lower()
    if lower.endswith(".xlsx"):
        return _load_anp_xlsx(content)
    if lower.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_files = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            if not csv_files:
                raise RuntimeError("Arquivo ZIP nao contem CSV.")
            with zf.open(csv_files[0]) as csv_file:
                return pd.read_csv(csv_file, sep=None, engine="python", encoding="latin-1")
    return pd.read_csv(io.BytesIO(content), sep=None, engine="python", encoding="latin-1")


def download_anp_dataframe(raw_dir: Path, source_url: str | None = None) -> tuple[pd.DataFrame, str, Path]:
    # Baixa o arquivo da ANP, converte para DataFrame e salva o bruto localmente.
    raw_dir.mkdir(parents=True, exist_ok=True)

    file_url = source_url or os.getenv("ANP_SOURCE_URL") or resolve_latest_anp_file_url()
    resposta = _request(file_url)
    raw_bytes = resposta.content
    df = _load_table_from_bytes(raw_bytes, file_url)

    suffix = Path(file_url).suffix.lower() or ".csv"
    raw_file = raw_dir / f"anp_latest_raw{suffix}"
    if suffix == ".xlsx":
        raw_file.write_bytes(raw_bytes)
    else:
        df.to_csv(raw_file, index=False)

    return df, file_url, raw_file


def _resolve_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    # Resolve o nome real da coluna usando aliases comuns da ANP.
    colunas_canonicas = {_normalize_text(col).replace("_", " "): col for col in df.columns}
    for alias in aliases:
        key = _normalize_text(alias).replace("_", " ")
        if key in colunas_canonicas:
            return colunas_canonicas[key]
    return None


def _build_normalized_frame(df: pd.DataFrame) -> pd.DataFrame:
    # Padroniza as colunas essenciais e aplica o recorte de Joao Pessoa/PB.
    estado_col = _resolve_column(df, ["estado", "uf"])
    municipio_col = _resolve_column(df, ["municipio", "município"])
    bairro_col = _resolve_column(df, ["bairro"])
    combustivel_col = _resolve_column(df, ["produto", "combustivel", "combustível"])
    preco_col = _resolve_column(df, ["preco de revenda", "preco revenda", "preco", "preço", "valor de venda"])
    revenda_col = _resolve_column(df, ["revenda", "posto"])
    fantasia_col = _resolve_column(df, ["fantasia", "nome fantasia"])
    razao_col = _resolve_column(df, ["razao", "razão", "razao social", "razão social"])
    endereco_col = _resolve_column(df, ["endereco", "endereço", "logradouro"])
    data_col = _resolve_column(df, ["data da coleta", "data de coleta", "data", "data_coleta"])

    if municipio_col is None or combustivel_col is None or preco_col is None:
        raise RuntimeError("Colunas essenciais ausentes no arquivo ANP.")

    # Prioriza revenda; quando nao existir, usa fantasia e razao como fallback.
    if revenda_col:
        revenda_series = df[revenda_col]
    elif fantasia_col and razao_col:
        revenda_series = df[fantasia_col].replace("", pd.NA).fillna(df[razao_col])
    elif fantasia_col:
        revenda_series = df[fantasia_col]
    elif razao_col:
        revenda_series = df[razao_col]
    else:
        revenda_series = ""

    base = pd.DataFrame(
        {
            "estado": df[estado_col] if estado_col else "",
            "municipio": df[municipio_col],
            "bairro": df[bairro_col] if bairro_col else "Nao informado",
            "combustivel": df[combustivel_col],
            "preco": df[preco_col],
            "revenda": revenda_series,
            "endereco": df[endereco_col] if endereco_col else "",
            "data_coleta": df[data_col] if data_col else "",
        }
    )

    filtro_cidade = base["municipio"].map(_normalize_text).eq("joao pessoa")
    filtro_estado = base["estado"].map(_normalize_text).isin({"pb", "paraiba"}) if estado_col else True
    filtrado = base[filtro_cidade & filtro_estado].copy()

    # Normaliza bairro com fallback simples.
    filtrado["bairro"] = filtrado["bairro"].fillna("Nao informado").astype(str).str.strip().replace("", "Nao informado")

    # Usa pandas vetorizado para padronizar combustivel.
    combustivel_texto = filtrado["combustivel"].fillna("").astype(str).map(_normalize_text)
    combustivel_padrao = pd.Series("outro", index=filtrado.index)
    combustivel_padrao.loc[combustivel_texto.str.contains("diesel", regex=False)] = "diesel comum"
    combustivel_padrao.loc[combustivel_texto.str.contains("etanol", regex=False)] = "etanol"
    combustivel_padrao.loc[combustivel_texto.str.contains("gasolina", regex=False)] = "gasolina comum"
    combustivel_padrao.loc[
        combustivel_texto.str.contains("gasolina", regex=False)
        & combustivel_texto.str.contains("aditiv", regex=False)
    ] = "gasolina aditivada"
    filtrado["combustivel"] = combustivel_padrao

    # Trata preco em lote: primeiro tenta numerico puro, depois formato brasileiro com virgula.
    preco_numerico = pd.to_numeric(filtrado["preco"], errors="coerce")
    preco_texto = (
        filtrado["preco"]
        .astype(str)
        .str.replace("R$", "", regex=False)
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    preco_convertido = pd.to_numeric(preco_texto, errors="coerce")
    filtrado["preco"] = preco_numerico.where(preco_numerico.notna(), preco_convertido)

    filtrado = filtrado[filtrado["combustivel"].isin(TARGET_FUELS)]
    return filtrado.dropna(subset=["preco"])


def _rank_top_by_bairro_and_fuel(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    # Ordena por menor preco e pega os primeiros por bairro + combustivel.
    ordered = df.sort_values(["bairro", "combustivel", "preco", "revenda", "endereco"])
    return ordered.groupby(["bairro", "combustivel"], as_index=False).head(limit).reset_index(drop=True)


def prepare_top3_joao_pessoa(df: pd.DataFrame, limit: int = 3) -> pd.DataFrame:
    # Funcao principal de transformacao para gerar o top por bairro/combustivel.
    if df.empty:
        return pd.DataFrame(columns=["bairro", "combustivel", "preco", "revenda", "endereco", "data_coleta"])

    base_padronizada = _build_normalized_frame(df)
    return _rank_top_by_bairro_and_fuel(base_padronizada, limit=limit)


def update_dataset(raw_dir: Path, processed_file: Path, source_url: str | None = None, limit: int = 3) -> dict[str, object]:
    # Orquestra o fluxo completo: download, processamento e persistencia do CSV final.
    raw_df, resolved_url, raw_file = download_anp_dataframe(raw_dir=raw_dir, source_url=source_url)
    top3_df = prepare_top3_joao_pessoa(raw_df, limit=limit)

    processed_file.parent.mkdir(parents=True, exist_ok=True)
    top3_df.to_csv(processed_file, index=False)

    return {
        "source_url": resolved_url,
        "raw_rows": len(raw_df),
        "result_rows": len(top3_df),
        "raw_file": raw_file,
        "processed_file": processed_file,
    }
