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
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char)).lower()


def _request(url: str, timeout: int = 90, session: requests.Session | None = None) -> requests.Response:
    s = session or requests.Session()
    response = s.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response


def _extract_revendas_links(page_html: str) -> list[str]:
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', page_html, flags=re.IGNORECASE)
    links: list[str] = []
    for href in hrefs:
        normalized = html.unescape(href).strip()
        lower = normalized.lower()
        if "revendas_lpc" in lower and lower.endswith(".xlsx"):
            links.append(urljoin(ANP_WEEKLY_PAGE_URL, normalized))
    return links


def resolve_latest_anp_file_url(session: requests.Session | None = None) -> str:
    response = _request(ANP_WEEKLY_PAGE_URL, timeout=45, session=session)
    links = _extract_revendas_links(response.text)
    if not links:
        raise RuntimeError("Nao foi possivel encontrar links semanais da ANP na pagina oficial.")
    return links[0]


def _load_anp_xlsx(content: bytes) -> pd.DataFrame:
    preview = pd.read_excel(io.BytesIO(content), header=None, engine="openpyxl")

    for row_index in range(min(20, len(preview))):
        values = {_normalize_text(value) for value in preview.iloc[row_index].tolist() if pd.notna(value)}
        if {"municipio", "produto", "preco de revenda"}.issubset(values):
            return pd.read_excel(io.BytesIO(content), header=row_index, engine="openpyxl")

    raise RuntimeError("Nao foi possivel localizar o cabecalho util na planilha da ANP.")


def _load_table_from_bytes(content: bytes, file_url: str) -> pd.DataFrame:
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
    raw_dir.mkdir(parents=True, exist_ok=True)

    file_url = source_url or os.getenv("ANP_SOURCE_URL") or resolve_latest_anp_file_url()
    response = _request(file_url)
    raw_bytes = response.content
    df = _load_table_from_bytes(raw_bytes, file_url)

    suffix = Path(file_url).suffix.lower() or ".csv"
    raw_file = raw_dir / f"anp_latest_raw{suffix}"
    if suffix == ".xlsx":
        raw_file.write_bytes(raw_bytes)
    else:
        df.to_csv(raw_file, index=False)

    return df, file_url, raw_file


def _resolve_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    by_canonical = {_normalize_text(col).replace("_", " "): col for col in df.columns}
    for alias in aliases:
        key = _normalize_text(alias).replace("_", " ")
        if key in by_canonical:
            return by_canonical[key]
    return None


def _normalize_fuel(value: object) -> str:
    text = _normalize_text(value)
    if "gasolina" in text and "aditiv" in text:
        return "gasolina aditivada"
    if "gasolina" in text:
        return "gasolina comum"
    if "etanol" in text:
        return "etanol"
    if "diesel" in text:
        return "diesel comum"
    return "outro"


def _parse_price(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("R$", "")
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _build_normalized_frame(df: pd.DataFrame) -> pd.DataFrame:
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

    work = pd.DataFrame(
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

    city_mask = work["municipio"].map(_normalize_text).eq("joao pessoa")
    state_mask = work["estado"].map(_normalize_text).isin({"pb", "paraiba"}) if estado_col else True
    filtered = work[city_mask & state_mask].copy()

    filtered["bairro"] = filtered["bairro"].fillna("Nao informado").astype(str).str.strip().replace("", "Nao informado")
    filtered["combustivel"] = filtered["combustivel"].map(_normalize_fuel)
    filtered["preco"] = filtered["preco"].map(_parse_price)

    filtered = filtered[filtered["combustivel"].isin(TARGET_FUELS)]
    return filtered.dropna(subset=["preco"])


def _rank_top_by_bairro_and_fuel(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    ordered = df.sort_values(["bairro", "combustivel", "preco", "revenda", "endereco"])
    return ordered.groupby(["bairro", "combustivel"], as_index=False).head(limit).reset_index(drop=True)


def prepare_top3_joao_pessoa(df: pd.DataFrame, limit: int = 3) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["bairro", "combustivel", "preco", "revenda", "endereco", "data_coleta"])

    normalized = _build_normalized_frame(df)
    return _rank_top_by_bairro_and_fuel(normalized, limit=limit)


def update_dataset(raw_dir: Path, processed_file: Path, source_url: str | None = None, limit: int = 3) -> dict[str, object]:
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
