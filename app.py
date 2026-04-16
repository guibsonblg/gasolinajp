from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import time

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_FILE = PROJECT_ROOT / "data" / "processed" / "joao_pessoa_combustiveis.csv"
CSS_FILE = PROJECT_ROOT / "assets" / "app.css"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gasolina_jp.pipeline import update_dataset
from gasolina_jp.ui_logic import apply_bairro_actions, apply_filters, initialize_bairros_state, resolve_selected_fuel

st.set_page_config(page_title="Combustivel Barato JP", page_icon="⛽", layout="wide")

css = CSS_FILE.read_text(encoding="utf-8")

st.markdown(
    f"<style>{css}</style>",
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
        <h1>Combustível mais barato em João Pessoa</h1>
        <p>Veja os menores preços por bairro com base nos dados oficiais semanais da ANP</p>
        <div class="chip-row">
            <span class="chip">Gasolina comum</span>
            <span class="chip">Gasolina aditivada</span>
            <span class="chip">Etanol</span>
            <span class="chip">Diesel comum</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False, ttl=3600)
def load_processed_data() -> pd.DataFrame:
    # Evita reprocessar a cada clique e deixa a interface mais responsiva.
    if PROCESSED_FILE.exists():
        return pd.read_csv(PROCESSED_FILE)
    return pd.DataFrame(columns=["bairro", "combustivel", "preco", "revenda", "endereco", "data_coleta"])


def refresh_data() -> None:
    update_dataset(raw_dir=RAW_DIR, processed_file=PROCESSED_FILE, limit=3)


with st.sidebar:
    st.subheader("Atualização")
    if st.button("Atualizar dados agora", width="stretch"):
        feedback = st.empty()
        try:
            with st.spinner("Baixando e processando ANP..."):
                refresh_data()
            feedback.success("Dados atualizados.")
            st.cache_data.clear()
        except Exception as exc:
            feedback.error(
                "Falha ao atualizar os dados. Se necessário, configure ANP_SOURCE_URL com um XLSX/CSV/ZIP direto da ANP.\n\n"
                + str(exc)
            )
        time.sleep(2)
        feedback.empty()

    if PROCESSED_FILE.exists():
        modified_at = datetime.fromtimestamp(PROCESSED_FILE.stat().st_mtime)
        st.caption(f"Última atualização: {modified_at:%d/%m/%Y %H:%M}")

    st.divider()
    st.caption("Fonte oficial: ANP (SHP C)")
    st.caption("Cobertura: João Pessoa/PB")


df = load_processed_data()

if df.empty:
    # Primeira execucao: sem base processada, pedimos uma atualizacao manual inicial.
    st.warning("Nenhum dado processado ainda. Use 'Atualizar dados agora' na barra lateral.")
    st.stop()

all_fuels = sorted(df["combustivel"].dropna().unique().tolist())
all_bairros = sorted(df["bairro"].dropna().unique().tolist())

# Inicializa o estado dos bairros selecionados na primeira execução.
initialize_bairros_state(st.session_state, all_bairros)

fuel_labels = {
    "gasolina comum": "Gasolina comum",
    "gasolina aditivada": "Gasolina aditivada",
    "etanol": "Etanol",
    "diesel comum": "Diesel comum",
}
fuel_options = ["Todos"] + [fuel_labels.get(f, f.title()) for f in all_fuels]
fuel_reverse_map = {fuel_labels.get(f, f.title()): f for f in all_fuels}

st.markdown('<div class="section-title">Filtros</div>', unsafe_allow_html=True)

col1, col2, col3 = st.columns([1.2, 1.4, 1])
with col1:
    selected_fuel_label = st.radio(
        "Tipo de combustível",
        options=fuel_options,
        horizontal=False,
    )
with col2:
    st.markdown("**Bairros**")
    bairros_col1, bairros_col2 = st.columns(2)
    with bairros_col1:
        select_all_bairros = st.button("Selecionar todos", width="stretch")
    with bairros_col2:
        clear_bairros = st.button("Limpar", width="stretch")

    apply_bairro_actions(
        st.session_state,
        all_bairros,
        select_all=select_all_bairros,
        clear=clear_bairros,
    )

    selected_bairro = st.multiselect(
        "Selecionar bairros",
        all_bairros,
        key="bairros_selecionados",
        placeholder="Escolha bairros",
        label_visibility="collapsed",
    )
with col3:
    search_text = st.text_input("Buscar posto ou endereço", value="", placeholder="Ex.: avenida epitácio...")

if selected_fuel_label == "Todos":
    selected_fuel = all_fuels
else:
    selected_fuel = resolve_selected_fuel(selected_fuel_label, fuel_reverse_map, all_fuels)

filtered = apply_filters(df, selected_fuel, selected_bairro, search_text)

metric1, metric2, metric3 = st.columns(3)
metric1.metric("Registros exibidos", len(filtered))
metric2.metric("Bairros encontrados", filtered["bairro"].nunique())
metric3.metric("Combustíveis", filtered["combustivel"].nunique())

if filtered.empty:
    st.warning("Nenhum resultado com os filtros atuais. Tente limpar a busca ou selecionar mais bairros/combustíveis.")
    st.stop()

st.markdown('<div class="section-title">Destaques rápidos</div>', unsafe_allow_html=True)
destaques = filtered.sort_values(["combustivel", "preco"]).groupby("combustivel", as_index=False).head(1)
destaques["revenda"] = destaques["revenda"].fillna("Não informado").replace("", "Não informado")
destaques = destaques[["combustivel", "bairro", "preco", "revenda"]].copy()
destaques = destaques.rename(
    columns={
        "combustivel": "Combustível",
        "bairro": "Bairro",
        "preco": "Menor preço",
        "revenda": "Posto",
    }
)
st.dataframe(
    destaques,
    width="stretch",
    hide_index=True,
    column_config={
        "Menor preço": st.column_config.NumberColumn("Menor preço", format="R$ %.2f"),
    },
)

st.markdown('<div class="section-title">Tabela completa</div>', unsafe_allow_html=True)
st.markdown('<div class="small-note">Cada linha representa uma opção de menor preço por bairro e combustível.</div>', unsafe_allow_html=True)

display_df = filtered[["combustivel", "bairro", "preco", "revenda", "endereco", "data_coleta"]].copy()
display_df["revenda"] = display_df["revenda"].fillna("Não informado").replace("", "Não informado")
display_df = display_df.rename(
    columns={
        "combustivel": "Combustível",
        "bairro": "Bairro",
        "preco": "Preço",
        "revenda": "Posto",
        "endereco": "Endereço",
        "data_coleta": "Data da coleta",
    }
)

st.dataframe(
    display_df,
    width="stretch",
    hide_index=True,
    column_config={
        "Preço": st.column_config.NumberColumn("Preço", format="R$ %.2f"),
    },
)

st.download_button(
    "Baixar resultados filtrados (CSV)",
    data=display_df.to_csv(index=False).encode("utf-8"),
    file_name="combustivel-jp-filtrado.csv",
    mime="text/csv",
    width="stretch",
)
