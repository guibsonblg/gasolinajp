from __future__ import annotations

from collections.abc import MutableMapping

import pandas as pd


def initialize_bairros_state(
    session_state: MutableMapping[str, object],
    all_bairros: list[str],
    key: str = "bairros_selecionados",
) -> list[str]:
    current = session_state.get(key)
    if not isinstance(current, list):
        session_state[key] = all_bairros[:]
        return session_state[key]

    if any(bairro not in all_bairros for bairro in current):
        session_state[key] = all_bairros[:]

    return session_state[key]


def apply_bairro_actions(
    session_state: MutableMapping[str, object],
    all_bairros: list[str],
    *,
    select_all: bool,
    clear: bool,
    key: str = "bairros_selecionados",
) -> list[str]:
    initialize_bairros_state(session_state, all_bairros, key=key)

    if select_all:
        session_state[key] = all_bairros[:]
    elif clear:
        session_state[key] = []

    return session_state[key]


def resolve_selected_fuel(selected_label: str, fuel_reverse_map: dict[str, str], all_fuels: list[str]) -> list[str]:
    if selected_label == "Todos":
        return all_fuels
    return [fuel_reverse_map[selected_label]] if selected_label in fuel_reverse_map else all_fuels


def apply_filters(
    df: pd.DataFrame,
    selected_fuel: list[str],
    selected_bairro: list[str],
    search_text: str,
) -> pd.DataFrame:
    filtered = df[df["combustivel"].isin(selected_fuel) & df["bairro"].isin(selected_bairro)].copy()

    query = search_text.strip().lower()
    if query:
        revenda_match = filtered["revenda"].fillna("").astype(str).str.lower().str.contains(query, regex=False)
        endereco_match = filtered["endereco"].fillna("").astype(str).str.lower().str.contains(query, regex=False)
        filtered = filtered[revenda_match | endereco_match]

    return filtered.sort_values(["combustivel", "bairro", "preco"], ascending=[True, True, True])