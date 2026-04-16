from __future__ import annotations

import pandas as pd

from gasolina_jp.ui_logic import apply_bairro_actions, apply_filters, initialize_bairros_state, resolve_selected_fuel


def test_initialize_bairros_state_sets_all_on_first_run() -> None:
    state: dict[str, object] = {}
    all_bairros = ["Bancarios", "Manaira", "Tambaú"]

    selected = initialize_bairros_state(state, all_bairros)

    assert selected == all_bairros
    assert state["bairros_selecionados"] == all_bairros


def test_initialize_bairros_state_preserves_user_selection() -> None:
    state: dict[str, object] = {"bairros_selecionados": ["Manaira"]}
    all_bairros = ["Bancarios", "Manaira", "Tambaú"]

    selected = initialize_bairros_state(state, all_bairros)

    assert selected == ["Manaira"]


def test_apply_bairro_actions_supports_select_all_and_clear() -> None:
    state: dict[str, object] = {"bairros_selecionados": ["Manaira"]}
    all_bairros = ["Bancarios", "Manaira", "Tambaú"]

    selected_all = apply_bairro_actions(state, all_bairros, select_all=True, clear=False)
    assert selected_all == all_bairros

    selected_none = apply_bairro_actions(state, all_bairros, select_all=False, clear=True)
    assert selected_none == []


def test_resolve_selected_fuel_maps_labels_to_internal_values() -> None:
    all_fuels = ["gasolina comum", "etanol"]
    reverse_map = {"Gasolina comum": "gasolina comum", "Etanol": "etanol"}

    assert resolve_selected_fuel("Todos", reverse_map, all_fuels) == all_fuels
    assert resolve_selected_fuel("Etanol", reverse_map, all_fuels) == ["etanol"]


def test_apply_filters_handles_search_in_station_or_address() -> None:
    df = pd.DataFrame(
        {
            "combustivel": ["gasolina comum", "etanol"],
            "bairro": ["Manaira", "Tambaú"],
            "preco": [5.99, 4.49],
            "revenda": ["Posto Azul", "Posto Verde"],
            "endereco": ["Av Epitacio Pessoa", "Rua das Flores"],
        }
    )

    result_station = apply_filters(df, ["gasolina comum", "etanol"], ["Manaira", "Tambaú"], "azul")
    assert len(result_station) == 1
    assert result_station.iloc[0]["revenda"] == "Posto Azul"

    result_address = apply_filters(df, ["gasolina comum", "etanol"], ["Manaira", "Tambaú"], "flores")
    assert len(result_address) == 1
    assert result_address.iloc[0]["bairro"] == "Tambaú"
