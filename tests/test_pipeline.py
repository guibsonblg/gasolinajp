from __future__ import annotations

import pandas as pd

from gasolina_jp.pipeline import _extract_revendas_links, prepare_top3_joao_pessoa


def test_extract_revendas_links_returns_latest_first() -> None:
    page_html = """
    <html>
        <body>
            <a href="/anp/arquivos-lpc/2026/revendas_lpc_2026-04-05_2026-04-11.xlsx">ultima</a>
            <a href="/anp/arquivos-lpc/2026/revendas_lpc_2026-03-29_2026-04-04.xlsx">anterior</a>
            <a href="/anp/arquivos-lpc/2026/resumo_semanal_lpc_2026-04-05_2026-04-11.xlsx">resumo</a>
        </body>
    </html>
    """

    links = _extract_revendas_links(page_html)

    assert len(links) == 2
    assert links[0].endswith("revendas_lpc_2026-04-05_2026-04-11.xlsx")
    assert links[1].endswith("revendas_lpc_2026-03-29_2026-04-04.xlsx")


def test_prepare_top3_joao_pessoa_filters_and_ranks() -> None:
    raw = pd.DataFrame(
        {
            "Estado": ["PB", "PB", "PB", "PE", "PB"],
            "Municipio": ["Joao Pessoa", "Joao Pessoa", "Joao Pessoa", "Joao Pessoa", "Cabedelo"],
            "Bairro": ["Manaira", "Manaira", "Manaira", "Manaira", "Bessa"],
            "Produto": ["GASOLINA", "GASOLINA", "GASOLINA", "GASOLINA", "ETANOL"],
            "Preco de Revenda": ["5,90", "5,70", "5,80", "5,60", "4,20"],
            "Revenda": ["Posto C", "Posto A", "Posto B", "Posto PE", "Posto X"],
            "Endereco": ["Rua 3", "Rua 1", "Rua 2", "Rua PE", "Rua X"],
        }
    )

    result = prepare_top3_joao_pessoa(raw, limit=3)

    assert len(result) == 3
    assert result["preco"].tolist() == [5.7, 5.8, 5.9]
    assert result["bairro"].eq("Manaira").all()


def test_prepare_uses_fantasia_as_station_name_fallback() -> None:
    raw = pd.DataFrame(
        {
            "Estado": ["PB", "PB"],
            "Município": ["João Pessoa", "João Pessoa"],
            "Bairro": ["Tambaú", "Tambaú"],
            "Produto": ["GASOLINA", "ETANOL"],
            "Preço de Revenda": [6.0, 4.5],
            "FANTASIA": ["Posto A", "Posto B"],
            "RAZÃO": ["Razao A", "Razao B"],
            "ENDEREÇO": ["Av 1", "Av 2"],
        }
    )

    result = prepare_top3_joao_pessoa(raw, limit=3)

    assert len(result) == 2
    assert set(result["revenda"].tolist()) == {"Posto A", "Posto B"}


def test_prepare_drops_invalid_prices() -> None:
    raw = pd.DataFrame(
        {
            "Estado": ["PB", "PB"],
            "Municipio": ["Joao Pessoa", "Joao Pessoa"],
            "Bairro": ["Centro", "Centro"],
            "Produto": ["GASOLINA", "GASOLINA"],
            "Preco de Revenda": ["sem preco", "5,90"],
            "Revenda": ["Posto X", "Posto Y"],
            "Endereco": ["Rua A", "Rua B"],
        }
    )

    result = prepare_top3_joao_pessoa(raw, limit=3)

    assert len(result) == 1
    assert result.iloc[0]["revenda"] == "Posto Y"
