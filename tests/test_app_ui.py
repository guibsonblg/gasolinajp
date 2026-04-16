from __future__ import annotations

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_FILE = PROJECT_ROOT / "app.py"
CSS_FILE = PROJECT_ROOT / "assets" / "app.css"


def test_app_renders_core_ui_without_exceptions() -> None:
    at = AppTest.from_file(str(APP_FILE))
    at.run(timeout=30)

    assert not at.exception
    assert at.radio
    assert at.multiselect
    assert at.text_input
    assert len(at.button) >= 3
    assert at.dataframe


def test_bairro_multiselect_keeps_single_manual_selection() -> None:
    at = AppTest.from_file(str(APP_FILE))
    at.run(timeout=30)

    if not at.multiselect:
        pytest.skip("Multiselect de bairros não foi renderizado")

    options = list(at.multiselect[0].options)
    if len(options) < 2:
        pytest.skip("Dados insuficientes para validar seleção de bairros")

    one_bairro = [options[0]]
    at.multiselect[0].set_value(one_bairro).run(timeout=30)

    assert at.multiselect[0].value == one_bairro


def test_app_has_mobile_responsive_css_rules() -> None:
    content = CSS_FILE.read_text(encoding="utf-8")
    app_content = APP_FILE.read_text(encoding="utf-8")

    assert "@media (max-width: 768px)" in content
    assert ".hero h1" in content
    assert "width=\"stretch\"" in app_content
