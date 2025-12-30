# Dashboard de Importação Orientada por Dados
# Autor: Domingos Mulungo
# Objetivo: Apoiar decisão (o que importar, quanto e quando) com KPIs, alertas e simulação.
#
# ✅ Este ficheiro funciona em 2 modos:
#   1) STREAMLIT (se "streamlit" estiver instalado): interface web interativa.
#   2) OFFLINE/CLI (se "streamlit" NÃO estiver instalado): gera um relatório HTML + gráficos.
#
# Principais melhorias nesta versão:
# - Se for executado em modo offline sem argumentos, **assume --demo por padrão** (evita SystemExit).
# - Mensagens de ajuda mais claras quando faltarem ficheiros.
# - Mantém testes e adiciona novos testes.
#
# Como executar:
#   - Streamlit (se instalado):
#       streamlit run app.py
#
#   - Offline/CLI (sem streamlit):
#       # 1) Com dados reais
#       python app.py --produtos produtos.csv --custos custos.csv --vendas vendas.csv --stock stock.csv
#       # 2) Com dados de demonstração (recomendado para testar)
#       python app.py --demo
#       # 3) Sem argumentos (AGORA funciona: assume --demo)
#       python app.py

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd

# ----------------------------
# Optional dependency: Streamlit
# ----------------------------
try:
    import streamlit as st  # type: ignore

    STREAMLIT_AVAILABLE = True
except ModuleNotFoundError:
    st = None  # type: ignore
    STREAMLIT_AVAILABLE = False


# ============================
# Config / Defaults
# ============================
DEFAULT_OUTDIR = Path("outputs")
DEFAULT_OUTDIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLS = {
    "produtos": {"produto_id", "nome_produto"},
    "custos": {"produto_id", "custo_total"},
    "vendas": {"produto_id", "data", "quantidade", "preco_venda"},
    "stock": {"produto_id", "stock_atual", "stock_min"},
}


# ============================
# Core logic (independente de UI)
# ============================

def load_data(path_or_file) -> pd.DataFrame:
    """Carrega CSV/XLSX a partir de um path (CLI) ou file-like (Streamlit)."""
    name = getattr(path_or_file, "name", None)
    if name is None:
        name = str(path_or_file)

    name_lower = name.lower()
    if name_lower.endswith(".csv"):
        return pd.read_csv(path_or_file)
    if name_lower.endswith((".xlsx", ".xls")):
        return pd.read_excel(path_or_file)
    raise ValueError(f"Formato não suportado: {name}. Use .csv ou .xlsx")


def validate_columns(df: pd.DataFrame, required: set[str], label: str) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"O ficheiro '{label}' está a faltar colunas obrigatórias: {sorted(missing)}. "
            f"Colunas disponíveis: {sorted(df.columns)}"
        )


def prepare_dataset(
    produtos: pd.DataFrame,
    custos: pd.DataFrame,
    vendas: pd.DataFrame,
    stock: pd.DataFrame,
) -> pd.DataFrame:
    """Faz merge, cria métricas e prepara dataframe final."""
    validate_columns(produtos, REQUIRED_COLS["produtos"], "produtos")
    validate_columns(custos, REQUIRED_COLS["custos"], "custos")
    validate_columns(vendas, REQUIRED_COLS["vendas"], "vendas")
    validate_columns(stock, REQUIRED_COLS["stock"], "stock")

    vendas = vendas.copy()
    vendas["data"] = pd.to_datetime(vendas["data"], errors="coerce")
    if vendas["data"].isna().any():
        bad = vendas[vendas["data"].isna()].head(5)
        raise ValueError(
            "Há datas inválidas em 'vendas.data'. Exemplos (primeiras linhas problemáticas):\n"
            + bad.to_string(index=False)
        )

    df = (
        vendas.merge(produtos, on="produto_id", how="left")
        .merge(custos, on="produto_id", how="left")
        .merge(stock, on="produto_id", how="left")
    )

    # Checagens básicas após merge
    for col in ["nome_produto", "custo_total", "stock_atual", "stock_min"]:
        if df[col].isna().any():
            raise ValueError(
                f"Após o merge, existem valores em falta na coluna '{col}'. "
                "Verifique se todos os 'produto_id' batem entre os ficheiros."
            )

    # Métricas
    df["receita"] = df["quantidade"] * df["preco_venda"]
    df["lucro_unitario"] = df["preco_venda"] - df["custo_total"]
    df["lucro_total"] = df["lucro_unitario"] * df["quantidade"]

    # Evitar divisão por zero
    df["margem_%"] = np.where(
        df["preco_venda"].astype(float) == 0,
        np.nan,
        (df["lucro_unitario"] / df["preco_venda"]) * 100,
    )

    # Estimar dias para ruptura
    df = df.sort_values(["produto_id", "data"]).reset_index(drop=True)

    daily = (
        df.groupby(["produto_id", pd.Grouper(key="data", freq="D")])["quantidade"]
        .sum()
        .reset_index()
    )
    daily_rate = daily.groupby("produto_id")["quantidade"].mean().rename("taxa_diaria")

    df = df.merge(daily_rate, on="produto_id", how="left")

    # Heurística quando taxa_diaria é NaN/0
    df["taxa_diaria"] = df["taxa_diaria"].fillna(df.groupby("produto_id")["quantidade"].transform("mean") / 7)
    df["taxa_diaria"] = df["taxa_diaria"].replace(0, np.nan)

    df["dias_para_ruptura"] = np.where(
        df["taxa_diaria"].isna(),
        np.nan,
        df["stock_atual"] / df["taxa_diaria"],
    )

    return df


@dataclass
class KPIs:
    receita_total: float
    lucro_total: float
    margem_media: float
    produtos_ativos: int


def compute_kpis(df: pd.DataFrame) -> KPIs:
    return KPIs(
        receita_total=float(df["receita"].sum()),
        lucro_total=float(df["lucro_total"].sum()),
        margem_media=float(np.nanmean(df["margem_%"])) if len(df) else float("nan"),
        produtos_ativos=int(df["produto_id"].nunique()),
    )


def stock_alerts(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna produtos com risco de ruptura (stock_atual <= stock_min) usando o último registo por produto."""
    latest = df.sort_values("data").groupby("produto_id").tail(1)
    alerts = latest[latest["stock_atual"] <= latest["stock_min"]].copy()
    return alerts[["produto_id", "nome_produto", "stock_atual", "stock_min", "dias_para_ruptura"]].sort_values(
        ["dias_para_ruptura", "stock_atual"], ascending=[True, True]
    )


def lucro_por_produto(df: pd.DataFrame) -> pd.Series:
    return df.groupby("nome_produto")["lucro_total"].sum().sort_values(ascending=False)


def receita_mensal(df: pd.DataFrame) -> pd.Series:
    s = df.groupby(pd.Grouper(key="data", freq="M"))["receita"].sum()
    s.index = s.index.to_period("M").to_timestamp()
    return s


def simulate_profit(df: pd.DataFrame, nome_produto: str, quantidade: int) -> float:
    if quantidade < 0:
        raise ValueError("quantidade deve ser >= 0")
    row = df[df["nome_produto"] == nome_produto].sort_values("data").tail(1)
    if row.empty:
        raise ValueError(f"Produto '{nome_produto}' não encontrado")
    p = row.iloc[0]
    return float(quantidade * (p["preco_venda"] - p["custo_total"]))


# ============================
# Offline report (HTML + plots)
# ============================

def generate_offline_report(df: pd.DataFrame, outdir: Path = DEFAULT_OUTDIR) -> Path:
    """Gera relatório HTML simples + PNGs com matplotlib."""
    import matplotlib.pyplot as plt

    outdir.mkdir(parents=True, exist_ok=True)

    k = compute_kpis(df)
    alerts = stock_alerts(df)
    s_receita = receita_mensal(df)
    s_lucro_prod = lucro_por_produto(df).head(20)

    fig1 = plt.figure()
    plt.plot(s_receita.index, s_receita.values)
    plt.xticks(rotation=45, ha="right")
    plt.title("Receita mensal")
    plt.tight_layout()
    receita_png = outdir / "receita_mensal.png"
    fig1.savefig(receita_png, dpi=160)
    plt.close(fig1)

    fig2 = plt.figure()
    plt.bar(s_lucro_prod.index.astype(str), s_lucro_prod.values)
    plt.xticks(rotation=70, ha="right")
    plt.title("Lucro por produto (Top 20)")
    plt.tight_layout()
    lucro_png = outdir / "lucro_por_produto_top20.png"
    fig2.savefig(lucro_png, dpi=160)
    plt.close(fig2)

    alerts_html = alerts.to_html(index=False) if not alerts.empty else "<p><b>✅ Stock sob controlo.</b></p>"

    html = f"""<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Relatório de Importação</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .kpi {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }}
    .card {{ border: 1px solid #ddd; border-radius: 10px; padding: 12px; }}
    img {{ max-width: 100%; height: auto; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; }}
    th {{ background: #f6f6f6; }}
  </style>
</head>
<body>
  <h1>📦 Relatório de Importação Orientada por Dados</h1>

  <h2>📊 Indicadores-chave</h2>
  <div class="kpi">
    <div class="card"><b>Receita total</b><br/>{k.receita_total:,.0f} MZN</div>
    <div class="card"><b>Lucro total</b><br/>{k.lucro_total:,.0f} MZN</div>
    <div class="card"><b>Margem média</b><br/>{k.margem_media:.1f}%</div>
    <div class="card"><b>Produtos ativos</b><br/>{k.produtos_ativos}</div>
  </div>

  <h2>📈 Receita mensal</h2>
  <img src="{receita_png.name}" alt="Receita mensal"/>

  <h2>💰 Lucro por produto (Top 20)</h2>
  <img src="{lucro_png.name}" alt="Lucro por produto"/>

  <h2>🚨 Alertas de stock</h2>
  {alerts_html}

  <hr/>
  <p><small>Gerado automaticamente.</small></p>
</body>
</html>
"""

    report_path = outdir / "relatorio_importacao.html"
    report_path.write_text(html, encoding="utf-8")
    return report_path


# ============================
# Streamlit app
# ============================

def run_streamlit_app() -> None:
    assert st is not None

    st.set_page_config(page_title="Dashboard de Importação", layout="wide")
    st.title("📦 Dashboard de Importação Orientada por Dados")
    st.markdown("Apoio à decisão: **o que importar, quanto importar e quando importar**")

    st.sidebar.header("📂 Carregar dados")
    produtos_file = st.sidebar.file_uploader("Produtos", type=["xlsx", "csv"])
    custos_file = st.sidebar.file_uploader("Custos de Importação", type=["xlsx", "csv"])
    vendas_file = st.sidebar.file_uploader("Vendas", type=["xlsx", "csv"])
    stock_file = st.sidebar.file_uploader("Stock", type=["xlsx", "csv"])

    demo = st.sidebar.checkbox("Usar dados de demonstração", value=False)

    @st.cache_data
    def _load(file):
        return load_data(file)

    if demo:
        produtos, custos, vendas, stock = make_demo_data()
    elif produtos_file and custos_file and vendas_file and stock_file:
        produtos = _load(produtos_file)
        custos = _load(custos_file)
        vendas = _load(vendas_file)
        stock = _load(stock_file)
    else:
        st.info("⬅️ Carregue os 4 ficheiros (ou ative dados de demonstração).")
        return

    try:
        df = prepare_dataset(produtos, custos, vendas, stock)
    except Exception as e:
        st.error(f"Erro ao preparar dados: {e}")
        return

    k = compute_kpis(df)
    st.subheader("📊 Indicadores-chave")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Receita Total", f"{k.receita_total:,.0f} MZN")
    c2.metric("Lucro Total", f"{k.lucro_total:,.0f} MZN")
    c3.metric("Margem Média", f"{k.margem_media:.1f} %")
    c4.metric("Produtos Ativos", k.produtos_ativos)

    st.subheader("📈 Desempenho de Vendas")
    st.line_chart(receita_mensal(df))

    st.subheader("💰 Lucro por Produto")
    st.bar_chart(lucro_por_produto(df))

    st.subheader("🚨 Controlo de Stock")
    alerts = stock_alerts(df)
    if not alerts.empty:
        st.error("⚠️ Produtos com risco de ruptura")
        st.dataframe(alerts, use_container_width=True)
    else:
        st.success("✅ Stock sob controlo")

    st.subheader("🔮 Simulador de Importação")
    produto_sel = st.selectbox("Produto", sorted(df["nome_produto"].unique()))
    qtd = st.slider("Quantidade a importar", 0, 5000, 100)
    lucro_estimado = simulate_profit(df, produto_sel, qtd)
    st.info(f"💡 Lucro estimado para {qtd} unidades: **{lucro_estimado:,.0f} MZN**")

    st.subheader("🧾 Exportar relatório")
    if st.button("Gerar relatório HTML (offline)"):
        report = generate_offline_report(df, DEFAULT_OUTDIR)
        st.success(f"Relatório gerado: {report}")


# ============================
# Demo data
# ============================

def make_demo_data(seed: int = 7) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)

    produtos = pd.DataFrame(
        {
            "produto_id": np.arange(1, 11),
            "nome_produto": [
                "Powerbank 20k",
                "Auriculares BT",
                "Cabo USB-C",
                "Carregador 20W",
                "Teclado",
                "Mouse",
                "Smartwatch",
                "Ring light",
                "Tripé",
                "Adaptador",
            ],
        }
    )

    custos = pd.DataFrame(
        {
            "produto_id": produtos["produto_id"],
            "custo_total": rng.uniform(120, 900, size=len(produtos)).round(2),
        }
    )

    dates = pd.date_range("2025-09-01", periods=90, freq="D")
    rows = []
    for d in dates:
        for pid in produtos["produto_id"]:
            if rng.random() < 0.30:
                continue
            qtd = int(rng.integers(1, 15))
            custo = float(custos.loc[custos["produto_id"] == pid, "custo_total"].iloc[0])
            markup = float(rng.uniform(1.2, 1.9))
            preco = round(custo * markup, 2)
            rows.append((pid, d, qtd, preco))

    vendas = pd.DataFrame(rows, columns=["produto_id", "data", "quantidade", "preco_venda"])

    stock = pd.DataFrame(
        {
            "produto_id": produtos["produto_id"],
            "stock_atual": rng.integers(0, 250, size=len(produtos)),
            "stock_min": rng.integers(20, 80, size=len(produtos)),
        }
    )

    return produtos, custos, vendas, stock


# ============================
# Tests (executar: python app.py --run-tests)
# ============================

def _run_tests() -> None:
    # Teste 1: demo data gera dataset sem NaNs críticos
    produtos, custos, vendas, stock = make_demo_data(seed=1)
    df = prepare_dataset(produtos, custos, vendas, stock)
    assert df["receita"].ge(0).all()
    assert df["produto_id"].nunique() == len(produtos)

    # Teste 2: simulate_profit
    p = df["nome_produto"].iloc[0]
    profit0 = simulate_profit(df, p, 0)
    assert profit0 == 0.0
    profit10 = simulate_profit(df, p, 10)
    last = df[df["nome_produto"] == p].sort_values("data").tail(1).iloc[0]
    assert np.isclose(profit10, 10 * (last["preco_venda"] - last["custo_total"]))

    # Teste 3: stock_alerts respeita regra stock_atual <= stock_min
    stock2 = stock.copy()
    stock2.loc[0, "stock_atual"] = 0
    stock2.loc[0, "stock_min"] = 10
    df2 = prepare_dataset(produtos, custos, vendas, stock2)
    alerts = stock_alerts(df2)
    assert (alerts["stock_atual"] <= alerts["stock_min"]).all()
    assert (alerts["produto_id"] == 1).any()

    # Teste 4: load_data formato inválido
    try:
        load_data("ficheiro.txt")
        assert False, "Era esperado ValueError para formato inválido"
    except ValueError:
        pass

    print("✅ Todos os testes passaram.")


# ============================
# CLI entrypoint
# ============================

def _missing_args_message() -> str:
    return (
        "Modo offline precisa de:\n"
        "  - --demo (para testar com dados gerados)\n"
        "  OU\n"
        "  - os 4 ficheiros: --produtos --custos --vendas --stock\n\n"
        "Exemplos:\n"
        "  python app.py --demo\n"
        "  python app.py --produtos produtos.csv --custos custos.csv --vendas vendas.csv --stock stock.csv\n"
    )


def _is_running_in_streamlit() -> bool:
    """Detecta execução via `streamlit run ...`.

    Importante para deploy no Streamlit Community Cloud: o Streamlit executa o script
    e `__name__` costuma ser "__main__". Sem esta detecção, o argparse tentaria
    ler argumentos do Streamlit e falharia.
    """
    if not STREAMLIT_AVAILABLE:
        return False
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx  # type: ignore

        return get_script_run_ctx() is not None
    except Exception:
        # fallback: heurística por argv
        import sys

        return any("streamlit" in a.lower() for a in sys.argv)


def main_cli() -> None:
    parser = argparse.ArgumentParser(description="Dashboard de Importação (Streamlit opcional)")
    parser.add_argument("--produtos", type=str, help="Path para produtos.csv/xlsx")
    parser.add_argument("--custos", type=str, help="Path para custos.csv/xlsx")
    parser.add_argument("--vendas", type=str, help="Path para vendas.csv/xlsx")
    parser.add_argument("--stock", type=str, help="Path para stock.csv/xlsx")
    parser.add_argument("--outdir", type=str, default=str(DEFAULT_OUTDIR), help="Pasta de saída")
    parser.add_argument("--demo", action="store_true", help="Usar dados de demonstração")
    parser.add_argument("--run-tests", action="store_true", help="Executar testes básicos")
    args = parser.parse_args()

    if args.run_tests:
        _run_tests()
        return

    outdir = Path(args.outdir)

    # ✅ FIX: se o utilizador não passar argumentos nenhuns, assume demo por padrão.
    no_paths = not (args.produtos or args.custos or args.vendas or args.stock)
    if no_paths and not args.demo:
        args.demo = True

    if args.demo:
        produtos, custos, vendas, stock = make_demo_data()
    else:
        if not (args.produtos and args.custos and args.vendas and args.stock):
            parser.print_help()
            raise SystemExit("\n" + _missing_args_message())

        produtos = load_data(args.produtos)
        custos = load_data(args.custos)
        vendas = load_data(args.vendas)
        stock = load_data(args.stock)

    df = prepare_dataset(produtos, custos, vendas, stock)
    report = generate_offline_report(df, outdir)
    print(f"✅ Relatório gerado em: {report}")


def main() -> None:
    # Se estiver a correr via Streamlit, abre a UI.
    if _is_running_in_streamlit():
        run_streamlit_app()
    else:
        main_cli()


if __name__ == "__main__":
    main()
