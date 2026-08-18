# dashboard.py
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

# ---- Config ----
DB_URL = "postgresql://streaming_user:streaming_pass@localhost:5434/streaming_dw"
engine = create_engine(DB_URL)

st.set_page_config(page_title="Pipeline de E-commerce em Tempo Real", layout="wide")
st.title("📊 Pipeline de E-commerce em Tempo Real")
st.caption("Dados de streaming com observabilidade — atualiza a cada refresh da página")


@st.cache_data(ttl=15)  # cache de 15s pra não sobrecarregar o banco a cada interação
def load_valid_events():
    return pd.read_sql("SELECT * FROM streaming.eventos_validos ORDER BY processed_at DESC", engine)


@st.cache_data(ttl=15)
def load_quarantine():
    return pd.read_sql("SELECT * FROM streaming.eventos_quarentena ORDER BY quarantined_at DESC", engine)


@st.cache_data(ttl=15)
def load_metrics():
    return pd.read_sql("SELECT * FROM streaming.pipeline_metrics ORDER BY window_start", engine)


valid_df = load_valid_events()
quarantine_df = load_quarantine()
metrics_df = load_metrics()

# ---- Abas: Negócio vs Saúde do Pipeline ----
tab_negocio, tab_saude = st.tabs(["🛒 Visão de Negócio", "🩺 Saúde do Pipeline"])

with tab_negocio:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de eventos válidos", len(valid_df))
    col2.metric("Cliques", len(valid_df[valid_df["event_type"] == "click"]))
    col3.metric("Compras", len(valid_df[valid_df["event_type"] == "purchase"]))
    col4.metric(
        "Receita (compras válidas)",
        f"R$ {(valid_df[valid_df['event_type'] == 'purchase']['product_price'] * valid_df[valid_df['event_type'] == 'purchase']['quantity']).sum():,.2f}"
    )

    st.subheader("Funil de conversão")
    funil = valid_df["event_type"].value_counts().reindex(
        ["click", "add_to_cart", "purchase", "cancellation"]
    ).fillna(0)
    fig_funil = px.funnel(x=funil.values, y=funil.index, title="Jornada do usuário")
    st.plotly_chart(fig_funil, use_container_width=True)

    st.subheader("Produtos mais populares")
    top_produtos = valid_df["product_id"].value_counts().head(10)
    fig_produtos = px.bar(x=top_produtos.index, y=top_produtos.values, labels={"x": "Produto", "y": "Eventos"})
    st.plotly_chart(fig_produtos, use_container_width=True)

with tab_saude:
    if metrics_df.empty:
        st.info("Ainda não há métricas suficientes. Deixe o consumidor rodando por mais tempo.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Taxa de erro (última janela)", f"{metrics_df.iloc[-1]['error_rate']}%")
        col2.metric("Total processado", int(metrics_df["total_events"].sum()))
        col3.metric("Total em quarentena", len(quarantine_df))

        st.subheader("Taxa de erro ao longo do tempo")
        fig_erro = px.line(metrics_df, x="window_start", y="error_rate", title="Taxa de erro por janela (%)")
        fig_erro.add_hline(y=20, line_dash="dash", line_color="red", annotation_text="Limite de alerta (20%)")
        st.plotly_chart(fig_erro, use_container_width=True)

        st.subheader("Volume de eventos válidos vs inválidos por janela")
        fig_volume = px.bar(
            metrics_df, x="window_start", y=["valid_events", "invalid_events"],
            title="Válidos vs Inválidos", barmode="stack"
        )
        st.plotly_chart(fig_volume, use_container_width=True)

    st.subheader("Principais motivos de erro (eventos em quarentena)")
    if not quarantine_df.empty:
        # validation_errors é um array de strings tipo "campo_faltando:product_price"
        exploded = quarantine_df.explode("validation_errors")
        exploded["motivo"] = exploded["validation_errors"].str.split(":").str[0]
        motivos = exploded["motivo"].value_counts()
        fig_motivos = px.pie(values=motivos.values, names=motivos.index, title="Distribuição de erros")
        st.plotly_chart(fig_motivos, use_container_width=True)

        st.subheader("Últimos eventos em quarentena")
        st.dataframe(quarantine_df[["quarantined_at", "validation_errors", "raw_event"]].head(20))
    else:
        st.info("Nenhum evento em quarentena ainda.")