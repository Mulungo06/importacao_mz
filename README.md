# Dashboard de Importação Orientada por Dados (Moçambique)

## 1) Rodar localmente
```bash
pip install -r requirements.txt
streamlit run app.py
```

## 2) Deploy no Streamlit Community Cloud
1. Crie um repositório no GitHub e faça upload destes ficheiros:
   - `app.py`
   - `requirements.txt`
2. Vá ao Streamlit Community Cloud e conecte o repositório.
3. Em **Main file path**, selecione `app.py`.

## 3) Formato dos ficheiros (para usar dados reais)
Carregue 4 ficheiros (CSV ou XLSX) com estas colunas mínimas:

**produtos**
- `produto_id`, `nome_produto`

**custos**
- `produto_id`, `custo_total`

**vendas**
- `produto_id`, `data`, `quantidade`, `preco_venda`

**stock**
- `produto_id`, `stock_atual`, `stock_min`

> Dica: marque “Usar dados de demonstração” para testar sem ficheiros.
