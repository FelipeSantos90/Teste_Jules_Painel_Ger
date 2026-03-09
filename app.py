import streamlit as st
import requests
from datetime import datetime, time
from contrato import Venda

def main():
    st.title("Sistema de input de dados")

    # Sidebar for environment selection
    st.sidebar.title("Configurações")
    ambiente = st.sidebar.selectbox("Selecione o ambiente", ["Teste", "Produção"])

    if ambiente == "Teste":
        webhook_url = "http://localhost:5678/webhook-test/validate-docs"
    else:
        webhook_url = "http://localhost:5678/webhook/validate-docs"

    st.sidebar.write(f"Webhook URL: `{webhook_url}`")

    email = st.text_input("Email do Vendedor")
    data = st.date_input("Data da Ocorrência", datetime.now())
    hora = st.time_input("Hora da Ocorrência", value=time(9, 0))
    valor = st.number_input("Valor da venda", min_value=0.0, format="%.2f")
    quantidade = st.number_input("Quantidade da venda", min_value=1, step=1)
    produto = st.selectbox("Selecione o produto", options=[
        "Mel especial 240g - Alerquina",
        "Mel especial 240g - Batman",
        "Mel especial 240g - Coringa"
    ])

    if st.button("Salvar"):
        try:
            data_hora = datetime.combine(data, hora)

            # Create the Pydantic model for validation
            venda = Venda(
                email=email,
                data=data_hora,
                valor=valor,
                quantidade=quantidade,
                produto=produto
            )

            st.write("**Dados da Venda validados com sucesso:**")
            st.json(venda.model_dump()) # Improved UI feedback

            # Send to webhook
            try:
                response = requests.post(webhook_url, json=venda.model_dump(mode='json'))

                if response.status_code == 200:
                    st.success(f"Dados enviados com sucesso para o webhook de {ambiente}!")
                else:
                    st.error(f"Erro ao enviar para o webhook. Status: {response.status_code}")
                    st.write(response.text)
            except requests.exceptions.ConnectionError:
                st.error(f"Não foi possível conectar ao webhook em {webhook_url}. Certifique-se de que o serviço está rodando.")

        except Exception as e:
            st.error(f"Erro na validação: {e}")

if __name__ == "__main__":
    main()
