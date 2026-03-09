import unittest
from datetime import datetime
from pydantic import ValidationError
from contrato import Venda, ProdutoEnum
import requests
from unittest.mock import patch, MagicMock

class TestVendaModel(unittest.TestCase):
    def test_valid_venda(self):
        data = {
            "email": "vendedor@teste.com",
            "data": datetime.now(),
            "valor": 100.50,
            "quantidade": 2,
            "produto": "Mel especial 240g - Alerquina"
        }
        venda = Venda(**data)
        self.assertEqual(venda.email, data["email"])
        self.assertEqual(venda.produto, ProdutoEnum.produto1)

    def test_invalid_email(self):
        data = {
            "email": "email-invalido",
            "data": datetime.now(),
            "valor": 100.50,
            "quantidade": 2,
            "produto": "Mel especial 240g - Alerquina"
        }
        with self.assertRaises(ValidationError):
            Venda(**data)

    def test_negative_valor(self):
        data = {
            "email": "vendedor@teste.com",
            "data": datetime.now(),
            "valor": -10.0,
            "quantidade": 2,
            "produto": "Mel especial 240g - Alerquina"
        }
        with self.assertRaises(ValidationError):
            Venda(**data)

    def test_invalid_produto(self):
        data = {
            "email": "vendedor@teste.com",
            "data": datetime.now(),
            "valor": 100.50,
            "quantidade": 2,
            "produto": "Produto Inexistente"
        }
        with self.assertRaises(ValidationError):
            Venda(**data)

class TestWebhookIntegration(unittest.TestCase):
    @patch('requests.post')
    def test_webhook_post(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        url = "http://localhost:5678/webhook-test/validate-docs"
        payload = {
            "email": "vendedor@teste.com",
            "data": datetime.now().isoformat(),
            "valor": 100.50,
            "quantidade": 2,
            "produto": "Mel especial 240g - Alerquina"
        }

        response = requests.post(url, json=payload)

        mock_post.assert_called_once_with(url, json=payload)
        self.assertEqual(response.status_code, 200)

if __name__ == "__main__":
    unittest.main()
