from pydantic import BaseModel, EmailStr, PositiveFloat, PositiveInt
from datetime import datetime
from enum import Enum

class ProdutoEnum(str, Enum):
    produto1 = "Mel especial 240g - Alerquina"
    produto2 = "Mel especial 240g - Batman"
    produto3 = "Mel especial 240g - Coringa"

class Venda(BaseModel):
    email: EmailStr
    data: datetime
    valor: PositiveFloat
    quantidade: PositiveInt
    produto: ProdutoEnum
