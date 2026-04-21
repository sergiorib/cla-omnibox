from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pathlib import Path
import json
import math

# Para colocar esta API no ar use: Poetry run uvicorn cla_omnibox_ingestao_marketing_api:app --reload
# Veja rodando em: http://127.0.0.1:8000/docs

app = FastAPI(
    title="Omnibox Marketing API (Mock)",
    description="API simulada para extração de dados de campanhas, gastos e ações.",
    version="1.0.0"
)

diretorio_atual = Path(__file__).resolve().parent    
diretorio_base = diretorio_atual.parents[1]

# Caminho dos dados a serem disponibilizados pela API
DIRETORIO_RAW = diretorio_base / "datalake" / "raw" / "marketing_api" 

# 2. Simulação de Segurança (Bearer Token)
TOKEN_VALIDO = "omnibox_sec_token_2026"
security = HTTPBearer()

def verificar_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    O FastAPI agora valida se o cabeçalho Authorization existe.
    credentials.credentials entrega o token já limpo (sem a palavra 'Bearer').
    """
    if credentials.credentials != TOKEN_VALIDO:
        raise HTTPException(
            status_code=401, 
            detail="Não autorizado. Token de segurança inválido."
        )
    return credentials.credentials

# 3. Função Auxiliar de Leitura e Paginação
def ler_e_paginar_json(nome_arquivo: str, page: int, size: int):
    caminho_arquivo = DIRETORIO_RAW / nome_arquivo
    
    if not caminho_arquivo.exists():
        raise HTTPException(status_code=404, detail=f"Arquivo base {nome_arquivo} não encontrado no Datalake Raw.")
        
    try:
        with open(caminho_arquivo, "r", encoding="utf-8") as f:
            dados = json.load(f)
            
        total_registros = len(dados)
        total_paginas = math.ceil(total_registros / size)
        
        # Lógica de fatiamento (slice) da lista para simular a página
        inicio = (page - 1) * size
        fim = inicio + size
        dados_pagina = dados[inicio:fim]
        
        return {
            "metadata": {
                "total_registros": total_registros,
                "total_paginas": total_paginas,
                "pagina_atual": page,
                "tamanho_pagina": size,
                "tem_proxima_pagina": page < total_paginas
            },
            "data": dados_pagina
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar JSON: {str(e)}")

# 4. Endpoints da API (Protegidos pelo Token)

@app.get("/campaigns", dependencies=[Depends(verificar_token)])
def get_campaigns(page: int = Query(1, ge=1), size: int = Query(50, ge=1, le=500)):
    """Retorna a lista de campanhas de marketing."""
    return ler_e_paginar_json("mkt_campaigns.json", page, size)

@app.get("/actions", dependencies=[Depends(verificar_token)])
def get_actions(page: int = Query(1, ge=1), size: int = Query(50, ge=1, le=500)):
    """Retorna os eventos e ações de marketing (cliques, views, etc)."""
    return ler_e_paginar_json("mkt_actions.json", page, size)

@app.get("/spend", dependencies=[Depends(verificar_token)])
def get_spend(page: int = Query(1, ge=1), size: int = Query(50, ge=1, le=500)):
    """Retorna o histórico de gastos (investimento) por campanha."""
    return ler_e_paginar_json("mkt_spend.json", page, size)