import gspread
import os
import json
import sys
from datetime import datetime
import requests # NOVO: Para fazer requisições HTTP (API Supabase)

# ===============================================
# 1. CONFIGURAÇÕES DO SUPABASE (HARDCODED)
# ===============================================
SUPABASE_URL = "https://uidlyplhksbwerbdgtys.supabase.co"
SUPABASE_KEY = "sb_publishable_kUFjQWo7t2d4NccZYi4E9Q_okgJ1DOe"

# --- CONFIGURAÇÕES GERAIS ---
# IDs das planilhas (MANTIDAS)
PLANILHA_ORIGEM_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug" # Planilha de Vendas e Gastos (Origem)

# Mapeamento das Abas: {ABA_ORIGEM (minúscula): TABELA NO SUPABASE}
MAP_ABAS = {
    "vendas": "vendas", 
    "gastos": "despesas" 
}

# MAPA DE TRADUÇÃO (Sheets Column Header -> Supabase Column Name)
# Usando o mapeamento 1:1 ('sujo') que fizemos funcionar no Apps Script.
COLUNA_MAP = {
    "Carimbo de data/hora": "Carimbo de data/hora", 
    "PRODUTO": "PRODUTO",
    "QUANTIDADE": "QUANTIDADE",
    "VALOR": "VALOR"
}
# -----------------------------------------------------------


# --- FUNÇÕES DE CONEXÃO E UTILIDADE ---

def autenticar_gspread():
    """Autentica o gspread usando a variável de ambiente (GSPREAD_SERVICE_ACCOUNT_CREDENTIALS)."""
    credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')
    # ... (restante da autenticação gspread) ...
    if not credenciais_json_string:
        # Se você estiver rodando isso localmente, esta exceção é importante
        raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada!")

    try:
        credenciais_dict = json.loads(credenciais_json_string)
        return gspread.service_account_from_dict(credenciais_dict)
    except Exception as e:
        raise Exception(f"Erro ao carregar ou autenticar credenciais JSON: {e}")

def clean_value(valor):
    """
    Tradutor cultural: Converte valores com vírgula (R$) para o formato 
    de ponto decimal (DB) e trata valores nulos/vazios.
    """
    if not valor or str(valor).strip() == '':
        return None
    
    cleaned = str(valor)
    # 1. Remove separador de milhares (ponto)
    cleaned = cleaned.replace('.', '')
    # 2. Troca a vírgula pelo ponto
    cleaned = cleaned.replace(',', '.')
    
    try:
        return float(cleaned)
    except ValueError:
        # Se não for um número (ex: texto 'R$ 100'), retorna o valor original
        return valor  


# --- FUNÇÃO PRINCIPAL DE BACKUP/MIGRAÇÃO ---

def fazer_migracao(gc, planilha_origem_id, aba_origem_name, tabela_destino_name):
    """
    Lê do Sheets, processa e envia para o Supabase.
    """
    print(f"\n--- Iniciando Migração: {aba_origem_name.upper()} para Supabase ({tabela_destino_name}) ---")
    
    try:
        # 1. Abre a aba de origem e pega todos os dados
        planilha_origem = gc.open_by_key(planilha_origem_id).worksheet(aba_origem_name)
        dados_do_mes = planilha_origem.get_all_values()
        
        # 2. Verifica se há dados novos
        headers = dados_do_mes[0]
        dados_para_processar = dados_do_mes[1:] 

        if not dados_para_processar:
            print(f"Não há novos dados na aba '{aba_origem_name}' para migrar.")
            return

        payload_supa = []

        # 3. Processamento e Limpeza (Cria o Payload JSON)
        for linha in dados_para_processar:
            registro = {}
            for idx, valor_sheet in enumerate(linha):
                header_sheet = headers[idx]
                
                if header_sheet in COLUNA_MAP:
                    coluna_supa = COLUNA_MAP[header_sheet]
                    valor_processado = valor_sheet
                    
                    # Aplica a limpeza de formato APENAS na coluna VALOR
                    if header_sheet.upper() == "VALOR":
                        valor_processado = clean_value(valor_sheet)

                    registro[coluna_supa] = valor_processado
            
            if registro:
                payload_supa.append(registro)

        # 4. Envio para o Supabase
        enviar_para_supabase(payload_supa, tabela_destino_name)
        
        print(f"=========================================================================")
        # Manter o alerta para a limpeza manual (Governança)
        print(f"!!! ATENÇÃO !!!: A limpeza da aba de origem ('{aba_origem_name}') NÃO FOI FEITA.")
        print(f"Se esta for uma migração de backup, a limpeza manual deve ser feita.")
        print(f"=========================================================================")


    except gspread.exceptions.WorksheetNotFound as e:
        print(f"ERRO: A aba '{aba_origem_name}' não foi encontrada.")
        raise RuntimeError(f"Falha na validação da Planilha: {e}") 
    except Exception as e:
        print(f"ERRO GRAVE durante a migração de {aba_origem_name}: {e}")
        raise


def enviar_para_supabase(dados_para_copiar, tabela_destino):
    """Faz a requisição POST para o Supabase (Suporta inserção em lote)."""
    url = f"{SUPABASE_URL}/rest/v1/{tabela_destino}"

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }

    print(f"Enviando {len(dados_para_copiar)} registros para a tabela '{tabela_destino}'...")

    response = requests.post(url, headers=headers, json=dados_para_copiar)

    if response.status_code == 201:
        print(f"Sucesso! {len(dados_para_copiar)} registros inseridos em '{tabela_destino}'.")
    else:
        print(f"ERRO Supabase ({tabela_destino}): Status {response.status_code}")
        print(f"Resposta: {response.text}")
        raise RuntimeError(f"Falha na inserção no Supabase.")

    return True


def main():
    """Função principal para orquestrar a execução."""
    
    # As variáveis do Supabase estão agora hardcoded no topo do script.

    # Verifica se a execução foi forçada manualmente (governança de tempo)
    FORCA_EXECUCAO = os.environ.get('FORCA_EXECUCAO_MANUAL', 'false').lower() == 'true'
    hoje = datetime.now().day
    
    # -------------------------------------------------------------
    # Controle de Execução: Apenas no dia 1 (OU se for forçado)
    # -------------------------------------------------------------
    
    if hoje != 1 and not FORCA_EXECUCAO:
        print(f"Hoje é dia {hoje}. O Agente de Backup está dormindo (aguardando o dia 1 do mês).")
        sys.exit(0) 
    
    # 1. Autentica UMA VEZ no GSheets
    gc = autenticar_gspread()
    
    # 2. Executa a função de migração para Vendas e Gastos
    for origem, destino in MAP_ABAS.items():
        # Aba 'origem' no Sheets e 'destino' no Supabase
        fazer_migracao(gc, PLANILHA_ORIGEM_ID, origem, destino)
        
    print("\n✅ ORQUESTRAÇÃO DE MIGRAÇÃO CONCLUÍDA.")


if __name__ == "__main__":
    try:
        main()
    except Exception as final_e:
        print(f"\n### FALHA CRÍTICA DO AGENTE ###\n{final_e}")
        sys.exit(1)
