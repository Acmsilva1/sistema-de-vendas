import gspread
import os
import json
import sys
import time
from datetime import datetime
import requests 

# ===============================================
# 1. CONFIGURAÇÕES DO SUPABASE 
# ===============================================
SUPABASE_URL = "https://uidlyplhksbwerbdgtys.supabase.co"
SUPABASE_KEY = "sb_publishable_kUFjQWo7t2d4NccZYi4E9Q_okgJ1DOe"

# --- CONSTANTES CRÍTICAS (Governança: TUDO MINÚSCULO/SNAKE_CASE) ---
# O código presume que as colunas no DB (Supabase) são:
SUPABASE_CARIMBO_KEY_DB = "carimbo_data_hora" 
SUPABASE_PRODUTO_KEY = "produto" 
SUPABASE_QUANTIDADE_KEY = "quantidade"
SUPABASE_VALOR_KEY = "valor"
SUPABASE_COMPRADOR_KEY = "dados_do_comprador" 

# --- CONFIGURAÇÕES GERAIS (Mapeamento Planilhas e Abas) ---

MAP_MIGRATION = {
    "vendas": {
        "planilha_id": "1ygApI7DemPMEjfRcZmR1LVU9ofHP-dkL71m59-USnuY", 
        "aba_nome": "VENDAS", 
        "tabela_supa": "vendas"
    }, 
    "gastos": {
        "planilha_id": "1y2YlMaaVMb0K4XlT7rx5s7X_2iNGL8dMAOSOpX4y_FA", 
        "aba_nome": "despesas", 
        "tabela_supa": "despesas"
    } 
}

# --- MAPA DE ÍNDICES (Sheets Index -> Supabase Column Name) ---
# Baseado na ordem de colunas do Sheets (A, B, C, D...)
# 0 (Coluna A) -> Carimbo de data/hora
# 1 (Coluna B) -> PRODUTO/DESPESA
# 2 (Coluna C) -> QUANTIDADE
# 3 (Coluna D) -> VALOR
# 4 (Coluna E) -> DADOS DO COMPRADOR/Outros (Ajuste se necessário!)
INDEX_MAP = {
    0: SUPABASE_CARIMBO_KEY_DB,
    1: SUPABASE_PRODUTO_KEY, 
    2: SUPABASE_QUANTIDADE_KEY,
    3: SUPABASE_VALOR_KEY,
    4: SUPABASE_COMPRADOR_KEY, 
}
# -----------------------------------------------------------


# --- FUNÇÕES AUXILIARES (Inalteradas) ---

def autenticar_gspread():
    credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')
    
    if not credenciais_json_string:
        raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada!")

    try:
        credenciais_dict = json.loads(credenciais_json_string)
        return gspread.service_account_from_dict(credenciais_dict)
    except Exception as e:
        raise Exception(f"Erro ao carregar ou autenticar credenciais JSON: {e}")

def clean_value(valor):
    if not valor or str(valor).strip() == '':
        return None
    cleaned = str(valor).replace('.', '').replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return valor  

def format_datetime_for_supabase(carimbo_str):
    if not isinstance(carimbo_str, str) or not carimbo_str.strip():
        return None
    try:
        dt_obj = datetime.strptime(carimbo_str.strip(), '%d/%m/%Y %H:%M:%S')
        return dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return None

def enviar_registro_simples(registro, tabela_destino):
    
    global SUPABASE_CARIMBO_KEY_DB 

    # O carimbo JÁ VEM formatado neste ponto
    carimbo_formatado = registro.get(SUPABASE_CARIMBO_KEY_DB) 
    
    url_insert = f"{SUPABASE_URL}/rest/v1/{tabela_destino}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    
    try:
        response_insert = requests.post(url_insert, headers=headers, json=[registro])
        response_insert.raise_for_status()
        print(f"✅ INSERIDO: Registro com Carimbo '{carimbo_formatado}' inserido em '{tabela_destino}'.")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO CRÍTICO na inserção do Supabase. Resposta: ***{response_insert.text}***. Erro: {e}")
        return False

# --- FUNÇÃO PRINCIPAL DE BACKUP/MIGRAÇÃO (FIX DE SLICING APLICADO) ---

def fazer_migracao(gc, planilha_origem_id, aba_origem_name, tabela_destino_name):
    
    global SUPABASE_CARIMBO_KEY_DB, SUPABASE_VALOR_KEY, SUPABASE_QUANTIDADE_KEY

    print(f"\n--- Iniciando Migração SIMPLES: {aba_origem_name.upper()} para Supabase ({tabela_destino_name}) ---")
    
    try:
        planilha_origem = gc.open_by_key(planilha_origem_id).worksheet(aba_origem_name)
        dados_do_mes = planilha_origem.get_all_values()
        
        # 🚨 FIX CRÍTICO: Dados começam na Linha 3 (índice 2)
        dados_para_processar = dados_do_mes[2:] 

        print(f"DEBUG: Planilha '{aba_origem_name}' lida. Total de Linhas (Incl. Cabecalho e Título): {len(dados_do_mes)}")
        print(f"DEBUG: Total de Dados para Processar (Linha 3 em diante): {len(dados_para_processar)}")

        # ************ 📢 DEBUG CRÍTICO: IMPRIME OS HEADERS (LINHA 2) ************
        # Imprime a linha 2 (índice 1) para referência, mas não a usa para mapeamento
        if len(dados_do_mes) > 1:
            print(f"\n📢 DEBUG LIDO: CABEÇALHO (Linha 2): {dados_do_mes[1]}\n")
        # *************************************************************************
        

        if not dados_para_processar:
            print(f"Não há novos dados na aba '{aba_origem_name}' para migrar.")
            return

        inseridos_ou_ignorados = 0
        
        for linha in dados_para_processar:
            registro = {}
            
            # Novo Loop: Mapeia pelo ÍNDICE (idx) em vez do Header
            for idx, valor_sheet in enumerate(linha):
                
                coluna_supa = INDEX_MAP.get(idx)
                
                if coluna_supa:
                    valor_processado = valor_sheet
                    
                    # Aplica a limpeza apenas para as colunas de valor/quantidade
                    if coluna_supa in [SUPABASE_VALOR_KEY, SUPABASE_QUANTIDADE_KEY]:
                        valor_processado = clean_value(valor_sheet)
                    
                    # Se for a coluna de carimbo (índice 0), precisamos formatar AGORA.
                    elif coluna_supa == SUPABASE_CARIMBO_KEY_DB:
                         valor_processado = format_datetime_for_supabase(valor_sheet)


                    registro[coluna_supa] = valor_processado

            
            carimbo = registro.get(SUPABASE_CARIMBO_KEY_DB)
            if not carimbo: # O valor já deve vir formatado aqui
                continue 

            # ************ 📢 DEBUG CRÍTICO: IMPRIME O REGISTRO ANTES DE ENVIAR ************
            print(f"📢 DEBUG ENVIADO: REGISTRO PRONTO PARA INSERÇÃO: {registro}")
            # *****************************************************************************

            if registro and enviar_registro_simples(registro, tabela_destino_name):
                inseridos_ou_ignorados += 1
            
            time.sleep(0.1) 


        if inseridos_ou_ignorados > 0:
            print(f"✅ {inseridos_ou_ignorados} registros processados (inseridos ou ignorados) na aba '{aba_origem_name}'.")
        else:
             print("Nenhum registro foi processado ou encontrado para migração.")

        print("--- MIGRAÇÃO SIMPLES CONCLUÍDA ---")


    except gspread.exceptions.WorksheetNotFound as e:
        print(f"❌ ERRO DE ABA/WORKSHEET: A aba '{aba_origem_name}' não foi encontrada na planilha de ID: {planilha_origem_id}.")
        raise RuntimeError(f"Falha na validação da Planilha: {e}") 
    except Exception as e:
        print(f"❌ ERRO GRAVE durante a migração de {aba_origem_name} (Planilha ID: {planilha_origem_id}): {e}")
        raise


def main():
    """Função principal para orquestrar a execução."""
    
    FORCA_EXECUCAO = os.environ.get('FORCA_EXECUCAO_MANUAL', 'false').lower() == 'true'
    
    if FORCA_EXECUCAO:
         print("\n🚨 AGENTE DE BACKUP ATIVADO (MANUAL OVERRIDE) - Executando sob demanda...")
    else:
         print("\n🚀 AGENTE DE MIGRAÇÃO ATIVADO - Executando agendamento a cada 2 horas...")

    gc = autenticar_gspread()
    
    for key, config in MAP_MIGRATION.items():
        fazer_migracao(gc, 
                       config["planilha_id"], 
                       config["aba_nome"], 
                       config["tabela_supa"])
        
    print("\n✅ ORQUESTRAÇÃO DE MIGRAÇÃO CONCLUÍDA.")


if __name__ == "__main__":
    try:
        main()
    except Exception as final_e:
        print(f"\n### FALHA CRÍTICA DO AGENTE ###\n{final_e}")
        sys.exit(1)
