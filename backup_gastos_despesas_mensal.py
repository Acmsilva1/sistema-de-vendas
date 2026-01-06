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

# --- CONSTANTE CRÍTICA ---
# Chave da coluna Carimbo de Data/Hora no Supabase (usamos snake_case para evitar 400)
SUPABASE_CARIMBO_KEY = "carimbo_de_data_hora"

# --- CONFIGURAÇÕES GERAIS (Mapeamento Planilhas e Abas) ---

MAP_MIGRATION = {
    # VENDAS
    "vendas": {
        "planilha_id": "1ygApI7DemPMEjfRcZmR1LVU9ofHP-dkL71m59-USnuY", 
        "aba_nome": "VENDAS", 
        "tabela_supa": "vendas"
    }, 
    # DESPESAS (Gastos)
    "gastos": {
        "planilha_id": "1y2YlMaaVMb0K4XlT7rx5s7X_2iNGL8dMAOSOpX4y_FA", 
        "aba_nome": "despesas", 
        "tabela_supa": "despesas"
    } 
}

# MAPA DE TRADUÇÃO (Sheets Column Header -> Supabase Column Name)
COLUNA_MAP = {
    "Carimbo de data/hora": SUPABASE_CARIMBO_KEY, 
    "PRODUTO": "PRODUTO",
    "QUANTIDADE": "QUANTIDADE",
    "VALOR": "VALOR",
    "SABORES": "PRODUTO", 
    "DADOS DO COMPRADOR": "DADOS_DO_COMPRADOR",
    "TOTAL": "TOTAL",
}
# -----------------------------------------------------------


# --- FUNÇÕES DE CONEXÃO E UTILIDADE ---

def autenticar_gspread():
    """Autentica o gspread usando a variável de ambiente."""
    credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')
    
    if not credenciais_json_string:
        raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada!")

    try:
        credenciais_dict = json.loads(credenciais_json_string)
        return gspread.service_account_from_dict(credenciais_dict)
    except Exception as e:
        raise Exception(f"Erro ao carregar ou autenticar credenciais JSON: {e}")

def clean_value(valor):
    """Tradutor cultural: Converte valores com vírgula (R$) para o formato de ponto decimal (DB)."""
    if not valor or str(valor).strip() == '':
        return None
    
    cleaned = str(valor)
    cleaned = cleaned.replace('.', '')
    cleaned = cleaned.replace(',', '.')
    
    try:
        return float(cleaned)
    except ValueError:
        return valor  

def format_datetime_for_supabase(carimbo_str):
    """
    FIX CRÍTICO: Converte o formato 'DD/MM/YYYY HH:MM:SS' (BR) 
    para 'YYYY-MM-DDTHH:MM:SS' (ISO 8601 estrito com 'T') para o Supabase.
    """
    if not isinstance(carimbo_str, str) or not carimbo_str.strip():
        return None
        
    try:
        dt_obj = datetime.strptime(carimbo_str.strip(), '%d/%m/%Y %H:%M:%S')
        # AQUI ESTÁ O FIX: Mudando de espaço para 'T'
        return dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return None

def enviar_registro_inteligente(registro, tabela_destino):
    """
    Tenta inserir um único registro. Primeiro, checa se o 'Carimbo de data/hora' já existe no Supabase.
    """
    global SUPABASE_CARIMBO_KEY 

    carimbo_sheets_value = registro.get(SUPABASE_CARIMBO_KEY) 
    
    # 1. FORMATAR o Carimbo para uso no Supabase
    carimbo_formatado = format_datetime_for_supabase(carimbo_sheets_value)
    
    if not carimbo_formatado:
         return True 

    # 2. CHECAGEM (SELECT) - Agora usando a chave snake_case e o valor formatado com 'T'
    url_check = f"{SUPABASE_URL}/rest/v1/{tabela_destino}?{SUPABASE_CARIMBO_KEY}=eq.{carimbo_formatado}"
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}'
    }

    try:
        response_check = requests.get(url_check, headers=headers)
        response_check.raise_for_status()
        
        if response_check.json():
            print(f"⏩ IGNORADO: Registro com Carimbo '{carimbo_formatado}' já existe na tabela '{tabela_destino}'.")
            return True 

    except requests.exceptions.RequestException as e:
        # Se a checagem falhou (ex: 400 Bad Request), NÃO assumimos que o registro existe.
        # Imprimimos o erro, mas NÃO retornamos False para tentar a inserção.
        # Vamos reverter a lógica para garantir que a falha na checagem não impeça a inserção:
        # Se falhar aqui, tentamos inserir abaixo.
        print(f"⚠️ AVISO: Falha na checagem do Supabase (código {response_check.status_code if 'response_check' in locals() else 'N/A'}) para o Carimbo '{carimbo_formatado}'. Tentando inserção...")
        
        # Pula para a inserção (passo 3)
        pass 

    # 3. INSERÇÃO (POST) - Atualiza o registro com o valor formatado (para garantir)
    registro[SUPABASE_CARIMBO_KEY] = carimbo_formatado
    
    url_insert = f"{SUPABASE_URL}/rest/v1/{tabela_destino}"
    headers['Content-Type'] = 'application/json'
    headers['Prefer'] = 'return=minimal'
    
    try:
        response_insert = requests.post(url_insert, headers=headers, json=[registro])
        response_insert.raise_for_status()
        print(f"✅ INSERIDO: Registro com Carimbo '{carimbo_formatado}' inserido em '{tabela_destino}'.")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO CRÍTICO na inserção do Supabase. Resposta: {response_insert.text}. Erro: {e}")
        return False

# --- FUNÇÃO PRINCIPAL DE BACKUP/MIGRAÇÃO (COM FIX DE CABEÇALHO) ---

def fazer_migracao(gc, planilha_origem_id, aba_origem_name, tabela_destino_name):
    """
    Lê do Sheets, processa, envia um por um para o Supabase (com checagem de duplicidade) 
    e NÃO deleta as linhas da origem.
    """
    global SUPABASE_CARIMBO_KEY 

    print(f"\n--- Iniciando Migração Inteligente: {aba_origem_name.upper()} para Supabase ({tabela_destino_name}) ---")
    
    try:
        planilha_origem = gc.open_by_key(planilha_origem_id).worksheet(aba_origem_name)
        dados_do_mes = planilha_origem.get_all_values()
        
        # 1. Limpeza e Força de Cabeçalho
        headers = [h.strip() for h in dados_do_mes[0]] 
        
        # Garante que o primeiro cabeçalho seja o nome esperado (Sheets)
        if len(headers) > 0:
            headers[0] = "Carimbo de data/hora" 
            
        dados_para_processar = dados_do_mes[1:] 

        print(f"DEBUG: Planilha '{aba_origem_name}' lida. Total de Linhas (Incl. Cabecalho): {len(dados_do_mes)}")
        print(f"DEBUG: Total de Dados para Processar (Excl. Cabecalho): {len(dados_para_processar)}")

        if not dados_para_processar:
            print(f"Não há novos dados na aba '{aba_origem_name}' para migrar.")
            return

        inseridos_ou_ignorados = 0
        
        # 3. Processamento, Limpeza e Inserção Inteligente (Iteração)
        for linha in dados_para_processar:
            registro = {}
            
            # Constrói o dicionário de registro (payload)
            for idx, valor_sheet in enumerate(linha):
                
                if idx >= len(headers):
                    continue
                    
                header_sheet = headers[idx]
                
                if header_sheet in COLUNA_MAP:
                    coluna_supa = COLUNA_MAP[header_sheet]
                    valor_processado = valor_sheet
                    
                    # Se não for o carimbo, aplica o clean_value para VALOR/QUANTIDADE
                    if coluna_supa != SUPABASE_CARIMBO_KEY and (coluna_supa == "VALOR" or coluna_supa == "QUANTIDADE"):
                        valor_processado = clean_value(valor_sheet)

                    registro[coluna_supa] = valor_processado

            
            # CHECK CRÍTICO: Se a linha é válida (tem o carimbo)
            carimbo = registro.get(SUPABASE_CARIMBO_KEY)
            if not carimbo or str(carimbo).strip() == '':
                continue 

            # Tentativa de Inserção Inteligente
            if registro and enviar_registro_inteligente(registro, tabela_destino_name):
                inseridos_ou_ignorados += 1
            
            time.sleep(0.1) 


        # 4. Finalização (Sem deleção)
        if inseridos_ou_ignorados > 0:
            print(f"✅ {inseridos_ou_ignorados} registros processados (inseridos ou ignorados) na aba '{aba_origem_name}'.")
        else:
             print("Nenhum registro foi processado ou encontrado para migração.")

        print("--- MIGRAÇÃO INTELIGENTE CONCLUÍDA ---")


    except gspread.exceptions.WorksheetNotFound as e:
        print(f"❌ ERRO DE ABA/WORKSHEET: A aba '{aba_origem_name}' não foi encontrada na planilha de ID: {planilha_origem_id}. CONFIRA SE O NOME DA ABA ESTÁ EXATAMENTE: 'VENDAS' ou 'despesas'.")
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

    # 1. Autentica UMA VEZ no GSheets
    gc = autenticar_gspread()
    
    # 2. Executa a função de migração para Vendas e Gastos (AGORA USANDO OS IDs E CONFIGS CORRETAS)
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
