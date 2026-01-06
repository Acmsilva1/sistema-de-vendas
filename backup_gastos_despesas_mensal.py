import gspread
import os
import json
import sys
import time # NOVO: Para fazer pausas entre as chamadas (evita sobrecarga da API)
from datetime import datetime
import requests # Para fazer requisições HTTP (API Supabase)

# ===============================================
# 1. CONFIGURAÇÕES DO SUPABASE (Hardcoded)
# ===============================================
SUPABASE_URL = "https://uidlyplhksbwerbdgtys.supabase.co"
SUPABASE_KEY = "sb_publishable_kUFjQWo7t2d4NccZYi4E9Q_okgJ1DOe"

# --- CONFIGURAÇÕES GERAIS ---
PLANILHA_ORIGEM_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug" # Vendas e Gastos

# Mapeamento das Abas: {ABA_ORIGEM (minúscula): TABELA NO SUPABASE}
MAP_ABAS = {
    "vendas": "vendas", 
    "gastos": "despesas" 
}

# MAPA DE TRADUÇÃO (Sheets Column Header -> Supabase Column Name)
# Usando o mapeamento 1:1 'sujo' (necessário por causa do seu Supabase)
COLUNA_MAP = {
    "Carimbo de data/hora": "Carimbo de data/hora", 
    "PRODUTO": "PRODUTO",
    "QUANTIDADE": "QUANTIDADE",
    "VALOR": "VALOR"
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
    # 1. Remove separador de milhares (ponto)
    cleaned = cleaned.replace('.', '')
    # 2. Troca a vírgula pelo ponto
    cleaned = cleaned.replace(',', '.')
    
    try:
        return float(cleaned)
    except ValueError:
        return valor  

def enviar_registro_inteligente(registro, tabela_destino):
    """
    Tenta inserir um único registro. Primeiro, checa se o 'Carimbo de data/hora' já existe no Supabase.
    """
    carimbo = registro.get("Carimbo de data/hora")
    if not carimbo:
        print("⚠️ Ignorando registro sem 'Carimbo de data/hora' para checagem de duplicidade.")
        return False
    
    # 1. CHECAGEM (SELECT) - Verifica se o carimbo já existe
    # A URL de filtro é construída com o nome da coluna (que precisa estar no DB)
    url_check = f"{SUPABASE_URL}/rest/v1/{tabela_destino}?Carimbo de data/hora=eq.{carimbo}"
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}'
    }

    try:
        response_check = requests.get(url_check, headers=headers)
        response_check.raise_for_status()
        
        # Se a lista retornada não estiver vazia, o dado existe.
        if response_check.json():
            print(f"⏩ IGNORADO: Registro com Carimbo '{carimbo}' já existe na tabela '{tabela_destino}'.")
            return True # O dado está lá, consideramos processado com sucesso (ignorando)

    except requests.exceptions.RequestException as e:
        # Se falhar na checagem, não insere.
        print(f"❌ ERRO na checagem do Supabase para o Carimbo '{carimbo}': {e}")
        return False 

    # 2. INSERÇÃO (POST) - Se a checagem não encontrou nada
    url_insert = f"{SUPABASE_URL}/rest/v1/{tabela_destino}"
    headers['Content-Type'] = 'application/json'
    headers['Prefer'] = 'return=minimal'
    
    try:
        # Envia o registro como uma lista de um item (formato de inserção em lote de 1)
        response_insert = requests.post(url_insert, headers=headers, json=[registro])
        response_insert.raise_for_status()
        print(f"✅ INSERIDO: Registro com Carimbo '{carimbo}' inserido em '{tabela_destino}'.")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO na inserção do Supabase. Resposta: {response_insert.text}. Erro: {e}")
        return False

# --- FUNÇÃO PRINCIPAL DE BACKUP/MIGRAÇÃO ---

def fazer_migracao(gc, planilha_origem_id, aba_origem_name, tabela_destino_name):
    """
    Lê do Sheets, processa, envia um por um para o Supabase (com checagem de duplicidade) 
    e deleta as linhas processadas da origem.
    """
    print(f"\n--- Iniciando Migração Inteligente: {aba_origem_name.upper()} para Supabase ({tabela_destino_name}) ---")
    
    try:
        planilha_origem = gc.open_by_key(planilha_origem_id).worksheet(aba_origem_name)
        dados_do_mes = planilha_origem.get_all_values()
        
        headers = dados_do_mes[0]
        dados_para_processar = dados_do_mes[1:] 

        if not dados_para_processar:
            print(f"Não há novos dados na aba '{aba_origem_name}' para migrar.")
            return

        sucesso_ou_ignorado_count = 0
        
        # 3. Processamento, Limpeza e Inserção Inteligente (Iteração)
        for linha in dados_para_processar:
            registro = {}
            
            # Constrói o dicionário de registro (payload)
            for idx, valor_sheet in enumerate(linha):
                header_sheet = headers[idx]
                if header_sheet in COLUNA_MAP:
                    coluna_supa = COLUNA_MAP[header_sheet]
                    valor_processado = valor_sheet
                    
                    # Aplica a limpeza de formato APENAS na coluna VALOR
                    if header_sheet.upper() == "VALOR":
                        valor_processado = clean_value(valor_sheet)

                    registro[coluna_supa] = valor_processado

            # Tentativa de Inserção Inteligente
            if registro and enviar_registro_inteligente(registro, tabela_destino_name):
                # Se a inserção foi bem-sucedida OU o dado já existia (retornou True)
                sucesso_ou_ignorado_count += 1
            
            # Pequena pausa para evitar sobrecarga da API
            time.sleep(0.1) 


        # 4. LIMPEZA/DELEÇÃO DAS LINHAS PROCESSADAS
        if sucesso_ou_ignorado_count > 0:
            # Apaga a quantidade de linhas que foram processadas com sucesso (ou ignoradas)
            # A deleção começa na linha 2 (logo abaixo do cabeçalho) e apaga o número de linhas processadas.
            planilha_origem.delete_rows(2, sucesso_ou_ignorado_count) 
            print(f"✅ {sucesso_ou_ignorado_count} linhas processadas (inseridas ou ignoradas) e DELETADAS da aba '{aba_origem_name}'.")
        
        print("--- MIGRAÇÃO INTELIGENTE CONCLUÍDA ---")


    except gspread.exceptions.WorksheetNotFound as e:
        print(f"ERRO: A aba '{aba_origem_name}' não foi encontrada.")
        raise RuntimeError(f"Falha na validação da Planilha: {e}") 
    except Exception as e:
        print(f"ERRO GRAVE durante a migração de {aba_origem_name}: {e}")
        raise


def main():
    """Função principal para orquestrar a execução."""
    
    # Removida a lógica de checagem de data (executa sempre que o GitHub Actions mandar).
    
    # Verifica se a execução foi forçada manualmente (governança de tempo)
    FORCA_EXECUCAO = os.environ.get('FORCA_EXECUCAO_MANUAL', 'false').lower() == 'true'
    
    if FORCA_EXECUCAO:
         print("\n🚨 AGENTE DE BACKUP ATIVADO (MANUAL OVERRIDE) - Executando sob demanda...")
    else:
         print("\n🚀 AGENTE DE MIGRAÇÃO ATIVADO - Executando agendamento a cada 2 horas...")

    # 1. Autentica UMA VEZ no GSheets
    gc = autenticar_gspread()
    
    # 2. Executa a função de migração para Vendas e Gastos
    for origem, destino in MAP_ABAS.items():
        fazer_migracao(gc, PLANILHA_ORIGEM_ID, origem, destino)
        
    print("\n✅ ORQUESTRAÇÃO DE MIGRAÇÃO CONCLUÍDA.")


if __name__ == "__main__":
    try:
        main()
    except Exception as final_e:
        print(f"\n### FALHA CRÍTICA DO AGENTE ###\n{final_e}")
        sys.exit(1)
