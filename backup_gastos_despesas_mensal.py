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

# --- CONFIGURAÇÕES GERAIS (NOVA ESTRUTURA PARA MÚLTIPLAS PLANILHAS) ---

# Mapeamento CRÍTICO: Define qual PLANILHA_ID e qual ABA_NOME usar para qual TABELA_SUPABASE
MAP_MIGRATION = {
    # VENDAS
    "vendas": {
        "planilha_id": "1ygApI7DemPMEjfRcZmR1LVU9ofHP-dkL71m59-USnuY", # ID CORRETO DE VENDAS
        "aba_nome": "Form Responses", # <-- CORRIGIDO PARA O NOME PADRÃO DA RESPOSTA DO FORM
        "tabela_supa": "vendas"
    }, 
    # DESPESAS (Gastos)
    "gastos": {
        "planilha_id": "1y2YlMaaVMb0K4XlT7rx5s7X_2iNGL8dMAOSOpX4y_FA", # ID CORRETO DE DESPESAS
        "aba_nome": "Form Responses", # <-- CORRIGIDO PARA O NOME PADRÃO DA RESPOSTA DO FORM
        "tabela_supa": "despesas"
    } 
}

# MAPA DE TRADUÇÃO (Sheets Column Header -> Supabase Column Name)
COLUNA_MAP = {
    "Carimbo de data/hora": "Carimbo de data/hora", 
    "PRODUTO": "PRODUTO",
    "QUANTIDADE": "QUANTIDADE",
    "VALOR": "VALOR",
    # ADICIONAL para VENDAS, caso você use:
    "SABORES": "PRODUTO",
    "DADOS DO COMPRADOR": "DADOS_DO_COMPRADOR",
    "TOTAL": "TOTAL",
}
# -----------------------------------------------------------


# --- FUNÇÕES DE CONEXÃO E UTILIDADE (Manter as mesmas) ---
# ... (autenticar_gspread, clean_value, enviar_registro_inteligente) ...
# ... (Para não poluir, use as funções completas da minha resposta anterior)

# --- FUNÇÃO PRINCIPAL DE BACKUP/MIGRAÇÃO (Mantenha o loop de iteração) ---

def fazer_migracao(gc, planilha_origem_id, aba_origem_name, tabela_destino_name):
    # USE A FUNÇÃO fazer_migracao COMPLETA DA MINHA ÚLTIMA RESPOSTA 
    # (A que tem os logs DEBUG: e o tratamento de cabeçalho)
    # ...
    pass # Substitua por toda a função fazer_migracao corrigida
    
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
        # AS FUNÇÕES QUE FALTAM PRECISAM ESTAR AQUI!
        main()
    except Exception as final_e:
        print(f"\n### FALHA CRÍTICA DO AGENTE ###\n{final_e}")
        sys.exit(1)
