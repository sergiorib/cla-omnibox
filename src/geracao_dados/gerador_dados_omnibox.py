import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import uuid
import datetime

# =====================================================================
# 1. CONFIGURAÇÃO DE DIRETÓRIOS E CAMINHOS
# =====================================================================

DIRETORIO_ATUAL = Path(__file__).resolve()
DIRETORIO_BASE = DIRETORIO_ATUAL.parents[2] 

BASE_DIR = DIRETORIO_BASE / "dados" / "gerado"

DADOS_DIR = BASE_DIR 
GA_DIR = DADOS_DIR / "google_analytics"

DADOS_DIR.mkdir(parents=True, exist_ok=True)
GA_DIR.mkdir(parents=True, exist_ok=True)

print("Diretórios configurados e validados.")

# =====================================================================
# 2. CARREGAMENTO DAS TABELAS AUXILIARES E HIGIENIZAÇÃO
# =====================================================================
print("Carregando matrizes e cadastros...")

df_category = pd.read_parquet(DADOS_DIR  / "category.parquet")
df_sub_category = pd.read_parquet(DADOS_DIR  / "sub_category.parquet")
df_products = pd.read_parquet(DADOS_DIR  / "products.parquet")

ARQUIVO_PARAMETROS = DIRETORIO_BASE / "config" / "parametros_geracao_dados.xlsx"

df_parametros = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="parametros")
df_municipios = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="municipios")
df_fretes = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="fretes")
df_reajustes = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="reajustes")
df_participacoes = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="participacoes")

# --- INÍCIO DA CORREÇÃO DE HIGIENIZAÇÃO (FRETES E UFS) ---
df_municipios.columns = df_municipios.columns.astype(str).str.strip().str.lower()
if 'uf' in df_municipios.columns:
    df_municipios['uf'] = df_municipios['uf'].astype(str).str.strip().str.upper()
if 'região' in df_municipios.columns:
    df_municipios.rename(columns={'região': 'regiao'}, inplace=True)
if 'regiao' in df_municipios.columns:
    df_municipios['regiao'] = df_municipios['regiao'].astype(str).str.strip().str.upper()

df_fretes.columns = df_fretes.columns.astype(str).str.strip().str.lower()
if 'região' in df_fretes.columns:  
    df_fretes.rename(columns={'região': 'regiao'}, inplace=True)
if 'regiao' in df_fretes.columns:
    df_fretes['regiao'] = df_fretes['regiao'].astype(str).str.strip().str.upper()

# Garante que números com vírgula do Excel (PT-BR) virem decimais no Python
for col in ['base', 'multiplicador']:
    if col in df_fretes.columns and df_fretes[col].dtype == object:
        df_fretes[col] = df_fretes[col].astype(str).str.replace(',', '.').astype(float)
# --- FIM DA CORREÇÃO ---

df_escala_vis = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="escala_visualizacao")
df_escala_car = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="escala_carrinho")
df_escala_chk = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="escala_checkout")
df_escala_cnv = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="escala_conversao")

df_mkt_bau = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="mkt_canais_bau")
df_mkt_camp = pd.read_excel(ARQUIVO_PARAMETROS, sheet_name="mkt_campanhas")

df_reajustes['data'] = pd.to_datetime(df_reajustes['data'], dayfirst=True)
df_reajustes = df_reajustes.sort_values('data')

# Limpeza colunas Marketing
df_mkt_bau.columns = df_mkt_bau.columns.astype(str).str.strip().str.lower()
df_mkt_camp.columns = df_mkt_camp.columns.astype(str).str.strip().str.lower()
df_mkt_camp['inicio'] = pd.to_datetime(df_mkt_camp['inicio'], dayfirst=True)
df_mkt_camp['fim'] = pd.to_datetime(df_mkt_camp['fim'], dayfirst=True)

print("Tabelas carregadas.")

# =====================================================================
# 3. FUNÇÕES AUXILIARES DE LIMPEZA E CÁLCULO
# =====================================================================
def extrair_lista(valor, tipo=str):
    if pd.isna(valor) or str(valor).strip() == '': return []
    v_str = str(valor).strip()
    if isinstance(valor, float):
        v_str = v_str.replace('.', ',')
    v_str = v_str.replace(';', ',')
    lista_final = []
    for x in v_str.split(','):
        x = x.strip()
        if not x: continue
        try:
            if tipo == int:
                lista_final.append(int(float(x)))
            else:
                lista_final.append(tipo(x))
        except ValueError:
            continue
    return lista_final

def limpar_percentual(valor, default=0.0):
    if pd.isna(valor) or str(valor).strip() == '': return default
    if isinstance(valor, (int, float)): 
        v = float(valor)
    else:
        v_str = str(valor).strip()
        tem_pct = '%' in v_str
        v_str = v_str.replace('%', '').replace(',', '.').strip()
        try:
            v = float(v_str)
            if tem_pct: v = v / 100.0
        except ValueError:
            return default
    if v > 1.0: v = v / 100.0
    return max(0.0, min(1.0, v))

def calcular_volume_bruto(df_escala, alvo, volume_base):
    if pd.isna(alvo) or volume_base <= 0: return 0
    if isinstance(alvo, (int, float)): return int(min(alvo, volume_base))
        
    alvo = str(alvo).strip().lower()
    colunas = df_escala.columns.astype(str).str.lower().str.strip()
    col_cenario = next((c for c in colunas if 'cenario' in c or 'target' in c or 'escala' in c), colunas[0])
    col_min = next((c for c in colunas if 'min' in c or 'de' in c), colunas[1])
    col_max = next((c for c in colunas if 'max' in c or 'ate' in c or 'até' in c), colunas[2])
    
    df_temp = df_escala.copy()
    df_temp.columns = colunas
    
    linha = df_temp[df_temp[col_cenario].astype(str).str.strip().str.lower() == alvo]
    if linha.empty: return 0
        
    pct_min = limpar_percentual(linha[col_min].values[0])
    pct_max = limpar_percentual(linha[col_max].values[0])
    
    pct_sorteado = np.random.uniform(pct_min, pct_max)
    return np.random.binomial(volume_base, pct_sorteado)

NOMES = ['Ana', 'Bruno', 'Carlos', 'Daniela', 'Eduardo', 'Fernanda', 'Gabriel', 'Helena', 'Igor', 'Julia', 'Lucas', 'Mariana', 'Pedro', 'Sofia']
SOBRENOMES = ['Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues', 'Ferreira', 'Alves', 'Pereira', 'Lima', 'Gomes', 'Costa', 'Ribeiro']

# =====================================================================
# 4. PREPARAÇÃO DE EXECUÇÃO E ESTRUTURAS DE MARKETING
# =====================================================================
df_parametros.columns = df_parametros.columns.astype(str).str.strip().str.lower()
df_execucao = df_parametros[df_parametros['processar'].astype(str).str.strip().str.upper() == 'S'].copy()

last_customer_id = 0
last_order_id = 0
last_item_id = 0

customers_writer = None
orders_writer = None
items_writer = None

# --- COLD START (CARREGAR HISTÓRICO DE CLIENTES SE EXISTIR) ---
ARQUIVO_CUSTOMERS = DADOS_DIR / "customers.parquet"
if ARQUIVO_CUSTOMERS.exists():
    print("Lendo histórico de clientes (customers.parquet)...")
    df_customers_global = pd.read_parquet(ARQUIVO_CUSTOMERS)
    
    # Limpa os estados antigos para bater perfeitamente no cruzamento do frete
    if 'state' in df_customers_global.columns:
        df_customers_global['state'] = df_customers_global['state'].astype(str).str.strip().str.upper()
        
    if not df_customers_global.empty:
        last_customer_id = df_customers_global['customer_id'].max()
        # Grava os clientes antigos no novo arquivo para manter o histórico
        table_customers_old = pa.Table.from_pandas(df_customers_global)
        customers_writer = pq.ParquetWriter(ARQUIVO_CUSTOMERS, table_customers_old.schema)
        customers_writer.write_table(table_customers_old)
        print(f"{len(df_customers_global)} clientes antigos carregados. Iniciando novos IDs a partir de {last_customer_id + 1}.")
else:
    df_customers_global = pd.DataFrame(columns=['customer_id', 'first_name', 'last_name', 'email', 'gender', 'birth_date', 'city', 'state', 'signup_date'])

# --- CONSTRUÇÃO DO DICIONÁRIO E REGRAS DE MARKETING ---
print("Estruturando tabelas de Marketing e Atribuição...")
mkt_campaigns_list = []
mkt_actions_list = []
mkt_spend_records = []

# 1. Campanha BAU (Base)
mkt_campaigns_list.append({
    'campaign_id': 1, 'name': 'Business As Usual', 'code': 'BAU',
    'description': 'Tráfego de base (Orgânico + Pago)'
})

action_id_counter = 1
for _, row in df_mkt_bau.iterrows():
    source = str(row['source']).strip().lower()
    mkt_actions_list.append({
        'action_id': action_id_counter, 'campaign_id': 1,
        'action_code': f"BAU-{source.upper()}-{str(row.get('objective', 'always on')).strip().upper().replace(' ', '')}", 'source': source,
        'medium': str(row['medium']).strip().lower(),
        'objective': str(row.get('objective', 'always on')).strip().lower(),
        'start_date': pd.to_datetime('2024-01-01').date(), 
        'end_date': pd.to_datetime('2026-12-31').date(),   
        'base_share': limpar_percentual(row['share']),
        'cpc': float(str(row['cpc']).replace(',','.')) if pd.notna(row['cpc']) else 0.0,
        'ctr': float(str(row['ctr']).replace(',','.')) if pd.notna(row['ctr']) else 0.0,
        'inicio': pd.to_datetime('2024-01-01'), 'fim': pd.to_datetime('2026-12-31')
    })
    action_id_counter += 1

# 2. Campanhas Sazonais
camp_id_counter = 2
camp_code_to_id = {}

for _, row in df_mkt_camp.iterrows():
    code = str(row['codigo_campanha']).strip().upper()
    if code not in camp_code_to_id:
        camp_code_to_id[code] = camp_id_counter
        mkt_campaigns_list.append({
            'campaign_id': camp_id_counter, 'name': str(row['nome']).strip(),
            'code': code, 'description': f"Campanha Sazonal: {code}"
        })
        camp_id_counter += 1

    cid = camp_code_to_id[code]
    source = str(row['source']).strip().lower()
    mkt_actions_list.append({
        'action_id': action_id_counter, 'campaign_id': cid,
        'action_code': f"{code}-{source.upper()}-{str(row.get('objetivo', 'conversion')).strip().upper().replace(' ', '')}", 'source': source,
        'medium': str(row['medium']).strip().lower(),
        'objective': str(row.get('objetivo', 'conversion')).strip().lower(),
        'start_date': row['inicio'].date(), 
        'end_date': row['fim'].date(),       
        'base_share': limpar_percentual(row['share']),
        'cpc': float(str(row['cpc']).replace(',','.')) if pd.notna(row['cpc']) else 0.0,
        'ctr': float(str(row['ctr']).replace(',','.')) if pd.notna(row['ctr']) else 0.0,
        'inicio': row['inicio'], 'fim': row['fim']
    })
    action_id_counter += 1

df_mkt_actions_config = pd.DataFrame(mkt_actions_list)

print(f"Linhas habilitadas no Motor: {len(df_execucao)}")

# =====================================================================
# 5. O MOTOR PRINCIPAL (LOOP MÊS A MÊS E DIA A DIA)
# =====================================================================
for index, row in df_execucao.iterrows():
    seq_str = str(row['sequencia']).zfill(3)
    
    inicio_dt = pd.to_datetime(row['inicio'], dayfirst=True).normalize()
    fim_dt = pd.to_datetime(row['fim'], dayfirst=True).normalize()
    
    print(f"\nProcessando sequência {seq_str} (Período: {inicio_dt.date()} a {fim_dt.date()})")
    
    tx_novos = limpar_percentual(row.get('clientes_novos'), 0.65)
    tx_canc = limpar_percentual(row.get('cancelamentos'), 0.005)
    tx_dev = limpar_percentual(row.get('devolucoes'), 0.01)
    tx_aumento_itens = limpar_percentual(row.get('aumento_items'), 0.0)
    tx_atraso = limpar_percentual(row.get('atraso entrega', row.get('atraso_entrega')), 0.01)
    
    frete_val = row.get('frete gratis')
    frete_gratis_val = str(frete_val).strip().upper() if pd.notna(frete_val) and str(frete_val).strip() != '' else 'NAO'
    frete_gratis = True if frete_gratis_val in ['SIM', 'S', 'TRUE', '1'] else False
    
    lista_ufs = extrair_lista(row.get('uf'), str)
    col_subcat = 'sub-categoria' if 'sub-categoria' in row else ('sub_categoria' if 'sub_categoria' in row else None)
    lista_subcats = extrair_lista(row.get(col_subcat), int) if col_subcat else []
    lista_prods = extrair_lista(row.get('produto'), int)
    
    vol_sessoes_total = int(row['visitas']) if pd.notna(row['visitas']) else 0
    if vol_sessoes_total <= 0: continue
    
    datas_periodo = pd.date_range(start=inicio_dt, end=fim_dt)
    prob_dias = np.ones(len(datas_periodo)) / len(datas_periodo)
    visitas_por_dia = np.random.multinomial(vol_sessoes_total, prob_dias) 
    
    bases_lista = []
    tot_cnv = 0
    
    # -----------------------------------------------------------------
    # BLOCO 1: GOOGLE ANALYTICS E ATRIBUIÇÃO DE MARKETING
    # -----------------------------------------------------------------
    for i, data_atual in enumerate(datas_periodo):
        vol_dia = visitas_por_dia[i]
        if vol_dia == 0: continue
        
        # --- ENGENHARIA REVERSA DO MARKETING (SESSÕES E CUSTOS) ---
        active_seasonal = df_mkt_actions_config[
            (df_mkt_actions_config['campaign_id'] > 1) &
            (df_mkt_actions_config['inicio'] <= data_atual) &
            (df_mkt_actions_config['fim'] >= data_atual)
        ]
        
        seasonal_share_total = active_seasonal['base_share'].sum() if not active_seasonal.empty else 0.0
        seasonal_share_total = min(1.0, seasonal_share_total)
        bau_multiplier = 1.0 - seasonal_share_total
        
        actions_today = []
        probs_today = []
        
        bau_actions = df_mkt_actions_config[df_mkt_actions_config['campaign_id'] == 1]
        for _, act in bau_actions.iterrows():
            actions_today.append(act['action_id'])
            probs_today.append(act['base_share'] * bau_multiplier)
            
        if not active_seasonal.empty:
            for _, act in active_seasonal.iterrows():
                actions_today.append(act['action_id'])
                probs_today.append(act['base_share'])
                
        probs_today = np.array(probs_today) / np.sum(probs_today)
        sess_action_counts = np.random.multinomial(vol_dia, probs_today)
        
        assigned_actions = np.repeat(actions_today, sess_action_counts)
        np.random.shuffle(assigned_actions)
        
        for act_id, count in zip(actions_today, sess_action_counts):
            if count == 0: continue
            act_info = df_mkt_actions_config[df_mkt_actions_config['action_id'] == act_id].iloc[0]
            
            if act_info['cpc'] > 0:
                clicks = int(count * np.random.uniform(1.01, 1.05))
                impressions = int(clicks / act_info['ctr']) if act_info['ctr'] > 0 else clicks * 100
                spend = clicks * act_info['cpc']
            else:
                clicks = count
                impressions = count
                spend = 0.0
                
            mkt_spend_records.append({
                'date': data_atual.date(),
                'action_id': act_id,
                'spend_amount': spend,
                'clicks': clicks,
                'impressions': impressions
            })
        # ------------------------------------------------------------
        
        raw_consultas = calcular_volume_bruto(df_escala_vis, row['consultas'], vol_dia)
        raw_carrinhos = calcular_volume_bruto(df_escala_car, row['carrinho'], vol_dia)
        raw_checkouts = calcular_volume_bruto(df_escala_chk, row['checkout'], vol_dia)
        raw_conversoes = calcular_volume_bruto(df_escala_cnv, row['conversao'], vol_dia)
        
        vol_consultas = min(vol_dia, raw_consultas)
        vol_carrinhos = min(vol_consultas, raw_carrinhos)
        vol_checkouts = min(vol_carrinhos, raw_checkouts)
        vol_conversoes = min(vol_checkouts, raw_conversoes)
        tot_cnv += vol_conversoes
        
        sessoes_ids = [str(uuid.uuid4()) for _ in range(vol_dia)]
        pseudo_ids = [str(uuid.uuid4())[:18] for _ in range(vol_dia)]
        
        estagios = np.zeros(vol_dia, dtype=int)
        idx_total = np.arange(vol_dia)
        
        idx_view = np.random.choice(idx_total, vol_consultas, replace=False)
        estagios[idx_view] = 1
        
        if vol_carrinhos > 0:
            idx_cart = np.random.choice(idx_view, vol_carrinhos, replace=False)
            estagios[idx_cart] = 2
            
        if vol_checkouts > 0:
            idx_chk = np.random.choice(idx_cart, vol_checkouts, replace=False)
            estagios[idx_chk] = 3
            
        if vol_conversoes > 0:
            idx_conv = np.random.choice(idx_chk, vol_conversoes, replace=False)
            estagios[idx_conv] = 4
            
        segundos_add = np.random.randint(0, 86400, size=vol_dia)
        base_timestamps = data_atual + pd.to_timedelta(segundos_add, unit='s')
        
        df_base_dia = pd.DataFrame({
            'ga_session_id': sessoes_ids,
            'user_pseudo_id': pseudo_ids,
            'base_timestamp': base_timestamps,
            'max_stage': estagios,
            'action_id': assigned_actions
        })
        bases_lista.append(df_base_dia)
        
    if not bases_lista: continue
    
    df_base = pd.concat(bases_lista, ignore_index=True)
    eventos_lista = []

    df_start = df_base.copy()
    df_start['event_name'] = 'session_start'
    df_start['time_offset'] = 0
    eventos_lista.append(df_start)

    df_view = df_base[df_base['max_stage'] >= 1].copy()
    df_view['event_name'] = 'view_item'
    df_view['time_offset'] = np.random.randint(10, 60, size=len(df_view))
    eventos_lista.append(df_view)

    df_cart = df_base[df_base['max_stage'] >= 2].copy()
    df_cart['event_name'] = 'add_to_cart'
    df_cart['time_offset'] = np.random.randint(65, 180, size=len(df_cart))
    eventos_lista.append(df_cart)

    df_chk = df_base[df_base['max_stage'] >= 3].copy()
    df_chk['event_name'] = 'begin_checkout'
    df_chk['time_offset'] = np.random.randint(185, 300, size=len(df_chk))
    eventos_lista.append(df_chk)

    df_pur = df_base[df_base['max_stage'] >= 4].copy()
    df_pur['event_name'] = 'purchase'
    df_pur['time_offset'] = np.random.randint(310, 500, size=len(df_pur))
    df_pur['transaction_id'] = [f"TXN-{seq_str}-{str(uuid.uuid4())[:8].upper()}" for _ in range(len(df_pur))]
    eventos_lista.append(df_pur)

    df_events = pd.concat(eventos_lista, ignore_index=True)
    df_events['final_timestamp'] = df_events['base_timestamp'] + pd.to_timedelta(df_events['time_offset'], unit='s')
    
    # -----------------------------------------------------------------
    # BLOCO 2 E 3: CLIENTES E VENDAS
    # -----------------------------------------------------------------
    compras_df = df_events[df_events['event_name'] == 'purchase'].copy()
    
    if len(compras_df) > 0:
        print(f"      -> Processando {len(compras_df)} vendas para a tabela Orders e Customers.")
        
        qtd_novos = np.random.binomial(len(compras_df), tx_novos)
        if len(df_customers_global) == 0: qtd_novos = len(compras_df)
        qtd_retorno = len(compras_df) - qtd_novos
        
        novos_ids = []
        
        df_mun_filtrado = df_municipios.copy()
        if lista_ufs:
            df_mun_filtrado = df_mun_filtrado[df_mun_filtrado['uf'].str.upper().isin([u.upper() for u in lista_ufs])]
            if df_mun_filtrado.empty: df_mun_filtrado = df_municipios
        
        if qtd_novos > 0:
            novos_ids = np.arange(last_customer_id + 1, last_customer_id + 1 + qtd_novos)
            last_customer_id = novos_ids[-1]
            
            pesos_cidade = df_mun_filtrado['populacao'] / df_mun_filtrado['populacao'].sum()
            idx_cidades = np.random.choice(df_mun_filtrado.index, size=qtd_novos, p=pesos_cidade)
            cidades_sorteadas = df_mun_filtrado.loc[idx_cidades, 'municipio'].values
            ufs_sorteadas = df_mun_filtrado.loc[idx_cidades, 'uf'].values
            
            df_novos_clientes = pd.DataFrame({
                'customer_id': novos_ids,
                'first_name': np.random.choice(NOMES, qtd_novos),
                'last_name': np.random.choice(SOBRENOMES, qtd_novos),
                'gender': np.random.choice(['F', 'M'], qtd_novos, p=[0.75, 0.25]),
                'city': cidades_sorteadas,
                'state': ufs_sorteadas,
            })
            df_novos_clientes['email'] = df_novos_clientes['first_name'].str.lower() + "." + df_novos_clientes['last_name'].str.lower() + "@email.com"
            df_novos_clientes['birth_date'] = pd.to_datetime('1990-01-01') + pd.to_timedelta(np.random.randint(-5000, 5000, qtd_novos), unit='D')
            df_novos_clientes['signup_date'] = compras_df['final_timestamp'].dt.date.values[:qtd_novos]
            
            # ---------------------------------------------------
            # CORREÇÃO DE SCHEMA PARQUET (Tipos e Ordem)
            # ---------------------------------------------------
            df_novos_clientes['customer_id'] = df_novos_clientes['customer_id'].astype('int32')
            df_novos_clientes['birth_date'] = df_novos_clientes['birth_date'].dt.date
            
            df_novos_clientes['first_name'] = df_novos_clientes['first_name'].astype('string')
            df_novos_clientes['last_name'] = df_novos_clientes['last_name'].astype('string')
            df_novos_clientes['gender'] = df_novos_clientes['gender'].astype('string')
            df_novos_clientes['city'] = df_novos_clientes['city'].astype('string')
            df_novos_clientes['state'] = df_novos_clientes['state'].astype('string')
            df_novos_clientes['email'] = df_novos_clientes['email'].astype('string')
            
            colunas_ordem = ['customer_id', 'first_name', 'last_name', 'email', 'gender', 'birth_date', 'city', 'state', 'signup_date']
            df_novos_clientes = df_novos_clientes[colunas_ordem]
            # ---------------------------------------------------
            
            df_customers_global = pd.concat([df_customers_global, df_novos_clientes], ignore_index=True)
            
            table_customers = pa.Table.from_pandas(df_novos_clientes)
            if customers_writer is None: customers_writer = pq.ParquetWriter(DADOS_DIR / "customers.parquet", table_customers.schema)
            customers_writer.write_table(table_customers)
            
        clientes_atribuidos = []
        if qtd_novos > 0: clientes_atribuidos.extend(novos_ids)
        
        if qtd_retorno > 0:
            clientes_validos = df_customers_global['customer_id'].values
            if lista_ufs and len(df_customers_global) > 0:
                c_filtrados = df_customers_global[df_customers_global['state'].str.upper().isin([u.upper() for u in lista_ufs])]['customer_id'].values
                if len(c_filtrados) > 0: clientes_validos = c_filtrados
            
            clientes_atribuidos.extend(np.random.choice(clientes_validos, qtd_retorno))
            
        np.random.shuffle(clientes_atribuidos)
        compras_df['customer_id'] = clientes_atribuidos
        
        compras_df = pd.merge(compras_df, df_customers_global[['customer_id', 'state']], on='customer_id', how='left')
        mapa_sessao_cliente = compras_df.set_index('ga_session_id')[['customer_id', 'state']].to_dict('index')
        
        df_events['user_id'] = df_events['ga_session_id'].map(lambda x: mapa_sessao_cliente[x]['customer_id'] if x in mapa_sessao_cliente else None)
        df_events['region'] = df_events['ga_session_id'].map(lambda x: mapa_sessao_cliente[x]['state'] if x in mapa_sessao_cliente else None)

        qtd_orders = len(compras_df)
        order_ids = np.arange(last_order_id + 1, last_order_id + 1 + qtd_orders)
        last_order_id = order_ids[-1]
        
        compras_df['order_id'] = order_ids
        
        probs_status = np.random.uniform(0, 1, qtd_orders)
        status_arr = np.where(probs_status < tx_canc, 'cancelled', 
                     np.where(probs_status < (tx_canc + tx_dev), 'returned', 'completed'))
        
        # ---------------------------------------------------
        # CORREÇÃO DO FRETE (PONTE UF -> REGIÃO)
        # ---------------------------------------------------
        mapa_uf_regiao = df_municipios.drop_duplicates('uf').set_index('uf')['regiao'].to_dict()
        compras_df['regiao_cliente'] = compras_df['state'].map(mapa_uf_regiao)
        compras_df = pd.merge(compras_df, df_fretes, left_on='regiao_cliente', right_on='regiao', how='left')
        custo_frete = compras_df['base'] * compras_df['multiplicador']
        custo_frete = custo_frete.fillna(0.0) 
        # ---------------------------------------------------
        
        dias_para_entrega = np.where(
            np.random.uniform(0, 1, qtd_orders) < tx_atraso,
            np.random.randint(6, 15, qtd_orders),
            np.random.randint(1, 5, qtd_orders)
        )
        
        data_envio = compras_df['final_timestamp'].dt.date + pd.Timedelta(days=1)
        
        map_act_code = dict(zip(df_mkt_actions_config['action_id'], df_mkt_actions_config['action_code']))
        compras_df['action_code'] = compras_df['action_id'].map(map_act_code).astype('string')
        
        df_orders = pd.DataFrame({
            'order_id': order_ids,
            'customer_id': compras_df['customer_id'],
            'order_date': compras_df['final_timestamp'].dt.date,
            'shipped_date': data_envio,
            'delivery_date': data_envio + pd.to_timedelta(dias_para_entrega, unit='D'),
            'order_status': status_arr,
            'payment_method': np.random.choice(['credit_card', 'pix', 'boleto'], qtd_orders, p=[0.6, 0.3, 0.1]),
            'transaction_id': compras_df['transaction_id'],
            'action_code': compras_df['action_code'], 
            'free_shipping': 1 if frete_gratis else 0,
            'shipping_cost': custo_frete.round(2),
            'discount': 0.0,
            'net_amount': 0.0
        })

        df_orders['is_new_customer'] = df_orders['customer_id'].isin(novos_ids)

        # -----------------------------------------------------------------
        # BLOCO 4: ITENS DO PEDIDO, CATÁLOGO E DESCONTO
        # -----------------------------------------------------------------
        probs_base = np.array([0.60, 0.25, 0.10, 0.03, 0.02])
        if tx_aumento_itens > 0:
            shift = probs_base[0] * tx_aumento_itens
            probs_base[0] -= shift
            probs_base[1:] += shift * (probs_base[1:] / probs_base[1:].sum())
            
        itens_por_pedido = np.random.choice([1, 2, 3, 4, 5], size=qtd_orders, p=probs_base)
        total_itens = itens_por_pedido.sum()
        
        ordem_repetida = np.repeat(df_orders['order_id'].values, itens_por_pedido)
        data_repetida = np.repeat(pd.to_datetime(df_orders['order_date']).values, itens_por_pedido)
        
        item_ids = np.arange(last_item_id + 1, last_item_id + 1 + total_itens)
        last_item_id = item_ids[-1]
        
        df_items = pd.DataFrame({
            'order_item_id': item_ids,
            'order_id': ordem_repetida,
            'order_date_dt': data_repetida,
        })
        
        df_items['quantity'] = np.random.choice([1, 2, 3], size=total_itens, p=[0.85, 0.10, 0.05])
        
        df_part_filtrado = df_participacoes.copy()
        if lista_subcats:
            df_part_filtrado = df_part_filtrado[df_part_filtrado['sub_category_id'].isin(lista_subcats)]
            if df_part_filtrado.empty: df_part_filtrado = df_participacoes
            
        pesos_subcat = df_part_filtrado['participacao'] / df_part_filtrado['participacao'].sum()
        subcats_sorteadas = np.random.choice(df_part_filtrado['sub_category_id'], size=total_itens, p=pesos_subcat)
        
        prod_map = df_products.groupby('sub_category_id')['product_id'].apply(list).to_dict()
        produtos_finais = []
        for sc in subcats_sorteadas:
            opcoes_prod = prod_map.get(sc, [101])
            if lista_prods:
                opcoes_validas = [p for p in opcoes_prod if p in lista_prods]
                if opcoes_validas: opcoes_prod = opcoes_validas
            produtos_finais.append(np.random.choice(opcoes_prod))
            
        df_items['product_id'] = produtos_finais
        
        df_items = df_items.sort_values('order_date_dt')
        df_items = pd.merge_asof(
            df_items, 
            df_reajustes[['data', 'product_id', 'custo', 'markup']], 
            left_on='order_date_dt', right_on='data', by='product_id', direction='backward'
        )
        
        df_items = pd.merge(df_items, df_products[['product_id', 'cost', 'markup']], on='product_id', how='left', suffixes=('', '_base'))
        
        df_items['unit_cost'] = df_items['custo'].fillna(df_items['cost']).fillna(0.0).round(2)
        df_items['markup_final'] = df_items['markup'].fillna(df_items['markup_base']).fillna(1.0)
        df_items['price'] = (df_items['unit_cost'] * df_items['markup_final']).round(2)
        
        tx_desconto = limpar_percentual(row.get('desconto'), 0.0)

        # 1. Cálculo limpo do valor do item (sem desconto)
        df_items['item_value'] = (df_items['price'] * df_items['quantity']).round(2)
        
        # 2. Agrupa os totais do pedido para calcular o desconto na Orders (ALTERADO: total_item_value para gross_amount)
        totais_pedido = df_items.groupby('order_id').agg(
            gross_amount=('item_value', 'sum'),
            total_quantity=('quantity', 'sum')
        ).reset_index()
        
        df_orders = pd.merge(df_orders.drop(columns=['discount', 'net_amount']), totais_pedido, on='order_id', how='left')
        
        # 3. Lógica de Desconto aplicada APENAS no Pedido (ALTERADO: referência para gross_amount)
        tx_desconto = limpar_percentual(row.get('desconto'), 0.0)
        criterio_val = row.get('criterio_desconto')
        criterio_desc = str(criterio_val).strip().lower() if pd.notna(criterio_val) and str(criterio_val).strip() != '' else 'todos'
        
        df_orders['discount'] = 0.00
        if tx_desconto > 0:
            if 'antigo' in criterio_desc:
                aplicar_desconto = ~df_orders['is_new_customer']
            elif 'mais' in criterio_desc and 'item' in criterio_desc:
                aplicar_desconto = df_orders['total_quantity'] > 1
            else:
                aplicar_desconto = pd.Series(True, index=df_orders.index)
                
            df_orders['discount'] = np.where(aplicar_desconto, (df_orders['gross_amount'] * tx_desconto).round(2), 0.00)
            
        # 4. Cálculo final do net_amount do Pedido (ALTERADO: referência para gross_amount)
        df_orders['net_amount'] = (df_orders['gross_amount'] - df_orders['discount']).round(2)
        df_orders['shipping_cost'] = df_orders['shipping_cost'].fillna(0.0).round(2)
        
        df_orders['net_amount'] = np.where(
            df_orders['free_shipping'] == 1,
            df_orders['net_amount'], 
            df_orders['net_amount'] + df_orders['shipping_cost']
        ).round(2)
        
        # Limpa colunas auxiliares que não vão para o Parquet (ALTERADO: gross_amount removido do drop)
        df_orders.drop(columns=['is_new_customer', 'total_quantity'], inplace=True, errors='ignore')
        
        if 'is_new_customer' in df_orders.columns:
            df_orders.drop(columns=['is_new_customer'], inplace=True)
        
        # ---------------------------------------------------
        # FORÇAR TIPAGEM ESTRITA
        # ---------------------------------------------------
        df_orders['order_id'] = df_orders['order_id'].astype('int32')
        df_orders['customer_id'] = df_orders['customer_id'].astype('int32')
        df_orders['free_shipping'] = df_orders['free_shipping'].astype('int8')
        df_orders['order_status'] = df_orders['order_status'].astype('string')
        df_orders['payment_method'] = df_orders['payment_method'].astype('string')
        df_orders['transaction_id'] = df_orders['transaction_id'].astype('string')
        df_orders['action_code'] = df_orders['action_code'].astype('string') 
        df_orders['shipping_cost'] = df_orders['shipping_cost'].astype('float64')
        df_orders['gross_amount'] = df_orders['gross_amount'].astype('float64') # ALTERADO: tipagem forte do gross_amount
        df_orders['discount'] = df_orders['discount'].astype('float64')
        df_orders['net_amount'] = df_orders['net_amount'].astype('float64')
        
        # Tabela de itens atualizada
        df_items = df_items[['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_cost', 'price', 'item_value']]
        df_items['order_item_id'] = df_items['order_item_id'].astype('int32')
        df_items['order_id'] = df_items['order_id'].astype('int32')
        df_items['product_id'] = df_items['product_id'].astype('int32')
        df_items['quantity'] = df_items['quantity'].astype('int32')
        df_items['unit_cost'] = df_items['unit_cost'].astype('float64')
        df_items['price'] = df_items['price'].astype('float64')
        df_items['item_value'] = df_items['item_value'].astype('float64')
        
        table_orders = pa.Table.from_pandas(df_orders)
        
        decimal_type = pa.decimal128(12, 2)
        # ALTERADO: gross_amount incluso na lista de casting do PyArrow
        for col in ['shipping_cost', 'gross_amount', 'discount', 'net_amount']:
            idx = table_orders.schema.get_field_index(col)
            new_schema = table_orders.schema.set(idx, table_orders.schema.field(idx).with_type(decimal_type))
            table_orders = table_orders.cast(new_schema)
            
        if orders_writer is None: orders_writer = pq.ParquetWriter(DADOS_DIR / "orders.parquet", table_orders.schema)
        orders_writer.write_table(table_orders)
        
        table_items = pa.Table.from_pandas(df_items)
        
        # Converter os valores dos itens para DECIMAL
        for col in ['unit_cost', 'price', 'item_value']:
            idx = table_items.schema.get_field_index(col)
            new_schema = table_items.schema.set(idx, table_items.schema.field(idx).with_type(decimal_type))
            table_items = table_items.cast(new_schema)
            
        if items_writer is None: items_writer = pq.ParquetWriter(DADOS_DIR / "order_items.parquet", table_items.schema)
        items_writer.write_table(table_items)

    else:
        df_events['user_id'] = None
        df_events['region'] = None

    # -----------------------------------------------------------------
    # SALVAR GOOGLE ANALYTICS COM ATRIBUIÇÃO UTM
    # -----------------------------------------------------------------
    df_camp_ref = pd.DataFrame(mkt_campaigns_list)
    map_act_camp = dict(zip(df_mkt_actions_config['action_id'], df_mkt_actions_config['campaign_id'].map(dict(zip(df_camp_ref['campaign_id'], df_camp_ref['code'])))))
    map_act_src = dict(zip(df_mkt_actions_config['action_id'], df_mkt_actions_config['source']))
    map_act_med = dict(zip(df_mkt_actions_config['action_id'], df_mkt_actions_config['medium']))

    df_ga_final = pd.DataFrame()
    df_ga_final['event_name'] = df_events['event_name'].astype('string')
    df_ga_final['event_date'] = df_events['final_timestamp'].dt.date
    # CORREÇÃO: Divisão por 10**3 transforma nanosegundos (Pandas) em microssegundos (Padrão GA4)
    df_ga_final['event_timestamp'] = (df_events['final_timestamp'].astype('int64') // 10**3).astype('int64')
    df_ga_final['user_id'] = df_events['user_id'].astype('Int32') 
    df_ga_final['user_pseudo_id'] = df_events['user_pseudo_id'].astype('string')
    df_ga_final['region'] = df_events['region'].astype('string')
    df_ga_final['ga_session_id'] = df_events['ga_session_id'].astype('string')
    df_ga_final['product_id'] = pd.Series(dtype='string')
    df_ga_final['page_location'] = pd.Series(dtype='string')
    
    df_ga_final['source'] = df_events['action_id'].map(map_act_src).fillna('direct').astype('string')
    df_ga_final['medium'] = df_events['action_id'].map(map_act_med).fillna('(none)').astype('string')
    df_ga_final['campaign'] = df_events['action_id'].map(map_act_camp).fillna('BAU').astype('string')
    
    if 'transaction_id' in df_events.columns:
        df_ga_final['transaction_id'] = df_events['transaction_id'].astype('string')
    else:
        df_ga_final['transaction_id'] = pd.Series(dtype='string')
        
    df_ga_final['campaign_id'] = pd.Series(dtype='Int32')

    colunas_schema = [
        'event_name', 'event_date', 'event_timestamp', 'user_id', 
        'user_pseudo_id', 'region', 'ga_session_id', 'product_id', 
        'page_location', 'source', 'medium', 'transaction_id', 
        'campaign', 'campaign_id'
    ]
    df_ga_final = df_ga_final[colunas_schema]
    df_ga_final = df_ga_final.sort_values(by=['ga_session_id', 'event_timestamp'])

    arquivo_ga = GA_DIR / f"google_analytics_{seq_str}.parquet"
    df_ga_final.to_parquet(arquivo_ga, index=False)
    
# =====================================================================
# 6. CONSOLIDAÇÃO DE MARKETING E FECHAMENTO
# =====================================================================
if customers_writer is not None: customers_writer.close()
if orders_writer is not None: orders_writer.close()
if items_writer is not None: items_writer.close()

print("\n Processando Custos e Orçamentos de Marketing...")
df_mkt_spend = pd.DataFrame(mkt_spend_records)

if not df_mkt_spend.empty:
    action_spend = df_mkt_spend.groupby('action_id')['spend_amount'].sum().reset_index()
    action_spend['budget'] = (action_spend['spend_amount'] * np.random.uniform(1.02, 1.10)).round(2)

    df_mkt_actions = pd.merge(df_mkt_actions_config[['action_id', 'campaign_id', 'action_code', 'objective', 'medium', 'source', 'start_date', 'end_date']], action_spend[['action_id', 'budget']], on='action_id', how='left')
    df_mkt_actions['budget'] = df_mkt_actions['budget'].fillna(0.0).round(2)

    campaign_spend = df_mkt_actions.groupby('campaign_id')['budget'].sum().reset_index()
    
    campaign_dates = df_mkt_actions.groupby('campaign_id').agg({'start_date': 'min', 'end_date': 'max'}).reset_index()
    
    df_mkt_campaigns = pd.DataFrame(mkt_campaigns_list)[['campaign_id', 'name', 'code', 'description']]
    df_mkt_campaigns = pd.merge(df_mkt_campaigns, campaign_dates, on='campaign_id', how='left')
    df_mkt_campaigns = pd.merge(df_mkt_campaigns, campaign_spend, on='campaign_id', how='left')
    df_mkt_campaigns['budget'] = df_mkt_campaigns['budget'].fillna(0.0).round(2)

    # ---------------------------------------------------
    # TIPAGEM FORTE E GRAVAÇÃO PARQUET (MARKETING)
    # ---------------------------------------------------
    df_mkt_campaigns['campaign_id'] = df_mkt_campaigns['campaign_id'].astype('int64')
    df_mkt_campaigns['name'] = df_mkt_campaigns['name'].astype('string')
    df_mkt_campaigns['code'] = df_mkt_campaigns['code'].astype('string')
    df_mkt_campaigns['start_date'] = pd.to_datetime(df_mkt_campaigns['start_date']).dt.date
    df_mkt_campaigns['end_date'] = pd.to_datetime(df_mkt_campaigns['end_date']).dt.date
    df_mkt_campaigns['description'] = df_mkt_campaigns['description'].astype('string')
    df_mkt_campaigns['budget'] = df_mkt_campaigns['budget'].astype('float64')

    df_mkt_actions['action_id'] = df_mkt_actions['action_id'].astype('int64')
    df_mkt_actions['campaign_id'] = df_mkt_actions['campaign_id'].astype('int64')
    df_mkt_actions['action_code'] = df_mkt_actions['action_code'].astype('string')
    df_mkt_actions['objective'] = df_mkt_actions['objective'].astype('string')
    df_mkt_actions['source'] = df_mkt_actions['source'].astype('string')
    df_mkt_actions['medium'] = df_mkt_actions['medium'].astype('string')
    df_mkt_actions['start_date'] = pd.to_datetime(df_mkt_actions['start_date']).dt.date
    df_mkt_actions['end_date'] = pd.to_datetime(df_mkt_actions['end_date']).dt.date
    df_mkt_actions['budget'] = df_mkt_actions['budget'].astype('float64')

    df_mkt_spend['action_id'] = df_mkt_spend['action_id'].astype('int32')
    df_mkt_spend['clicks'] = df_mkt_spend['clicks'].astype('int32')
    df_mkt_spend['impressions'] = df_mkt_spend['impressions'].astype('int32')
    df_mkt_spend['spend_amount'] = df_mkt_spend['spend_amount'].astype('float64')

    table_camp = pa.Table.from_pandas(df_mkt_campaigns)
    table_act = pa.Table.from_pandas(df_mkt_actions)
    table_spd = pa.Table.from_pandas(df_mkt_spend)

    decimal_mkt = pa.decimal128(18, 3)
    
    idx_camp = table_camp.schema.get_field_index('budget')
    table_camp = table_camp.cast(table_camp.schema.set(idx_camp, table_camp.schema.field(idx_camp).with_type(decimal_mkt)))
    
    idx_act = table_act.schema.get_field_index('budget')
    table_act = table_act.cast(table_act.schema.set(idx_act, table_act.schema.field(idx_act).with_type(decimal_mkt)))
    
    idx_spd = table_spd.schema.get_field_index('spend_amount')
    table_spd = table_spd.cast(table_spd.schema.set(idx_spd, table_spd.schema.field(idx_spd).with_type(decimal_mkt)))

    pq.write_table(table_camp, DADOS_DIR / "mkt_campaigns.parquet")
    pq.write_table(table_act, DADOS_DIR / "mkt_actions.parquet")
    pq.write_table(table_spd, DADOS_DIR / "mkt_spend.parquet")

print("\n Geração Completa do Funil Total: Visitas -> Marketing -> Vendas!")