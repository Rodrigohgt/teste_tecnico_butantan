#!/usr/bin/env python3
"""
Teste Técnico - Engenheiro de Dados - Fundação Butantan
Pipeline de dados para identificar último preço de compra de materiais
com conversão de moedas estrangeiras usando API do Banco Central.
"""

import pandas as pd
import requests
import json 
from datetime import datetime, timedelta 
import os
from typing import Dict, Optional, Tuple
import warnings

warnings.filterwarnings('ignore') #Ignora alertas desnecessarios.

# CONFIGURAÇÃO DOS CAMINHOS DOS ARQUIVOS
ARQUIVO_CABECALHO = "02_dados/cabecalho_pedido.csv"
ARQUIVO_ITENS = "02_dados/item_pedido.csv"
ARQUIVO_SAIDA = "relatorio_ultimo_preco_materiais.csv"

class ProcessadorPedidos:  #Classe principal para processar dados de pedidos e gerar relatorio consolidado.

    def __init__(self):

        # Usa as constantes definidas no início do arquivo
        self.arquivo_cabecalho = ARQUIVO_CABECALHO
        self.arquivo_itens = ARQUIVO_ITENS
        
        # DataFrames que serão preenchidos
        self.df_cabecalho = None
        self.df_itens = None
        self.df_consolidado = None
        
        #Cache para cotações
        self.cotacoes_cache = {}
        
    def carregar_dados(self) -> bool:

        #Carrega os dados diretamente dos arquivos especificados nas constantes.
       
        try:
            print("CARREGANDO DADOS")
            print(f"Arquivo cabeçalho: {self.arquivo_cabecalho}") #<-- utilizo prints para facilitar visualização de debug ao longo do codigo.
            print(f"Arquivo itens: {self.arquivo_itens}")
            
            # VERIFICAR SE ARQUIVOS EXISTEM ANTES DE TENTAR CARREGAR
            if not os.path.exists(self.arquivo_cabecalho):
                print(f"Arquivo não encontrado: {self.arquivo_cabecalho}")
                return False
                
            if not os.path.exists(self.arquivo_itens):
                print(f"Arquivo não encontrado: {self.arquivo_itens}")
                return False
            
            # CARREGAR OS DADOS
            print("Carregando cabeçalho de pedidos")
            self.df_cabecalho = pd.read_csv(self.arquivo_cabecalho)
            print(f"Cabeçalho carregado: {len(self.df_cabecalho):,} registros")
            
            print("Carregando itens de pedidos...")
            self.df_itens = pd.read_csv(self.arquivo_itens)
            print(f"Itens carregados: {len(self.df_itens):,} registros")
            
            # CONVERTER COLUNA DE DATA
            print("Convertendo datas...")
            self.df_cabecalho['data_pedido'] = pd.to_datetime(self.df_cabecalho['data_pedido'])
            print("Conversão de datas concluída")
            
            return True
            
        except Exception as e:
            print(f"ERRO ao carregar dados: {e}")
            return False
    
    def obter_cotacao_bcb(self, moeda: str, data_referencia: str) -> Optional[float]:

        #Obtém cotação de moeda estrangeira da API do Banco Central.
        # Cache - evita consultas repetidas
        if moeda in self.cotacoes_cache:
            return self.cotacoes_cache[moeda]
        
        # Para o Real brasileiro não precisamos de conversão :)
        if moeda == 'BRL':
            return 1.0
        
        try:
            # Preparar datas para a API
            data_obj = datetime.strptime(data_referencia, '%Y-%m-%d')
            data_inicial = data_obj.strftime('%m-%d-%Y')
            data_final_obj = data_obj + timedelta(days=30)
            data_final = data_final_obj.strftime('%m-%d-%Y')
            
            # URL da API do Banco Central
            url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda='{moeda}',dataInicial='{data_inicial}',dataFinalCotacao='{data_final}')"
            
            print(f"cotação: {moeda}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'value' in data and len(data['value']) > 0:
                cotacao_mais_recente = data['value'][-1]
                taxa_venda = cotacao_mais_recente.get('cotacaoVenda')
                
                if taxa_venda:
                    self.cotacoes_cache[moeda] = float(taxa_venda)
                    print(f"Cotação {moeda}: {taxa_venda}")
                    return float(taxa_venda)
            
            print(f"Cotação não encontrada para {moeda}")
            return None
            
        except Exception as e:
            print(f"Erro ao obter cotação para {moeda}: {e}")
            return None
    
    def processar_dados(self) -> bool:
        
        #Processa os dados para identificar último preço de compra de cada material.
        
        try:
            print("\nPROCESSANDO DADOS")
            
            # FAÇO OS JOIN ENTRE AS TABELAS
            print("Unindo dados de cabeçalho e itens")
            df_merged = self.df_itens.merge(
                self.df_cabecalho, 
                on='codigo_pedido', 
                how='inner'
            )
            print(f"Dados unidos: {len(df_merged):,} registros")
            
            # CALCULO O PREÇO UNITÁRIO
            print("Calculando preços unitarios")
            df_merged['preco_unitario'] = df_merged['valor_total_item_pedido'] / df_merged['item_quantidade']
            
            # ORDENO POR MATERIAL E DATA
            print("Ordenando dados por material e data")
            df_merged = df_merged.sort_values(['codigo_material', 'data_pedido'])
            
            # PEGO O ÚLTIMO PREÇO DE CADA MATERIAL
            print("Identificando último preço de cada material")
            df_ultimo_preco = df_merged.groupby('codigo_material').last().reset_index()
            print(f"Materiais unicos identificados: {len(df_ultimo_preco):,}")
            
            # PROCESSO PARA CADA MATERIAL
            print("\nIniciando conversão de moedas")
            resultados = []
            total_materiais = len(df_ultimo_preco)
            
            for i, (_, row) in enumerate(df_ultimo_preco.iterrows(), 1):
                material = row['codigo_material']
                preco_original = row['preco_unitario']
                moeda = row['moeda']
                data_pedido = row['data_pedido'].strftime('%Y-%m-%d')
                codigo_pedido = row['codigo_pedido']
                
                # MOSTRAR PROGRESSO A CADA 1000 MATERIAIS
                if i % 1000 == 0 or i == total_materiais:
                    print(f"Progresso: {i:,}/{total_materiais:,} materiais ({i/total_materiais*100:.1f}%)")
                
                # CONVERTER MOEDA SE NECESSÁRIO
                if moeda != 'BRL':
                    cotacao = self.obter_cotacao_bcb(moeda, data_pedido)
                    if cotacao:
                        preco_brl = preco_original * cotacao
                        data_cotacao = datetime.now().strftime('%Y-%m-%d')
                    else:
                        preco_brl = preco_original
                        data_cotacao = None
                else:
                    preco_brl = preco_original
                    data_cotacao = None
                
                # CRIAR REGISTRO DO RESULTADO
                resultado = {
                    'codigo_material': material,
                    'ultimo_preco_brl': round(preco_brl, 2),
                    'ultimo_preco_original': round(preco_original, 2),
                    'moeda_pedido': moeda,
                    'data_ultima_compra': data_pedido,
                    'codigo_pedido_referencia': codigo_pedido,
                    'data_cotacao': data_cotacao
                }
                
                resultados.append(resultado)
            
            # CRIAR DATAFRAME FINAL
            self.df_consolidado = pd.DataFrame(resultados)
            print(f"Processamento concluído: {len(self.df_consolidado):,} materiais processados")
            
            return True
            
        except Exception as e:
            print(f"Erro no processamento: {e}")
            return False
    
    def gerar_relatorio(self, arquivo_saida: str = None) -> bool:
        
        #Gera relatorio consolidado em formato CSV.
        
        try:
            if arquivo_saida is None:
                arquivo_saida = ARQUIVO_SAIDA
                
            if self.df_consolidado is None:
                print("Erro: Dados não processados.")
                return False
            
            print(f"\nGERANDO RELATÓRIO")
            print(f"Arquivo de saída: {arquivo_saida}")
            
            # ORDENAR POR CÓDIGO DO MATERIAL
            df_ordenado = self.df_consolidado.sort_values('codigo_material')
            
            # SALVAR EM CSV
            df_ordenado.to_csv(arquivo_saida, index=False, encoding='utf-8')
            
            print(f"Relatório gerado com sucesso!")
            print(f"Total de materiais: {len(df_ordenado):,}")
            
            # MOSTRAR ESTATÍSTICAS
            print("\n=== ESTATÍSTICAS ===")
            materiais_brl = len(df_ordenado[df_ordenado['moeda_pedido'] == 'BRL'])
            materiais_estrangeira = len(df_ordenado[df_ordenado['moeda_pedido'] != 'BRL'])
            
            print(f"Materiais em BRL: {materiais_brl:,}")
            print(f"Materiais em moeda estrangeira: {materiais_estrangeira:,}")
            
            print("\nDistribuição por moeda:")
            moedas_encontradas = df_ordenado['moeda_pedido'].value_counts()
            for moeda, count in moedas_encontradas.items():
                print(f"  {moeda}: {count:,} materiais")
            
            return True
            
        except Exception as e:
            print(f"Erro ao gerar relatório: {e}")
            return False
    
    def executar_pipeline_completo(self, arquivo_saida: str = None) -> bool:
        
        #Executa todo o pipeline de processamento.
        
        print("INICIANDO PIPELINE DE PROCESSAMENTO")
        
        # Executa todas as etapas em sequência com um DEBUG para facilitar e  encontrar erros.
        if not self.carregar_dados():
            print("Falha no carregamento de dados")
            return False
        
        if not self.processar_dados():
            print("Falha no processamento de dados")
            return False
        
        if not self.gerar_relatorio(arquivo_saida):
            print("Falha na geração do relatório")
            return False
        
        print("\nPIPELINE CONCLUÍDO COM SUCESSO")
        return True


# EXECUÇÃO PRINCIPAL 

def main():
   
    #Função principal - execução simplificada sem argumentos de linha de comando.
    
    print("Teste Técnico - Fundação Butantan")
    print("Pipeline de Análise de Preços de Materiais")
    print("=" * 50)
    
    # CRIAR PROCESSADOR E EXECUTAR
    processador = ProcessadorPedidos()
    sucesso = processador.executar_pipeline_completo()
    
    if sucesso:
        print(f"\nRelatório gerado: {ARQUIVO_SAIDA}")
        print("O arquivo contém o último preço de compra de cada material")
        print("Com conversões de moeda quando necessário")
    else:
        print("\nFALHA na execução do pipeline")
        print("Verifique os caminhos dos arquivos nas constantes do início do código")

if __name__ == "__main__":
    main()
