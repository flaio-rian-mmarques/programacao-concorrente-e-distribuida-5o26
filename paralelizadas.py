import threading as td
import pandas as pd
import glob as gb
import numpy as np
import os
import time

ROOT_DATABASE = "./base"
df_list = []
lock = td.Lock()

def concat_files():
    def append_csv(path):
        read_df = pd.read_csv(path)
        with lock:
            df_list.append(read_df)
    
    df_list.clear()

    files = gb.glob("*.csv", root_dir=f"{ROOT_DATABASE}")
    thread_list = []

    for f in files:
        path = f"{ROOT_DATABASE}/{f}"
        thread = td.Thread(target=append_csv, args=(path, ))
        thread.start()
        thread_list.append(thread)

    for t in thread_list:
        t.join()
    
    if df_list:
        table = pd.concat(df_list)
        table.to_csv('concat_p.csv', index=False)
        print("Concatenado!")

def generate_summary_municipio():
    if not os.path.exists("./concat_p.csv"):
        concat_files()
    df = pd.read_csv("./concat_p.csv")

    unique_municipios = df['municipio_oj'].unique()
    municipios_batch = np.array_split(unique_municipios, 4)
    partial_results = []

    def process_batch(batch):
        filtered_df = df[df["municipio_oj"].isin(batch)]
        summary = filtered_df.groupby('municipio_oj').sum(numeric_only=True)
        summary['Meta1'] = (100 * summary['julgados_2026'] / (summary['casos_novos_2026'] + summary['dessobrestados_2026'] - summary['suspensos_2026']))
        summary['Meta2A'] = ((summary['julgm2_a'] / (summary['distm2_a'] - summary['suspm2_a'])) * 1000/7)
        summary['Meta2Ant'] = (100 * summary['julgm2_ant'] / (summary['distm2_ant'] - summary['suspm2_ant'] - summary['desom2_ant']))
        summary['Meta4A'] = (100 * summary['julgm4_a'] / (summary['distm4_a'] - summary['suspm4_a']))
        summary['Meta4B'] = (100 * summary['julgm4_b'] / (summary['distm4_b'] - summary['suspm4_b']))

        with lock:
            partial_results.append(summary)

    thread_list = []
    
    for batch in municipios_batch:
        thread = td.Thread(target=process_batch, args=(batch, ))
        thread.start()
        thread_list.append(thread)

    for t in thread_list:
        t.join()

    if partial_results:
        final_summary = pd.concat(partial_results)
        final_summary.to_csv('summary_p.csv', index=False)
        print("Resumo gerado!")

def generate_summary_top10_tribunais():
    if not os.path.exists("./concat_p.csv"):
        concat_files()
    df = pd.read_csv("./concat_p.csv")

    unique_tribunais = df['sigla_tribunal'].unique()
    tribunais_batch = np.array_split(unique_tribunais, 4)
    partial_results = []

    def process_batch(batch):
        filtered_df = df[df["sigla_tribunal"].isin(batch)]
        summary = filtered_df.groupby('sigla_tribunal').sum(numeric_only=True)
        summary['Meta1'] = (100 * summary['julgados_2026'] / (summary['casos_novos_2026'] + summary['dessobrestados_2026'] - summary['suspensos_2026']))
        summary['Meta2A'] = ((summary['julgm2_a'] / (summary['distm2_a'] - summary['suspm2_a'])) * 1000/7)
        summary['Meta2Ant'] = (100 * summary['julgm2_ant'] / (summary['distm2_ant'] - summary['suspm2_ant'] - summary['desom2_ant']))
        summary['Meta4A'] = (100 * summary['julgm4_a'] / (summary['distm4_a'] - summary['suspm4_a']))
        summary['Meta4B'] = (100 * summary['julgm4_b'] / (summary['distm4_b'] - summary['suspm4_b']))

        with lock:
            partial_results.append(summary)

    thread_list = []
    
    for batch in tribunais_batch:
        thread = td.Thread(target=process_batch, args=(batch, ))
        thread.start()
        thread_list.append(thread)

    for t in thread_list:
        t.join()

    if partial_results:
        final_summary = pd.concat(partial_results)
        top10 = final_summary.sort_values(by='Meta1', ascending=False).head(10)
        top10.to_csv('top10_tribunais_p.csv')
        print("Top10 gerado!")

def generate_csv_filtered_municipio():
    if not os.path.exists("./concat_p.csv"):
        concat_files()
    df = pd.read_csv("./concat_p.csv")
    filter = input("Digite o nome do municipio: ")
##    filter = "MACAPA"
    partial_results = []

    def process_chunk(chunk):
        filtered_df = chunk[chunk["municipio_oj"] == filter.upper()]
        with lock:
            partial_results.append(filtered_df)
    
    thread_list = []
    chunk_size = len(df) // 4
    for i in range(4):
        if (i != 3):
            chunk = df.iloc[(i*chunk_size):((i+1)*chunk_size)]
        else:
            chunk = df.iloc[(i*chunk_size):]
        thread = td.Thread(target=process_chunk, args=(chunk, ))
        thread.start()
        thread_list.append(thread)

    for t in thread_list:
        t.join()

    if partial_results:
        final_summary = pd.concat(partial_results)
        final_summary.to_csv(f"{filter.upper()}_p.csv")
        print("Filtro gerado!")

start = time.time()
##concat_files()
##generate_summary_municipio()
##generate_summary_top10_tribunais()
##generate_csv_filtered_municipio()
end = time.time()

time = end - start
print(f"Tempo decorrido:", time)