import pandas as pd
import glob as gb
import os
import time

ROOT_DATABASE = "./base"

def concat_files():
    global ROOT_DATABASE
    files_list = gb.glob("*.csv", root_dir=f"./{ROOT_DATABASE}")
    df_list = []

    for i in files_list:
        path = f"{ROOT_DATABASE}/{i}"
        df_list.append(pd.read_csv(path))

    table = pd.concat(df_list)
    table.to_csv('concat.csv', index=False)
    print("Concatenado!")

def generate_summary_municipio():    
    if not os.path.exists("./concat.csv"):
        concat_files()

    df = pd.read_csv("./concat.csv")
    summary = df.groupby('municipio_oj').sum(numeric_only=True)
    
    summary['Meta1'] = (100 * summary['julgados_2026'] / (summary['casos_novos_2026'] + summary['dessobrestados_2026'] - summary['suspensos_2026']))
    summary['Meta2A'] = ((summary['julgm2_a'] / (summary['distm2_a'] - summary['suspm2_a'])) * 1000/7)
    summary['Meta2Ant'] = (100 * summary['julgm2_ant'] / (summary['distm2_ant'] - summary['suspm2_ant'] - summary['desom2_ant']))
    summary['Meta4A'] = (100 * summary['julgm4_a'] / (summary['distm4_a'] - summary['suspm4_a']))
    summary['Meta4B'] = (100 * summary['julgm4_b'] / (summary['distm4_b'] - summary['suspm4_b']))
    
    summary.to_csv('summary.csv', index=False)
    print("Resumo gerado!")

def generate_summary_top10_tribunais():
    if not os.path.exists("./concat.csv"):
        concat_files()
    
    df = pd.read_csv("./concat.csv")
    summary = df.groupby('sigla_tribunal').sum(numeric_only=True)
    
    summary['Meta1'] = (100 * summary['julgados_2026'] / (summary['casos_novos_2026'] + summary['dessobrestados_2026'] - summary['suspensos_2026']))
    summary['Meta2A'] = ((summary['julgm2_a'] / (summary['distm2_a'] - summary['suspm2_a'])) * 1000/7)
    summary['Meta2Ant'] = (100 * summary['julgm2_ant'] / (summary['distm2_ant'] - summary['suspm2_ant'] - summary['desom2_ant']))
    summary['Meta4A'] = (100 * summary['julgm4_a'] / (summary['distm4_a'] - summary['suspm4_a']))
    summary['Meta4B'] = (100 * summary['julgm4_b'] / (summary['distm4_b'] - summary['suspm4_b']))
    
    top10 = summary.sort_values(by='Meta1', ascending=False).head(10)
    top10.to_csv('top10_tribunais.csv')
    print("Top10 gerado!")

def generate_csv_filtered_municipio():
    if not os.path.exists("./concat.csv"):
        concat_files()
    df = pd.read_csv("./concat.csv")
    filter = input("Digite o nome do municipio: ")
##    filter = "MACAPA"
    filtered_csv = df[df['municipio_oj'] == filter.upper()]
    filtered_csv.to_csv(f"{filter.upper()}.csv", index=False)
    print("Filtro gerado!")

start = time.time()
##concat_files()
##generate_summary_municipio()
##generate_summary_top10_tribunais()
##generate_csv_filtered_municipio()
end = time.time()

time = end - start
print(f"Tempo decorrido:", time)