# Carregando o pacote
import pandas as pd

# Lendo o arquivo CSV
df = pd.read_csv('dataset/dataset2.csv', sep=';', header=0, decimal=',')

# Visualizando o dataset carregado
df

df = df.set_index('Codigo')
# 
df.head()

df.groupby(['Produto']).sum()

df['Produto'].count()

df['Quantidade'].max()

# Filtrando o produto FANTA e contabilizando a quantidade - Forma 1
df['Quantidade'][df['Produto'] == 'FANTA'].sum()

# Filtrando o produto FANTA e contabilizando a quantidade - Forma 2
df[df['Produto'] == 'FANTA'].groupby('Produto').sum()

# Verificando a quantidade mínima vendida por produto
df.groupby(['Produto']).agg({"Quantidade": 'min'})

# Obtendo as quantidades minima, máxima, total e média de venda dos produtos agrupados
df.groupby(['Produto']).agg({"Quantidade": ['min','max','sum', 'mean']})

# Multiplicando a Quantidade pelo Valor_Unitario para saber o Valor_Total de vendas por produto
df['Valor_Total'] = df.Quantidade * df.Valor_Unitario

# Sumarizando Quantidade, Valor_Unitario e Valor_Total por Produto
df.groupby('Produto').sum()

# Contabilizando o Valor_Total por Produto, Valor_Unitario e Quantidade
df.groupby(['Produto', 'Valor_Unitario', 'Quantidade'])['Valor_Total'].sum()