# Instalar o pacote dplyr
#install.packages("dplyr")

# Carregando o pacote
library(dplyr)

# Lendo o arquivo CSV
df <- read.csv('dataset/dataset2.csv', header = TRUE, sep = ";", dec = ",")

# Visualizando o dataset carregado
df

head(df)

df %>% 
  group_by(Produto) %>%
    summarise(total = sum(Quantidade))

df %>% tally()


#df['Quantidade'].max()
df %>% 
  group_by(Quantidade) %>%
    top_n(n=1)

# Filtrando o produto FANTA e contabilizando a quantidade - Forma 1
df %>%  
  group_by(Produto) %>%
  filter(Produto == 'FANTA') %>%    
      summarise(total = sum(Quantidade))

# Filtrando o produto FANTA e contabilizando a quantidade - Forma 2
df %>%  
  filter(Produto == 'FANTA') %>%
    group_by(Produto) %>%
      summarise(total = sum(Quantidade))


# Verificando a quantidade mínima vendida por produto
df %>% 
  group_by(Produto) %>%
    summarise(min = min(Quantidade, na.rm=TRUE))


# Obtendo as quantidades minima, máxima, total e média de venda dos produtos agrupados
df %>%
  group_by(Produto) %>%
    summarise(min = min(Quantidade, na.rm=TRUE),
              max = max(Quantidade, na.rm=TRUE),
              sum = sum(Quantidade, na.rm=TRUE),
              mean = mean(Quantidade, na.rm=TRUE)
             )

# Multiplicando a Quantidade pelo Valor_Unitario para saber o Valor_Total de vendas por produto
df <- df %>%
        mutate(Valor_Total = Quantidade * Valor_Unitario)
df

# Sumarizando Quantidade, Valor_Unitario e Valor_Total por Produto
df %>%
  group_by(Produto) %>%
    summarise(Quantidade = sum(Quantidade),
              Valor_Unitario = sum(Valor_Unitario),
              Valor_Total = sum(Valor_Total)
             )

# Contabilizando o Valor_Total por Produto, Valor_Unitario e Quantidade
df %>%
  group_by(Produto, Valor_Unitario, Quantidade) %>%
    summarise(Valor_Total = sum(Valor_Total))