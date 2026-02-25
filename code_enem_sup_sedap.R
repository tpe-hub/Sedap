gc()
rm(list = ls())

library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(openxlsx)
library(writexl)

# -----------------------------
# Bases
# -----------------------------
PATH_ENEM <- "raw/ENEM.csv"  # Alterar para diretorio do dado"
PATH_SUP  <- "raw/SUP_ALUNO.csv" # Alterar para diretorio do dado"
PATH_CINE <- "raw/AUX_CINE_BRASIL.csv" # Alterar para diretorio do dado"


# -----------------------------
# ENEM: 
# -----------------------------
enem <- readr::read_csv(PATH_ENEM, show_col_types = FALSE) %>%
  transmute(
    NU_ANO = as.numeric(NU_ANO), # Ano do Enem
    CPF_MASC = as.character(CPF_MASC), #Conector Censo Superior
    TP_ST_CONCLUSAO = as.character(TP_ST_CONCLUSAO), #Situação de conclusão do aluno na Ed. Básica
    NU_NOTA_CN = as.numeric(NU_NOTA_CN), # Nota Ciências Natureza
    NU_NOTA_CH = as.numeric(NU_NOTA_CH), # Nota Ciências Humana
    NU_NOTA_LC = as.numeric(NU_NOTA_LC), # Nota Linguegens e Códigos
    NU_NOTA_MT = as.numeric(NU_NOTA_MT), # Nota Matemática
    NU_NOTA_REDACAO = as.numeric(NU_NOTA_REDACAO)) %>% # Nota Redação
  mutate(
    NOTA_MEDIA = if_else(
      !is.na(NU_NOTA_CN) & !is.na(NU_NOTA_CH) & !is.na(NU_NOTA_LC) &
        !is.na(NU_NOTA_MT) & !is.na(NU_NOTA_REDACAO),
      (NU_NOTA_CN + NU_NOTA_CH + NU_NOTA_LC + NU_NOTA_MT + NU_NOTA_REDACAO) / 5,NA_real_)) # média simples das cinco notas do Enem, calculada apenas quando todas as cinco notas são não ausentes; caso contrário, definida como NA.

# -----------------------------
# Cortes (q3, q4, q5) pela base ENEM
# -----------------------------
cortes <- enem %>%
  summarise( # na.rm = TRUE: ignora casos em que NOTA_MEDIA é NA; type = 7: método padrão do R para cálculo de quantis
    q3 = as.numeric(quantile(NOTA_MEDIA, probs = 2/3, na.rm = TRUE, type = 7)), #q3: ponto de corte do tercil superior (percentil 66,7%)
    q4 = as.numeric(quantile(NOTA_MEDIA, probs = 3/4, na.rm = TRUE, type = 7)), # q4: ponto de corte do quartil superior (percentil 75%)
    q5 = as.numeric(quantile(NOTA_MEDIA, probs = 4/5, na.rm = TRUE, type = 7))) # q5: ponto de corte do quintil superior (percentil 80%)

# Salva os cortes em objetos separados para facilitar o uso em mutates/sumários
q3 <- cortes$q3
q4 <- cortes$q4
q5 <- cortes$q5
print(cortes) # Confere os valores dos cortes calculados

# -----------------------------
# AUX_CINE_BRASIL: ler e deduplicar chave
# -----------------------------
cine <- readr::read_csv(PATH_CINE, show_col_types = FALSE) %>%
  transmute(
    CO_CINE_ROTULO = as.character(CO_CINE_ROTULO), # CO_CINE_ROTULO é a chave do curso no Censo Superior e será usada para o join
    NO_CINE_ROTULO = as.character(NO_CINE_ROTULO), # Rótulo do curso (descrição do rotulo CINE)
    # Classificações agregadas do curso (área geral, específica e detalhada)
    CO_CINE_AREA_GERAL = as.character(CO_CINE_AREA_GERAL),
    NO_CINE_AREA_GERAL = as.character(NO_CINE_AREA_GERAL),
    CO_CINE_AREA_ESPECIFICA = as.character(CO_CINE_AREA_ESPECIFICA),
    NO_CINE_AREA_ESPECIFICA = as.character(NO_CINE_AREA_ESPECIFICA),
    CO_CINE_AREA_DETALHADA = as.character(CO_CINE_AREA_DETALHADA),
    NO_CINE_AREA_DETALHADA = as.character(NO_CINE_AREA_DETALHADA)
  ) %>%
  distinct(CO_CINE_ROTULO, .keep_all = TRUE) # Garante que exista apenas uma linha por CO_CINE_ROTULO. Isso evita duplicar linhas no SUP_ALUNO ao fazer o join.


# -----------------------------
# SUP_ALUNO: ler e juntar com cine
# (Importante: estamos analisando ingressantes. A mesma pessoa pode ingressar em mais de um curso no mesmo ano,
# então o CPF pode aparecer mais de uma vez no SUP — isso é esperado. Por isso filtramos para os alunos que estão ingressando no ano do Censo)
# -----------------------------
sup_aluno <- readr::read_csv(PATH_SUP, show_col_types = FALSE) %>%
  transmute(
    NU_ANO = as.numeric(NU_ANO), # Ano Censo Superior
    CPF_MASC = as.character(CPF_MASC), #Conector CPF
    TP_GRAU_ACADEMICO = as.character(TP_GRAU_ACADEMICO), #Licenciatura e outras areas
    TP_MODALIDADE_ENSINO = as.character(TP_MODALIDADE_ENSINO), #Presencial /EaD
    TP_NIVEL_ACADEMICO = as.character(TP_NIVEL_ACADEMICO), #Graduação versus Formação Especifica
    CO_CINE_ROTULO = as.character(CO_CINE_ROTULO), #Código do Curso
    TP_SITUACAO = as.character(TP_SITUACAO), #Situação do aluno no Curso
    IN_INGRESSO_TOTAL = as.numeric(IN_INGRESSO_TOTAL), # Dummy se o aluno é ingressante
    NU_ANO_INGRESSO = as.numeric(NU_ANO_INGRESSO) # Ano de Ingresso do Aluno
  ) %>%
  filter(
    #TP_SITUACAO == "2", # (não está sendo utilizado) Aluno está cursando quando foi coletado o Censo Superior
    IN_INGRESSO_TOTAL == 1, #Aluno ingressante no Ensino Superior naquele ano
    NU_ANO_INGRESSO == NU_ANO #Aluno ingressante no Ensino Superior naquele ano do censo (safe)
  ) %>%
  left_join(cine, by = "CO_CINE_ROTULO")


# -----------------------------
# Base final (ENEM + SUP_ALUNO + CINE)
# -----------------------------
base_final <- enem %>%
  inner_join(sup_aluno, by = "CPF_MASC", suffix = c("_ENEM", "_SUP")) %>%  # Faz a vinculação ENEM ↔ Censo Superior usando CPF_MASC (chave mascarada). inner_join mantém apenas indivíduos presentes nas duas bases
  mutate( #Cria indicadores lógicos (TRUE/FALSE) para identificar se o estudante está nas faixas superiores de desempenho do ENEM, com base nos pontos de corte (Se NOTA_MEDIA for NA, o resultado também ficará NA) :
    ACIMA_Q3 = NOTA_MEDIA >= q3, # tercil superior
    ACIMA_Q4 = NOTA_MEDIA >= q4, # quartil superior
    ACIMA_Q5 = NOTA_MEDIA >= q5  # quintil superior
  ) %>%
  mutate(
    # Área geral: usa a área oficial do CINE e “destaca” Direito e Medicina como exceção
    AREA_GERAL = case_when(
      CO_CINE_ROTULO == "0421D01" ~ "Direito",
      CO_CINE_ROTULO == "0912M01" ~ "Medicina",
      !is.na(NO_CINE_AREA_GERAL)  ~ NO_CINE_AREA_GERAL,
      TRUE ~ "Outras/Não mapeada"),
    NAO_LIC = is.na(TP_GRAU_ACADEMICO) | TP_GRAU_ACADEMICO != "2" ) # Criar uma variável para selecionar os Cursos que não são licenciatura
# %>% filter(!is.na(NOTA_MEDIA))  # (Não está sendo utilizado!) restringe a base aos casos em que o vínculo Enem–Superior existe e o Enem tem NOTA_MEDIA calculada; exclui vinculados sem as 5 notas

glimpse(base_final) 

# -----------------------------
# Tabela ENEM 2023 + SUP 2024 (A–D))
# -----------------------------

df1 <- base_final %>%
  filter(!is.na(NOTA_MEDIA)) %>%   # mantém só quem tem as 5 notas (nota válida)
  group_by(NU_ANO_INGRESSO) %>%
  summarise(
    # A) fez ENEM (nota válida) e ingressou no ano
    N_ENEM2018_ING2019 = n(),
    # B) ingressou no ano em licenciatura
    N_ENEM2018_ING2019_LIC = sum(TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    # C) acima do corte e ingressou no ano
    N_ENEM2018_Q3_ING2019 = sum(NOTA_MEDIA >= q3, na.rm = TRUE),
    N_ENEM2018_Q4_ING2019 = sum(NOTA_MEDIA >= q4, na.rm = TRUE),
    N_ENEM2018_Q5_ING2019 = sum(NOTA_MEDIA >= q5, na.rm = TRUE),
    # D) acima do corte e ingressou no ano em licenciatura
    N_ENEM2018_Q3_ING2019_LIC = sum(NOTA_MEDIA >= q3 & TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    N_ENEM2018_Q4_ING2019_LIC = sum(NOTA_MEDIA >= q4 & TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    N_ENEM2018_Q5_ING2019_LIC = sum(NOTA_MEDIA >= q5 & TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(NU_ANO_INGRESSO)

print(df1)


#Analisar apenas para quem está cursando algum curso no ES
df1.1 <- base_final %>%
  filter(!is.na(NOTA_MEDIA), # Mantém apenas observações com NOTA_MEDIA válida
         TP_SITUACAO == "2") %>%    #seleciona apenas quem está cursando o ES no ano do censo
  group_by(NU_ANO_INGRESSO) %>%
  summarise(
    # A) fez ENEM (nota válida) e ingressou no ano
    N_ENEM2018_ING2019 = n(),
    # B) ingressou no ano em licenciatura
    N_ENEM2018_ING2019_LIC = sum(TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    # C) acima do corte e ingressou no ano
    N_ENEM2018_Q3_ING2019 = sum(NOTA_MEDIA >= q3, na.rm = TRUE),
    N_ENEM2018_Q4_ING2019 = sum(NOTA_MEDIA >= q4, na.rm = TRUE),
    N_ENEM2018_Q5_ING2019 = sum(NOTA_MEDIA >= q5, na.rm = TRUE),
    # D) acima do corte e ingressou no ano em licenciatura
    N_ENEM2018_Q3_ING2019_LIC = sum(NOTA_MEDIA >= q3 & TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    N_ENEM2018_Q4_ING2019_LIC = sum(NOTA_MEDIA >= q4 & TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    N_ENEM2018_Q5_ING2019_LIC = sum(NOTA_MEDIA >= q5 & TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(NU_ANO_INGRESSO)



# -----------------------------
# Tabela só ENEM (total com nota e acima dos cortes)
# -----------------------------

df2 <- enem %>%
  filter(!is.na(NOTA_MEDIA)) %>% # Mantém apenas observações com NOTA_MEDIA válida
  group_by(NU_ANO) %>%
  summarise(
    # 1) Total com nota válida no ano (como filtrou antes, é só contar linhas)
    N_COM_NOTA_ENEM = n(),
    N_NOTA_ENEM_ACIMA_Q3 = sum(NOTA_MEDIA >= q3, na.rm = TRUE), # 2) Quantos estão no tercil superior (>= q3)
    N_NOTA_ENEM_ACIMA_Q4 = sum(NOTA_MEDIA >= q4, na.rm = TRUE), # 3) Quantos estão no quartil superior (>= q4)
    N_NOTA_ENEM_ACIMA_Q5 = sum(NOTA_MEDIA >= q5, na.rm = TRUE), # 4) Quantos estão no quintil superior (>= q5)
    .groups = "drop"
  ) %>%
  arrange(NU_ANO)

print(df2)


# -----------------------------
# Tabela Superior 2019 (total e licenciatura)
# -----------------------------

df3 <- sup_aluno_2019 %>%
  group_by(NU_ANO_INGRESSO) %>%
  summarise(
    N_ING2019_TOTAL = n(),
    N_ING2019_LIC = sum(TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(NU_ANO_INGRESSO)

print(df3)


#Analisar apenas para quem está cursando algum curso no ES
df3.1 <- sup_aluno_2019 %>%
  filter (TP_SITUACAO == "2") |> 
  group_by(NU_ANO_INGRESSO) %>%
  summarise(
    N_ING2019_TOTAL = n(),
    N_ING2019_LIC = sum(TP_GRAU_ACADEMICO == "2", na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(NU_ANO_INGRESSO)


# -----------------------------
# Tabela Acesso ENEM 2018 + SUP 2019 por Área
# -----------------------------

df4 <- base_final %>%
  filter(!is.na(NOTA_MEDIA)) %>% # Mantém apenas observações com NOTA_MEDIA válida
  group_by(NU_ANO_INGRESSO, AREA_GERAL) %>%
  summarise(
    N_TOTAL_ING_NAO_LIC = sum(NAO_LIC, na.rm = TRUE), # Total de ingressantes "não licenciatura" dentro do recorte (ano x área)
    N_Q3_NAO_LIC = sum(NAO_LIC & NOTA_MEDIA >= q3, na.rm = TRUE), # Entre os "não licenciatura", quantos estão acima do tercil superior do Enem (>= q3)
    N_Q4_NAO_LIC = sum(NAO_LIC & NOTA_MEDIA >= q4, na.rm = TRUE), # Entre os "não licenciatura", quantos estão acima do quartil superior do Enem (>= q4)
    N_Q5_NAO_LIC = sum(NAO_LIC & NOTA_MEDIA >= q5, na.rm = TRUE), # Entre os "não licenciatura", quantos estão acima do quintil superior do Enem (>= q5)
    .groups = "drop"
  ) %>%
  arrange(NU_ANO_INGRESSO, AREA_GERAL)

print(df4)


#Analisar apenas para quem está cursando algum curso no ES
df4 <- base_final %>%
  filter(!is.na(NOTA_MEDIA), # Mantém apenas observações com NOTA_MEDIA válida
         TP_SITUACAO == "2") %>%    #seleciona apenas quem está cursando o ES no ano do censo
  group_by(NU_ANO_INGRESSO, AREA_GERAL) %>%
  summarise(
    N_TOTAL_ING_NAO_LIC = sum(NAO_LIC, na.rm = TRUE), # Total de ingressantes "não licenciatura" dentro do recorte (ano x área)
    N_Q3_NAO_LIC = sum(NAO_LIC & NOTA_MEDIA >= q3, na.rm = TRUE), # Entre os "não licenciatura", quantos estão acima do tercil superior do Enem (>= q3)
    N_Q4_NAO_LIC = sum(NAO_LIC & NOTA_MEDIA >= q4, na.rm = TRUE), # Entre os "não licenciatura", quantos estão acima do quartil superior do Enem (>= q4)
    N_Q5_NAO_LIC = sum(NAO_LIC & NOTA_MEDIA >= q5, na.rm = TRUE), # Entre os "não licenciatura", quantos estão acima do quintil superior do Enem (>= q5)
    .groups = "drop"
  ) %>%
  arrange(NU_ANO_INGRESSO, AREA_GERAL)
# -----------------------------
# Exportar Resultados
# -----------------------------

writexl::write_xlsx(
  list(
    df1 = df1,
    df1.1 = df1.1,
    df2 = df2,
    df3 = df3,
    df3.1 = df3.1,
    df4 = df4,
    df4.1 = df4.1,
    ),
  path = "outputs/tabelas_enem_sup.xlsx")

# Alternativa: exportar cada df em um arquivo separado (opcional)
# writexl::write_xlsx(df1, "outputs/df1.xlsx")
# writexl::write_xlsx(df2, "outputs/df2.xlsx")
# writexl::write_xlsx(df3, "outputs/df3.xlsx")
# writexl::write_xlsx(df4, "outputs/df4.xlsx")
# writexl::write_xlsx(df1.1, "outputs/df1.1.xlsx")
# writexl::write_xlsx(df3.1, "outputs/df3.1.xlsx")
# writexl::write_xlsx(df4.1, "outputs/df4.1.xlsx")