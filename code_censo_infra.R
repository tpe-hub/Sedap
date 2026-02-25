gc()
rm(list = ls())

pacman:: p_load(httr, jsonlite, ggplot2, stringr, tidyr, openxlsx, writexl, tidyverse,openxlsx)

API_URL <- "https://backend.rpinep2.prd.app.rnp.br/ide/send-process/d5cad842-4c33-415d-aed0-d8a33e0872d2"
FILEPATH_DIR <- "INEP_DATA"
TOKEN_IDE <- "TOKEN_IDE"
PROFILE_ID <- "PROFILE_ID"


# BRASIL ------------------------------------------------------------------

SQL_QUERY <- "
SELECT
  NU_ANO AS ANO,
  CASE WHEN TP_DEPENDENCIA = '4' THEN 'Privada' ELSE 'Publica' END AS REDE,

  COUNT(*) AS N_ESCOLAS,

-- Percentual de salas climatizadas = climatizadas / utilizadas
  CASE
    WHEN SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)) >= 1 THEN
      ROUND(
        100 * SUM(CAST(IFNULL(QT_SALAS_UTILIZA_CLIMATIZADAS, 0) AS FLOAT64))
          / SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)),
        2
      )
    ELSE NULL
  END AS PERC_CLIMATIZADAS,
  -- Infra básica (%)
    ROUND(100 * AVG(CAST(IN_COZINHA AS FLOAT64)), 2) AS IN_COZINHA,
  ROUND(100 * AVG(CAST(IN_AGUA_POTAVEL AS FLOAT64)), 2) AS IN_AGUA_POTAVEL,
  ROUND(100 * AVG(CAST(IN_ENERGIA_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ENERGIA_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_ESGOTO_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ESGOTO_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_LIXO_SERVICO_COLETA AS FLOAT64)), 2) AS IN_LIXO_SERVICO_COLETA,
  ROUND(
    100 * AVG(CAST(CASE WHEN IN_TRATAMENTO_LIXO_RECICLAGEM = 9 THEN NULL ELSE IN_TRATAMENTO_LIXO_RECICLAGEM END AS FLOAT64)),
    2
  ) AS IN_TRATAMENTO_LIXO_RECICLAGEM,

  -- Equipamentos / outros (%)
  ROUND(100 * AVG(CAST(IN_AREA_VERDE AS FLOAT64)), 2) AS IN_AREA_VERDE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO AS FLOAT64)), 2) AS IN_BANHEIRO,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_PNE AS FLOAT64)), 2) AS IN_BANHEIRO_PNE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_EI AS FLOAT64)), 2) AS IN_BANHEIRO_EI,
  ROUND(100 * AVG(CAST(IN_BIBLIOTECA_SALA_LEITURA AS FLOAT64)), 2) AS IN_BIBLIOTECA_SALA_LEITURA,
  ROUND(100 * AVG(CAST(IN_PARQUE_INFANTIL AS FLOAT64)), 2) AS IN_PARQUE_INFANTIL,
  ROUND(100 * AVG(CAST(IN_QUADRA_ESPORTES AS FLOAT64)), 2) AS IN_QUADRA_ESPORTES,
  ROUND(100 * AVG(CAST(IN_SALA_MULTIUSO AS FLOAT64)), 2) AS IN_SALA_MULTIUSO,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_CIENCIAS AS FLOAT64)), 2) AS IN_LABORATORIO_CIENCIAS,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_INFORMATICA AS FLOAT64)), 2) AS IN_LABORATORIO_INFORMATICA,
  ROUND(100 * AVG(CAST(IN_MATERIAL_PED_INFANTIL AS FLOAT64)), 2) AS IN_MATERIAL_PED_INFANTIL

FROM raw.BAS_ESCOLA_2023
WHERE
  IN_ESCOLARIZACAO = 1
  AND TP_SITUACAO_FUNCIONAMENTO = '1'
GROUP BY
  ANO, REDE
ORDER BY
  ANO, REDE
"





make_request <- function() {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  filepath <- file.path(FILEPATH_DIR, paste0("rows_", timestamp, ".json"))
  dir.create(FILEPATH_DIR, showWarnings = FALSE, recursive = TRUE)
  
  cat("Buscando os dados:", filepath, "...
")
  
  res <- POST(
    url = API_URL,
    body = list(content = SQL_QUERY),
    encode = "json",
    add_headers(
      Authorization = paste("Bearer", TOKEN_IDE),
      'Content-Type' = "application/json",
      'profile-id' = PROFILE_ID
    )
  )
  
  if (http_error(res)) {
    cat("Error during API request:
")
    print(status_code(res))
    print(content(res, "text"))
    return(NULL)
  }
  
  rows_json <- fromJSON(content(res, "text", encoding = "UTF-8"))$rows
  write(toJSON(rows_json, pretty = TRUE, auto_unbox = TRUE), filepath)
  cat("Dados salvos em:", filepath, "
")
  return(rows_json)
}

rows <- make_request()

if (!is.null(rows)) {
  df <- as.data.frame(rows)
  print(head(df, 3))
} else {
  cat("No data to create DataFrame.
")
}



# Educ. Infantil ----------------------------------------------------------


SQL_QUERY <- "
SELECT
  NU_ANO AS ANO,
  CASE WHEN TP_DEPENDENCIA = '4' THEN 'Privada' ELSE 'Publica' END AS REDE,

  COUNT(*) AS N_ESCOLAS_EI,

  -- Percentual de salas climatizadas = climatizadas / utilizadas
  CASE
    WHEN SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)) >= 1 THEN
      ROUND(
        100 * SUM(CAST(IFNULL(QT_SALAS_UTILIZA_CLIMATIZADAS, 0) AS FLOAT64))
          / SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)),
        2
      )
    ELSE NULL
  END AS PERC_CLIMATIZADAS,

  -- Médias (%) das variáveis (0/1)
    ROUND(100 * AVG(CAST(IN_COZINHA AS FLOAT64)), 2) AS IN_COZINHA,
  ROUND(100 * AVG(CAST(IN_AGUA_POTAVEL AS FLOAT64)), 2) AS IN_AGUA_POTAVEL,
  ROUND(100 * AVG(CAST(IN_ENERGIA_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ENERGIA_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_ESGOTO_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ESGOTO_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_LIXO_SERVICO_COLETA AS FLOAT64)), 2) AS IN_LIXO_SERVICO_COLETA,
  ROUND(
    100 * AVG(CAST(CASE WHEN IN_TRATAMENTO_LIXO_RECICLAGEM = 9 THEN NULL ELSE IN_TRATAMENTO_LIXO_RECICLAGEM END AS FLOAT64)),
    2
  ) AS IN_TRATAMENTO_LIXO_RECICLAGEM,

  ROUND(100 * AVG(CAST(IN_AREA_VERDE AS FLOAT64)), 2) AS IN_AREA_VERDE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO AS FLOAT64)), 2) AS IN_BANHEIRO,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_PNE AS FLOAT64)), 2) AS IN_BANHEIRO_PNE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_EI AS FLOAT64)), 2) AS IN_BANHEIRO_EI,
  ROUND(100 * AVG(CAST(IN_BIBLIOTECA_SALA_LEITURA AS FLOAT64)), 2) AS IN_BIBLIOTECA_SALA_LEITURA,
  ROUND(100 * AVG(CAST(IN_PARQUE_INFANTIL AS FLOAT64)), 2) AS IN_PARQUE_INFANTIL,
  ROUND(100 * AVG(CAST(IN_QUADRA_ESPORTES AS FLOAT64)), 2) AS IN_QUADRA_ESPORTES,
  ROUND(100 * AVG(CAST(IN_SALA_MULTIUSO AS FLOAT64)), 2) AS IN_SALA_MULTIUSO,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_CIENCIAS AS FLOAT64)), 2) AS IN_LABORATORIO_CIENCIAS,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_INFORMATICA AS FLOAT64)), 2) AS IN_LABORATORIO_INFORMATICA,
  ROUND(100 * AVG(CAST(IN_MATERIAL_PED_INFANTIL AS FLOAT64)), 2) AS IN_MATERIAL_PED_INFANTIL

FROM raw.BAS_ESCOLA_2023
WHERE
  IN_ESCOLARIZACAO = 1
  AND TP_SITUACAO_FUNCIONAMENTO = '1'
  AND (IN_COMUM_CRECHE = 1 OR IN_COMUM_PRE = 1)

GROUP BY
  ANO, REDE
ORDER BY
  ANO, REDE
"



make_request <- function() {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  filepath <- file.path(FILEPATH_DIR, paste0("rows_", timestamp, ".json"))
  dir.create(FILEPATH_DIR, showWarnings = FALSE, recursive = TRUE)
  
  cat("Buscando os dados:", filepath, "...
")
  
  res <- POST(
    url = API_URL,
    body = list(content = SQL_QUERY),
    encode = "json",
    add_headers(
      Authorization = paste("Bearer", TOKEN_IDE),
      'Content-Type' = "application/json",
      'profile-id' = PROFILE_ID
    )
  )
  
  if (http_error(res)) {
    cat("Error during API request:
")
    print(status_code(res))
    print(content(res, "text"))
    return(NULL)
  }
  
  rows_json <- fromJSON(content(res, "text", encoding = "UTF-8"))$rows
  write(toJSON(rows_json, pretty = TRUE, auto_unbox = TRUE), filepath)
  cat("Dados salvos em:", filepath, "
")
  return(rows_json)
}

rows <- make_request()

if (!is.null(rows)) {
  df2 <- as.data.frame(rows)
  print(head(df2, 3))
} else {
  cat("No data to create DataFrame.
")
}

# Anos Iniciais ----------------------------------------------------------

SQL_QUERY <- "
SELECT
  NU_ANO AS ANO,
  CASE WHEN TP_DEPENDENCIA = '4' THEN 'Privada' ELSE 'Publica' END AS REDE,

  COUNT(*) AS N_ESCOLAS_FUND_AI,

  -- Percentual de salas climatizadas = climatizadas / utilizadas
  CASE
    WHEN SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)) >= 1 THEN
      ROUND(
        100 * SUM(CAST(IFNULL(QT_SALAS_UTILIZA_CLIMATIZADAS, 0) AS FLOAT64))
          / SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)),
        2
      )
    ELSE NULL
  END AS PERC_CLIMATIZADAS,

  -- Médias (%) das variáveis (0/1)
    ROUND(100 * AVG(CAST(IN_COZINHA AS FLOAT64)), 2) AS IN_COZINHA,
  ROUND(100 * AVG(CAST(IN_AGUA_POTAVEL AS FLOAT64)), 2) AS IN_AGUA_POTAVEL,
  ROUND(100 * AVG(CAST(IN_ENERGIA_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ENERGIA_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_ESGOTO_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ESGOTO_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_LIXO_SERVICO_COLETA AS FLOAT64)), 2) AS IN_LIXO_SERVICO_COLETA,
  ROUND(
    100 * AVG(CAST(CASE WHEN IN_TRATAMENTO_LIXO_RECICLAGEM = 9 THEN NULL ELSE IN_TRATAMENTO_LIXO_RECICLAGEM END AS FLOAT64)),
    2
  ) AS IN_TRATAMENTO_LIXO_RECICLAGEM,

  ROUND(100 * AVG(CAST(IN_AREA_VERDE AS FLOAT64)), 2) AS IN_AREA_VERDE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO AS FLOAT64)), 2) AS IN_BANHEIRO,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_PNE AS FLOAT64)), 2) AS IN_BANHEIRO_PNE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_EI AS FLOAT64)), 2) AS IN_BANHEIRO_EI,
  ROUND(100 * AVG(CAST(IN_BIBLIOTECA_SALA_LEITURA AS FLOAT64)), 2) AS IN_BIBLIOTECA_SALA_LEITURA,
  ROUND(100 * AVG(CAST(IN_PARQUE_INFANTIL AS FLOAT64)), 2) AS IN_PARQUE_INFANTIL,
  ROUND(100 * AVG(CAST(IN_QUADRA_ESPORTES AS FLOAT64)), 2) AS IN_QUADRA_ESPORTES,
  ROUND(100 * AVG(CAST(IN_SALA_MULTIUSO AS FLOAT64)), 2) AS IN_SALA_MULTIUSO,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_CIENCIAS AS FLOAT64)), 2) AS IN_LABORATORIO_CIENCIAS,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_INFORMATICA AS FLOAT64)), 2) AS IN_LABORATORIO_INFORMATICA,
  ROUND(100 * AVG(CAST(IN_MATERIAL_PED_INFANTIL AS FLOAT64)), 2) AS IN_MATERIAL_PED_INFANTIL

FROM raw.BAS_ESCOLA_2023
WHERE
  IN_ESCOLARIZACAO = 1
  AND TP_SITUACAO_FUNCIONAMENTO = '1'
  AND IN_COMUM_FUND_AI = 1

GROUP BY
  ANO, REDE
ORDER BY
  ANO, REDE
"






make_request <- function() {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  filepath <- file.path(FILEPATH_DIR, paste0("rows_", timestamp, ".json"))
  dir.create(FILEPATH_DIR, showWarnings = FALSE, recursive = TRUE)
  
  cat("Buscando os dados:", filepath, "...
")
  
  res <- POST(
    url = API_URL,
    body = list(content = SQL_QUERY),
    encode = "json",
    add_headers(
      Authorization = paste("Bearer", TOKEN_IDE),
      'Content-Type' = "application/json",
      'profile-id' = PROFILE_ID
    )
  )
  
  if (http_error(res)) {
    cat("Error during API request:
")
    print(status_code(res))
    print(content(res, "text"))
    return(NULL)
  }
  
  rows_json <- fromJSON(content(res, "text", encoding = "UTF-8"))$rows
  write(toJSON(rows_json, pretty = TRUE, auto_unbox = TRUE), filepath)
  cat("Dados salvos em:", filepath, "
")
  return(rows_json)
}

rows <- make_request()

if (!is.null(rows)) {
  df3 <- as.data.frame(rows)
  print(head(df3, 3))
} else {
  cat("No data to create DataFrame.
")
}


# Anos Finais ----------------------------------------------------------

SQL_QUERY <- "
SELECT
  NU_ANO AS ANO,
  CASE WHEN TP_DEPENDENCIA = '4' THEN 'Privada' ELSE 'Publica' END AS REDE,

  COUNT(*) AS N_ESCOLAS_FUND_AF,

  -- Percentual de salas climatizadas = climatizadas / utilizadas
  CASE
    WHEN SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)) >= 1 THEN
      ROUND(
        100 * SUM(CAST(IFNULL(QT_SALAS_UTILIZA_CLIMATIZADAS, 0) AS FLOAT64))
          / SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)),
        2
      )
    ELSE NULL
  END AS PERC_CLIMATIZADAS,

  -- Médias (%) das variáveis (0/1)
    ROUND(100 * AVG(CAST(IN_COZINHA AS FLOAT64)), 2) AS IN_COZINHA,
  ROUND(100 * AVG(CAST(IN_AGUA_POTAVEL AS FLOAT64)), 2) AS IN_AGUA_POTAVEL,
  ROUND(100 * AVG(CAST(IN_ENERGIA_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ENERGIA_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_ESGOTO_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ESGOTO_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_LIXO_SERVICO_COLETA AS FLOAT64)), 2) AS IN_LIXO_SERVICO_COLETA,
  ROUND(
    100 * AVG(CAST(CASE WHEN IN_TRATAMENTO_LIXO_RECICLAGEM = 9 THEN NULL ELSE IN_TRATAMENTO_LIXO_RECICLAGEM END AS FLOAT64)),
    2
  ) AS IN_TRATAMENTO_LIXO_RECICLAGEM,

  ROUND(100 * AVG(CAST(IN_AREA_VERDE AS FLOAT64)), 2) AS IN_AREA_VERDE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO AS FLOAT64)), 2) AS IN_BANHEIRO,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_PNE AS FLOAT64)), 2) AS IN_BANHEIRO_PNE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_EI AS FLOAT64)), 2) AS IN_BANHEIRO_EI,
  ROUND(100 * AVG(CAST(IN_BIBLIOTECA_SALA_LEITURA AS FLOAT64)), 2) AS IN_BIBLIOTECA_SALA_LEITURA,
  ROUND(100 * AVG(CAST(IN_PARQUE_INFANTIL AS FLOAT64)), 2) AS IN_PARQUE_INFANTIL,
  ROUND(100 * AVG(CAST(IN_QUADRA_ESPORTES AS FLOAT64)), 2) AS IN_QUADRA_ESPORTES,
  ROUND(100 * AVG(CAST(IN_SALA_MULTIUSO AS FLOAT64)), 2) AS IN_SALA_MULTIUSO,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_CIENCIAS AS FLOAT64)), 2) AS IN_LABORATORIO_CIENCIAS,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_INFORMATICA AS FLOAT64)), 2) AS IN_LABORATORIO_INFORMATICA,
  ROUND(100 * AVG(CAST(IN_MATERIAL_PED_INFANTIL AS FLOAT64)), 2) AS IN_MATERIAL_PED_INFANTIL

FROM raw.BAS_ESCOLA_2023
WHERE
  IN_ESCOLARIZACAO = 1
  AND TP_SITUACAO_FUNCIONAMENTO = '1'
  AND IN_COMUM_FUND_AF = 1

GROUP BY
  ANO, REDE
ORDER BY
  ANO, REDE
"








make_request <- function() {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  filepath <- file.path(FILEPATH_DIR, paste0("rows_", timestamp, ".json"))
  dir.create(FILEPATH_DIR, showWarnings = FALSE, recursive = TRUE)
  
  cat("Buscando os dados:", filepath, "...
")
  
  res <- POST(
    url = API_URL,
    body = list(content = SQL_QUERY),
    encode = "json",
    add_headers(
      Authorization = paste("Bearer", TOKEN_IDE),
      'Content-Type' = "application/json",
      'profile-id' = PROFILE_ID
    )
  )
  
  if (http_error(res)) {
    cat("Error during API request:
")
    print(status_code(res))
    print(content(res, "text"))
    return(NULL)
  }
  
  rows_json <- fromJSON(content(res, "text", encoding = "UTF-8"))$rows
  write(toJSON(rows_json, pretty = TRUE, auto_unbox = TRUE), filepath)
  cat("Dados salvos em:", filepath, "
")
  return(rows_json)
}

rows <- make_request()

if (!is.null(rows)) {
  df4 <- as.data.frame(rows)
  print(head(df4, 3))
} else {
  cat("No data to create DataFrame.
")
}

# Ensino Médio ----------------------------------------------------------

SQL_QUERY <- "
SELECT
  NU_ANO AS ANO,
  CASE WHEN TP_DEPENDENCIA = '4' THEN 'Privada' ELSE 'Publica' END AS REDE,

  COUNT(*) AS N_ESCOLAS_MEDIO,

  -- Percentual de salas climatizadas = climatizadas / utilizadas
  CASE
    WHEN SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)) >= 1 THEN
      ROUND(
        100 * SUM(CAST(IFNULL(QT_SALAS_UTILIZA_CLIMATIZADAS, 0) AS FLOAT64))
          / SUM(CAST(IFNULL(QT_SALAS_UTILIZADAS, 0) AS FLOAT64)),
        2
      )
    ELSE NULL
  END AS PERC_CLIMATIZADAS,

  -- Médias (%) das variáveis (0/1)
  ROUND(100 * AVG(CAST(IN_COZINHA AS FLOAT64)), 2) AS IN_COZINHA,
  ROUND(100 * AVG(CAST(IN_AGUA_POTAVEL AS FLOAT64)), 2) AS IN_AGUA_POTAVEL,
  ROUND(100 * AVG(CAST(IN_ENERGIA_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ENERGIA_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_ESGOTO_REDE_PUBLICA AS FLOAT64)), 2) AS IN_ESGOTO_REDE_PUBLICA,
  ROUND(100 * AVG(CAST(IN_LIXO_SERVICO_COLETA AS FLOAT64)), 2) AS IN_LIXO_SERVICO_COLETA,
  ROUND(
    100 * AVG(CAST(CASE WHEN IN_TRATAMENTO_LIXO_RECICLAGEM = 9 THEN NULL ELSE IN_TRATAMENTO_LIXO_RECICLAGEM END AS FLOAT64)),
    2
  ) AS IN_TRATAMENTO_LIXO_RECICLAGEM,

  ROUND(100 * AVG(CAST(IN_AREA_VERDE AS FLOAT64)), 2) AS IN_AREA_VERDE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO AS FLOAT64)), 2) AS IN_BANHEIRO,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_PNE AS FLOAT64)), 2) AS IN_BANHEIRO_PNE,
  ROUND(100 * AVG(CAST(IN_BANHEIRO_EI AS FLOAT64)), 2) AS IN_BANHEIRO_EI,
  ROUND(100 * AVG(CAST(IN_BIBLIOTECA_SALA_LEITURA AS FLOAT64)), 2) AS IN_BIBLIOTECA_SALA_LEITURA,
  ROUND(100 * AVG(CAST(IN_PARQUE_INFANTIL AS FLOAT64)), 2) AS IN_PARQUE_INFANTIL,
  ROUND(100 * AVG(CAST(IN_QUADRA_ESPORTES AS FLOAT64)), 2) AS IN_QUADRA_ESPORTES,
  ROUND(100 * AVG(CAST(IN_SALA_MULTIUSO AS FLOAT64)), 2) AS IN_SALA_MULTIUSO,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_CIENCIAS AS FLOAT64)), 2) AS IN_LABORATORIO_CIENCIAS,
  ROUND(100 * AVG(CAST(IN_LABORATORIO_INFORMATICA AS FLOAT64)), 2) AS IN_LABORATORIO_INFORMATICA,
  ROUND(100 * AVG(CAST(IN_MATERIAL_PED_INFANTIL AS FLOAT64)), 2) AS IN_MATERIAL_PED_INFANTIL

FROM raw.BAS_ESCOLA_2023
WHERE
  IN_ESCOLARIZACAO = 1
  AND TP_SITUACAO_FUNCIONAMENTO = '1'
  AND IN_COMUM_MEDIO_MEDIO = 1

GROUP BY
  ANO, REDE
ORDER BY
  ANO, REDE
"


make_request <- function() {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  filepath <- file.path(FILEPATH_DIR, paste0("rows_", timestamp, ".json"))
  dir.create(FILEPATH_DIR, showWarnings = FALSE, recursive = TRUE)
  
  cat("Buscando os dados:", filepath, "...
")
  
  res <- POST(
    url = API_URL,
    body = list(content = SQL_QUERY),
    encode = "json",
    add_headers(
      Authorization = paste("Bearer", TOKEN_IDE),
      'Content-Type' = "application/json",
      'profile-id' = PROFILE_ID
    )
  )
  
  if (http_error(res)) {
    cat("Error during API request:
")
    print(status_code(res))
    print(content(res, "text"))
    return(NULL)
  }
  
  rows_json <- fromJSON(content(res, "text", encoding = "UTF-8"))$rows
  write(toJSON(rows_json, pretty = TRUE, auto_unbox = TRUE), filepath)
  cat("Dados salvos em:", filepath, "
")
  return(rows_json)
}

rows <- make_request()

if (!is.null(rows)) {
  df5 <- as.data.frame(rows)
  print(head(df5, 3))
} else {
  cat("No data to create DataFrame.
")
}



# Ajustes Bases -----------------------------------------------------------

df <- df %>%
  select(-1) %>%  # exclui a coluna 1
  pivot_longer(
    cols = 2:ncol(.),         # colunas 3+ do df original (agora são 2+ depois do select(-1))
    names_to = "variavel",
    values_to = "valor"
  ) %>%
  pivot_wider(
    names_from = 1,           # coluna 2 do df original (agora é a coluna 1 após select(-1))
    values_from = valor
  )


df2 <- df2 %>%
  select(-1) %>%  # exclui a coluna 1
  pivot_longer(
    cols = 2:ncol(.),         # colunas 3+ do df original (agora são 2+ depois do select(-1))
    names_to = "variavel",
    values_to = "valor"
  ) %>%
  pivot_wider(
    names_from = 1,           # coluna 2 do df original (agora é a coluna 1 após select(-1))
    values_from = valor
  )


df3 <- df3 %>%
  select(-1) %>%  # exclui a coluna 1
  pivot_longer(
    cols = 2:ncol(.),         # colunas 3+ do df original (agora são 2+ depois do select(-1))
    names_to = "variavel",
    values_to = "valor"
  ) %>%
  pivot_wider(
    names_from = 1,           # coluna 2 do df original (agora é a coluna 1 após select(-1))
    values_from = valor
  )


df4 <- df4 %>%
  select(-1) %>%  # exclui a coluna 1
  pivot_longer(
    cols = 2:ncol(.),         # colunas 3+ do df original (agora são 2+ depois do select(-1))
    names_to = "variavel",
    values_to = "valor"
  ) %>%
  pivot_wider(
    names_from = 1,           # coluna 2 do df original (agora é a coluna 1 após select(-1))
    values_from = valor
  )


df5 <- df5 %>%
  select(-1) %>%  # exclui a coluna 1
  pivot_longer(
    cols = 2:ncol(.),         # colunas 3+ do df original (agora são 2+ depois do select(-1))
    names_to = "variavel",
    values_to = "valor"
  ) %>%
  pivot_wider(
    names_from = 1,           # coluna 2 do df original (agora é a coluna 1 após select(-1))
    values_from = valor
  )





wb <- createWorkbook()
addWorksheet(wb, "df")
writeData(wb, "df", df)
addWorksheet(wb, "df2")
writeData(wb, "df2", df2)
addWorksheet(wb, "df3")
writeData(wb, "df3", df3)
addWorksheet(wb, "df4")
writeData(wb, "df4", df4)
addWorksheet(wb, "df5")
writeData(wb, "df5", df5)
saveWorkbook(wb, "E:/TPE/github/Sedap/sedap_infra.xlsx", overwrite = TRUE)





# Código INEP - Dados Públicos --------------------------------------------

#-----------------
#- SETUP INICIAL -
#-----------------

# Limpando arquivos armazenados na memória

rm(list = ls())
pacman::p_load(dplyr,  data.table, openxlsx)
microdados_ed_basica_2023 <- read_delim("E:/Downloads/censo_escolar/microdados_educacao_basica_2023/microdados_educacao_basica_2023/dados/microdados_ed_basica_2023.csv", 
                                        delim = ";", escape_double = FALSE, locale = locale(encoding = "ISO-8859-1"), 
                                        trim_ws = TRUE)
censo <-  microdados_ed_basica_2023 |> 
  filter (IN_ESCOLARIZACAO ==1, #escolas que tenham matrícula
          TP_SITUACAO_FUNCIONAMENTO ==1) |>  #escolas em funcionamento
  select (NU_ANO_CENSO, CO_ENTIDADE,CO_ENTIDADE,  CO_REGIAO, CO_UF, NO_UF, CO_MUNICIPIO, NO_MUNICIPIO, CO_ENTIDADE,
          TP_DEPENDENCIA, TP_LOCALIZACAO,
          IN_ESCOLARIZACAO, TP_SITUACAO_FUNCIONAMENTO,
          IN_INF, IN_FUND, IN_FUND_AI, IN_FUND_AF, IN_MED,
          IN_BANHEIRO,IN_BANHEIRO_PNE, IN_BANHEIRO_EI, IN_COZINHA,IN_AGUA_POTAVEL,IN_ENERGIA_REDE_PUBLICA,IN_ESGOTO_REDE_PUBLICA,IN_LIXO_SERVICO_COLETA,IN_TRATAMENTO_LIXO_RECICLAGEM,
          IN_BIBLIOTECA_SALA_LEITURA,IN_LABORATORIO_INFORMATICA,IN_LABORATORIO_CIENCIAS,IN_QUADRA_ESPORTES, QT_SALAS_UTILIZA_CLIMATIZADAS, QT_SALAS_UTILIZADAS,
          IN_INTERNET,IN_BANDA_LARGA,IN_INTERNET_ALUNOS,IN_INTERNET_ADMINISTRATIVO,IN_INTERNET_APRENDIZAGEM,IN_EQUIP_LOUSA_DIGITAL,IN_EQUIP_MULTIMIDIA,IN_DESKTOP_ALUNO,IN_COMP_PORTATIL_ALUNO,IN_TABLET_ALUNO,
          IN_AREA_VERDE, IN_PARQUE_INFANTIL,IN_MATERIAL_PED_INFANTIL, IN_SALA_MULTIUSO) |> 
  mutate(IN_TRATAMENTO_LIXO_RECICLAGEM = na_if(IN_TRATAMENTO_LIXO_RECICLAGEM, 9))

censo_publico <-  censo |>  filter (TP_DEPENDENCIA !=4)
censo_privada <-  censo |>  filter (TP_DEPENDENCIA ==4)




# Infra Básica ------------------------------------------------------------
vars_infra_basica <- c(
  "IN_BANHEIRO", "IN_BANHEIRO_PNE", "IN_BANHEIRO_EI", "IN_COZINHA", "IN_AGUA_POTAVEL",
  "IN_ENERGIA_REDE_PUBLICA", "IN_ESGOTO_REDE_PUBLICA",
  "IN_LIXO_SERVICO_COLETA", "IN_TRATAMENTO_LIXO_RECICLAGEM"
)

resumo_infra_geral <- function(data, nome_abertura = "Brasil") {
  data %>%
    summarize(across(
      all_of(vars_infra_basica),
      ~ round(mean(.x, na.rm = TRUE) * 100, 2)
    )) %>%
    mutate(
      Ano = 2023,
      Abertura = nome_abertura
    ) %>%
    select(Ano, Abertura, everything())
}

resumo_etapa <- function(data, etapa_var, nome_etapa) {
  data %>%
    filter(.data[[etapa_var]] == 1) %>%
    summarize(across(
      all_of(vars_infra_basica),
      ~ round(mean(.x, na.rm = TRUE) * 100, 2)
    )) %>%
    mutate(
      Ano = 2023,
      Abertura = nome_etapa
    ) %>%
    select(Ano, Abertura, everything())
}

resumo_infra_localizacao <- function(data) {
  data %>%
    group_by(TP_LOCALIZACAO) %>%
    summarize(across(
      all_of(vars_infra_basica),
      ~ round(mean(.x, na.rm = TRUE) * 100, 2)
    ), .groups = "drop") %>%
    mutate(
      Ano = 2023,
      Abertura = case_when(
        TP_LOCALIZACAO == 1 ~ "Urbana",
        TP_LOCALIZACAO == 2 ~ "Rural",
        TRUE ~ "Outros"
      )
    ) %>%
    select(Ano, Abertura, all_of(vars_infra_basica))
}

infra_geral     <- resumo_infra_geral(censo_privada)
infra_inf       <- resumo_etapa(censo_privada, "IN_INF", "Educação Infantil")
infra_fund_ai   <- resumo_etapa(censo_privada, "IN_FUND_AI", "Fundamental - Anos Iniciais")
infra_fund_af   <- resumo_etapa(censo_privada, "IN_FUND_AF", "Fundamental - Anos Finais")
infra_med       <- resumo_etapa(censo_privada, "IN_MED", "Ensino Médio")


infra_final_priv <- bind_rows(
  infra_geral,
  infra_inf,
  infra_fund_ai,
  infra_fund_af,
  infra_med
)

rm(infra_geral, infra_inf, infra_fund_ai, infra_fund_af, infra_med, infra_local)


infra_geral     <- resumo_infra_geral(censo_publico)
infra_inf       <- resumo_etapa(censo_publico, "IN_INF", "Educação Infantil")
infra_fund_ai   <- resumo_etapa(censo_publico, "IN_FUND_AI", "Fundamental - Anos Iniciais")
infra_fund_af   <- resumo_etapa(censo_publico, "IN_FUND_AF", "Fundamental - Anos Finais")
infra_med       <- resumo_etapa(censo_publico, "IN_MED", "Ensino Médio")


infra_final_pub <- bind_rows(
  infra_geral,
  infra_inf,
  infra_fund_ai,
  infra_fund_af,
  infra_med
)

rm(infra_geral, infra_inf, infra_fund_ai, infra_fund_af, infra_med, infra_local)



# Equipamentos de aprendizagem --------------------------------------------

vars_equip_dummies <- c("IN_BIBLIOTECA_SALA_LEITURA", "IN_LABORATORIO_INFORMATICA",
                        "IN_LABORATORIO_CIENCIAS", "IN_QUADRA_ESPORTES", "IN_MATERIAL_PED_INFANTIL",
                        "IN_AREA_VERDE", "IN_PARQUE_INFANTIL", "IN_SALA_MULTIUSO")

resumo_equip_geral <- function(data, nome_abertura = "Brasil") {
  dummies_summary <- data %>%
    summarize(across(
      all_of(vars_equip_dummies),
      ~ round(mean(.x, na.rm = TRUE) * 100, 2)
    ))
  
  # Proporção de salas climatizadas
  soma_climatizadas <- sum(data$QT_SALAS_UTILIZA_CLIMATIZADAS, na.rm = TRUE)
  soma_total_salas  <- sum(data$QT_SALAS_UTILIZADAS, na.rm = TRUE)
  
  prop_climatizadas <- round(100 * soma_climatizadas / soma_total_salas, 2)
  
  dummies_summary %>%
    mutate(
      Perc_Climatizadas = prop_climatizadas,
      Ano = 2023,
      Abertura = nome_abertura
    ) %>%
    select(Ano, Abertura, everything())
}

resumo_equip_etapa <- function(data, etapa_var, nome_etapa) {
  df <- data %>% filter(.data[[etapa_var]] == 1)
  
  dummies_summary <- df %>%
    summarize(across(
      all_of(vars_equip_dummies),
      ~ round(mean(.x, na.rm = TRUE) * 100, 2)
    ))
  
  soma_climatizadas <- sum(df$QT_SALAS_UTILIZA_CLIMATIZADAS, na.rm = TRUE)
  soma_total_salas  <- sum(df$QT_SALAS_UTILIZADAS, na.rm = TRUE)
  
  prop_climatizadas <- round(100 * soma_climatizadas / soma_total_salas, 2)
  
  dummies_summary %>%
    mutate(
      Perc_Climatizadas = prop_climatizadas,
      Ano = 2023,
      Abertura = nome_etapa
    ) %>%
    select(Ano, Abertura, everything())
}


resumo_equip_localizacao <- function(data) {
  data %>%
    group_by(TP_LOCALIZACAO) %>%
    summarize(
      across(all_of(vars_equip_dummies), ~ round(mean(.x, na.rm = TRUE) * 100, 2)),
      Climatizadas = sum(QT_SALAS_UTILIZA_CLIMATIZADAS, na.rm = TRUE),
      Total_Salas  = sum(QT_SALAS_UTILIZADAS, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      Perc_Climatizadas = round(100 * Climatizadas / Total_Salas, 2),
      Ano = 2023,
      Abertura = case_when(
        TP_LOCALIZACAO == 1 ~ "Urbana",
        TP_LOCALIZACAO == 2 ~ "Rural",
        TRUE ~ "Outros"
      )
    ) %>%
    select(Ano, Abertura, all_of(vars_equip_dummies), Perc_Climatizadas)
}


equip_geral     <- resumo_equip_geral(censo_privada)
equip_inf       <- resumo_equip_etapa(censo_privada, "IN_INF", "Educação Infantil")
equip_fund_ai   <- resumo_equip_etapa(censo_privada, "IN_FUND_AI", "Fundamental - Anos Iniciais")
equip_fund_af   <- resumo_equip_etapa(censo_privada, "IN_FUND_AF", "Fundamental - Anos Finais")
equip_med       <- resumo_equip_etapa(censo_privada, "IN_MED", "Ensino Médio")

equipamentos_final_priv <- bind_rows(
  equip_geral,
  equip_inf,
  equip_fund_ai,
  equip_fund_af,
  equip_med
)

remove (equip_geral,equip_inf,equip_fund_ai,equip_fund_af, equip_med,equip_local)


equip_geral     <- resumo_equip_geral(censo_publico)
equip_inf       <- resumo_equip_etapa(censo_publico, "IN_INF", "Educação Infantil")
equip_fund_ai   <- resumo_equip_etapa(censo_publico, "IN_FUND_AI", "Fundamental - Anos Iniciais")
equip_fund_af   <- resumo_equip_etapa(censo_publico, "IN_FUND_AF", "Fundamental - Anos Finais")
equip_med       <- resumo_equip_etapa(censo_publico, "IN_MED", "Ensino Médio")
equip_local     <- resumo_equip_localizacao(censo_publico)

equipamentos_final_pub <- bind_rows(
  equip_geral,
  equip_inf,
  equip_fund_ai,
  equip_fund_af,
  equip_med
)

remove (equip_geral,equip_inf,equip_fund_ai,equip_fund_af, equip_med,equip_local)



infra_pub <-  left_join(infra_final_pub, equipamentos_final_pub)
infra_priv <-  left_join(infra_final_priv, equipamentos_final_priv)


path <- "E:/TPE/github/Sedap/sedap_infra.xlsx"

# abre o arquivo existente
wb <- loadWorkbook(path)

# adiciona (ou substitui se já existir)
if ("infra_pub" %in% names(wb))  removeWorksheet(wb, "infra_pub")
addWorksheet(wb, "infra_pub")
writeData(wb, "infra_pub", infra_pub)

if ("infra_priv" %in% names(wb)) removeWorksheet(wb, "infra_priv")
addWorksheet(wb, "infra_priv")
writeData(wb, "infra_priv", infra_priv)

# salva no mesmo arquivo
saveWorkbook(wb, path, overwrite = TRUE)
