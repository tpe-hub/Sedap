gc()
rm(list = ls())

pacman:: p_load(httr, jsonlite, ggplot2, stringr, tidyr, openxlsx, writexl)



API_URL <- "https://backend.rpinep2.prd.app.rnp.br/ide/send-process/d5cad842-4c33-415d-aed0-d8a33e0872d2"
FILEPATH_DIR <- "INEP_DATA"
TOKEN_IDE <- "TOKEN_IDE"
PROFILE_ID <- "PROFILE_ID"



SQL_QUERY <- "
WITH base AS (
    -- 2011
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2011_SEDAP

    UNION ALL
    -- 2013
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2013_SEDAP

    UNION ALL
    -- 2015
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2015_SEDAP

    UNION ALL
    -- 2017
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2017_SEDAP

    UNION ALL
    -- 2019
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2019_SEDAP

    UNION ALL
    -- 2021 (usa Q004)
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q004 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q004 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2021_SEDAP
)
SELECT
    NU_ANO, cor_raca,
    CASE 
        WHEN TP_DEPENDENCIA_ADM_ESCOLA = '4' THEN 0 
        ELSE 1 
    END AS IN_PUBLICA,

    /* LP */
    SUM(
        CASE 
            WHEN PROFICIENCIA_LP_SAEB IS NOT NULL
             AND PESO_ALUNO_LP IS NOT NULL
             AND PROFICIENCIA_LP_SAEB >= 275
            THEN PESO_ALUNO_LP
            ELSE 0
        END
    )
    / NULLIF(SUM(
        CASE 
            WHEN PROFICIENCIA_LP_SAEB IS NOT NULL
             AND PESO_ALUNO_LP IS NOT NULL
            THEN PESO_ALUNO_LP
            ELSE 0
        END
    ),0) AS MEDIA_ADEQ_LP,

    /* MT */
    SUM(
        CASE 
            WHEN PROFICIENCIA_MT_SAEB IS NOT NULL
             AND PESO_ALUNO_MT IS NOT NULL
             AND PROFICIENCIA_MT_SAEB >= 300
            THEN PESO_ALUNO_MT
            ELSE 0
        END
    )
    / NULLIF(SUM(
        CASE 
            WHEN PROFICIENCIA_MT_SAEB IS NOT NULL
             AND PESO_ALUNO_MT IS NOT NULL
            THEN PESO_ALUNO_MT
            ELSE 0
        END
    ),0) AS MEDIA_ADEQ_MT

FROM base
GROUP BY
    NU_ANO,cor_raca,
    IN_PUBLICA
ORDER BY
    NU_ANO,cor_raca,
    IN_PUBLICA;

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
  df_9 <- as.data.frame(rows)
  print(head(df, 3))
} else {
  cat("No data to create DataFrame.
")
}



###############
SQL_QUERY <- "
WITH base AS (
    -- 2013
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2013_SEDAP

    UNION ALL
    -- 2015
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2015_SEDAP

    UNION ALL
    -- 2017
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2017_SEDAP

    UNION ALL
    -- 2019
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2019_SEDAP

    UNION ALL
    -- 2021 (usa Q004)
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q004 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q004 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2021_SEDAP
)
SELECT
    NU_ANO,cor_raca,
    CASE 
        WHEN TP_DEPENDENCIA_ADM_ESCOLA = '4' THEN 0 
        ELSE 1 
    END AS IN_PUBLICA,

    /* LP */
    SUM(
        CASE 
            WHEN PROFICIENCIA_LP_SAEB IS NOT NULL
             AND PESO_ALUNO_LP IS NOT NULL
             AND PROFICIENCIA_LP_SAEB >= 300
            THEN PESO_ALUNO_LP
            ELSE 0
        END
    )
    / NULLIF(SUM(
        CASE 
            WHEN PROFICIENCIA_LP_SAEB IS NOT NULL
             AND PESO_ALUNO_LP IS NOT NULL
            THEN PESO_ALUNO_LP
            ELSE 0
        END
    ),0) AS MEDIA_ADEQ_LP,

    /* MT */
    SUM(
        CASE 
            WHEN PROFICIENCIA_MT_SAEB IS NOT NULL
             AND PESO_ALUNO_MT IS NOT NULL
             AND PROFICIENCIA_MT_SAEB >= 350
            THEN PESO_ALUNO_MT
            ELSE 0
        END
    )
    / NULLIF(SUM(
        CASE 
            WHEN PROFICIENCIA_MT_SAEB IS NOT NULL
             AND PESO_ALUNO_MT IS NOT NULL
            THEN PESO_ALUNO_MT
            ELSE 0
        END
    ),0) AS MEDIA_ADEQ_MT

FROM base
GROUP BY
    NU_ANO,cor_raca,
    IN_PUBLICA
ORDER BY
    NU_ANO,cor_raca,
    IN_PUBLICA;

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
  df_12 <- as.data.frame(rows)
  print(head(df, 3))
} else {
  cat("No data to create DataFrame.
")
}



##############
SQL_QUERY <- "
WITH base AS (
    -- 2011
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2011_SEDAP

    UNION ALL
    -- 2013
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2013_SEDAP

    UNION ALL
    -- 2015
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2015_SEDAP

    UNION ALL
    -- 2017
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2017_SEDAP

    UNION ALL
    -- 2019 (com cast para corrigir vírgulas)
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q002 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q002 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           CAST(REPLACE(PROFICIENCIA_LP_SAEB, ',', '.') AS NUMERIC) AS PROFICIENCIA_LP_SAEB,
           CAST(REPLACE(PROFICIENCIA_MT_SAEB, ',', '.') AS NUMERIC) AS PROFICIENCIA_MT_SAEB,
           CAST(REPLACE(PESO_ALUNO_LP, ',', '.') AS NUMERIC) AS PESO_ALUNO_LP,
           CAST(REPLACE(PESO_ALUNO_MT, ',', '.') AS NUMERIC) AS PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2019_SEDAP

    UNION ALL
    -- 2021 (usa Q004)
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CASE 
               WHEN Q004 IN ('A','D') THEN 'Brancos/Amarelos'
               WHEN Q004 IN ('B','C','E') THEN 'PPI'
               ELSE NULL
           END AS cor_raca,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2021_SEDAP
)
SELECT
    NU_ANO,cor_raca,
    CASE 
        WHEN TP_DEPENDENCIA_ADM_ESCOLA = '4' THEN 0 
        ELSE 1 
    END AS IN_PUBLICA,

    /* LP */
    SUM(
        CASE 
            WHEN PROFICIENCIA_LP_SAEB IS NOT NULL
             AND PESO_ALUNO_LP IS NOT NULL
             AND PROFICIENCIA_LP_SAEB >= 200
            THEN PESO_ALUNO_LP
            ELSE 0
        END
    )
    / NULLIF(SUM(
        CASE 
            WHEN PROFICIENCIA_LP_SAEB IS NOT NULL
             AND PESO_ALUNO_LP IS NOT NULL
            THEN PESO_ALUNO_LP
            ELSE 0
        END
    ),0) AS MEDIA_ADEQ_LP,

    /* MT */
    SUM(
        CASE 
            WHEN PROFICIENCIA_MT_SAEB IS NOT NULL
             AND PESO_ALUNO_MT IS NOT NULL
             AND PROFICIENCIA_MT_SAEB >= 225
            THEN PESO_ALUNO_MT
            ELSE 0
        END
    )
    / NULLIF(SUM(
        CASE 
            WHEN PROFICIENCIA_MT_SAEB IS NOT NULL
             AND PESO_ALUNO_MT IS NOT NULL
            THEN PESO_ALUNO_MT
            ELSE 0
        END
    ),0) AS MEDIA_ADEQ_MT

FROM base
GROUP BY
    NU_ANO,cor_raca,
    IN_PUBLICA
ORDER BY
    NU_ANO,cor_raca,
    IN_PUBLICA;

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
  df_5 <- as.data.frame(rows)
  print(head(df, 3))
} else {
  cat("No data to create DataFrame.
")
}



# Criar um novo workbook
wb <- createWorkbook()

# Adicionar cada data frame em uma aba diferente
addWorksheet(wb, "df_5")
writeData(wb, "df_5", df_5)

addWorksheet(wb, "df_9")
writeData(wb, "df_9", df_9)

addWorksheet(wb, "df_12")
writeData(wb, "df_12", df_12)

# Salvar o arquivo Excel
saveWorkbook(wb, "E:/pedro.veloso/Downloads/dados_saeb_corraca.xlsx", overwrite = TRUE)
