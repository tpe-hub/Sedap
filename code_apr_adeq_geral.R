gc()
rm(list = ls())

pacman:: p_load(httr, jsonlite, ggplot2, stringr, tidyr, openxlsx, writexl)



API_URL <- "https://backend.rpinep2.prd.app.rnp.br/ide/send-process/d5cad842-4c33-415d-aed0-d8a33e0872d2"
FILEPATH_DIR <- "INEP_DATA"
TOKEN_IDE <- "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImE0MjJhN2U3LTg2OGItNDYxOC05MzM0LWE0NGUwOTExODllOCIsInJvbGUiOlsiUk9MRV9SRVNFQVJDSEVSIl0sIm5hbWUiOiJQRURSTyBBTEVYQU5EUkUgU0FOVE9TIFZFTE9TTyIsImVtYWlsIjoicGVkcm92ZWxsb3NvX0Bob3RtYWlsLmNvbSIsImNwZiI6IjA1MjM1ODQ4MzA2Iiwic291cmNlIjoiZXh0ZXJuYWwtaW50ZWdyYXRpb24iLCJpYXQiOjE3Njk2MTk3NDIsImV4cCI6MTc3MDIyNDU0Mn0.S4bWX6HyTVVPAwvmh05-RwYDVxJnHYegFBn8NuF26O4"
PROFILE_ID <- "2c25a386-88f1-4d68-ba24-a2088e98a12c"



SQL_QUERY <- "
WITH base AS (
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2011_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2013_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2015_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2017_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2019_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_9EF_2021_SEDAP
)
SELECT
    NU_ANO,
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
    NU_ANO,
    IN_PUBLICA
ORDER BY
    NU_ANO,
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
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2013_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2015_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2017_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2019_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_3EM_2021_SEDAP
)
SELECT
    NU_ANO,
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
    NU_ANO,
    IN_PUBLICA
ORDER BY
    NU_ANO,
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
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2011_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2013_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2015_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2017_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           CAST(REPLACE(PROFICIENCIA_LP_SAEB, ',', '.') AS NUMERIC) AS PROFICIENCIA_LP_SAEB,
           CAST(REPLACE(PROFICIENCIA_MT_SAEB, ',', '.') AS NUMERIC) AS PROFICIENCIA_MT_SAEB,
           CAST(REPLACE(PESO_ALUNO_LP, ',', '.') AS NUMERIC) AS PESO_ALUNO_LP,
           CAST(REPLACE(PESO_ALUNO_MT, ',', '.') AS NUMERIC) AS PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2019_SEDAP
    UNION ALL
    SELECT NU_ANO, CPF_MASC,
           TP_DEPENDENCIA_ADM_ESCOLA,
           PROFICIENCIA_LP_SAEB,
           PROFICIENCIA_MT_SAEB,
           PESO_ALUNO_LP,
           PESO_ALUNO_MT
    FROM raw.SAEB_ALUNO_5EF_2021_SEDAP
)
SELECT
    NU_ANO,
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
    NU_ANO,
    IN_PUBLICA
ORDER BY
    NU_ANO,
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
saveWorkbook(wb, "E:/pedro.veloso/Downloads/dados_saeb_geral.xlsx", overwrite = TRUE)


