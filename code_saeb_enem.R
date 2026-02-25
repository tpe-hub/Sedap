gc()
rm(list = ls())

pacman:: p_load(httr, jsonlite, ggplot2, stringr, tidyr, openxlsx, writexl)



API_URL <- "https://backend.rpinep2.prd.app.rnp.br/ide/send-process/d5cad842-4c33-415d-aed0-d8a33e0872d2"
FILEPATH_DIR <- "INEP_DATA"
TOKEN_IDE <- "TOKEN_IDE"
PROFILE_ID <- "PROFILE_ID"


SQL_QUERY <- "
SELECT
    CASE WHEN s.TP_DEPENDENCIA_ADM_ESCOLA = '4' THEN 'Privada' ELSE 'Publica' END AS TIPO_ESCOLA,
    CASE WHEN s.PROFICIENCIA_MT_SAEB >= 350 THEN 1 ELSE 0 END AS ADEQ_MT,
    CASE WHEN s.PROFICIENCIA_LP_SAEB >= 300 THEN 1 ELSE 0 END AS ADEQ_LP,
    COUNT(*) AS N_LINHAS,
    AVG(e.NU_NOTA_CN) AS MEDIA_CN,
    AVG(e.NU_NOTA_CH) AS MEDIA_CH,
    AVG(e.NU_NOTA_LC) AS MEDIA_LC,
    AVG(e.NU_NOTA_MT) AS MEDIA_MT,
    AVG(e.NU_NOTA_REDACAO) AS NOTA_REDACAO,
    AVG((e.NU_NOTA_CN + e.NU_NOTA_CH + e.NU_NOTA_LC + e.NU_NOTA_MT + e.NU_NOTA_REDACAO)/5) AS MEDIA_GERAL_ENEM
FROM
    raw.ENEM_2019_SEDAP e
INNER JOIN
    raw.SAEB_ALUNO_3EM_2019_SEDAP s
    ON e.CPF_MASC = s.CPF_MASC
WHERE
    e.NU_NOTA_CN IS NOT NULL
    AND e.NU_NOTA_CH IS NOT NULL
    AND e.NU_NOTA_LC IS NOT NULL
    AND e.NU_NOTA_MT IS NOT NULL
    AND e.NU_NOTA_REDACAO IS NOT NULL
GROUP BY
    TIPO_ESCOLA,
    ADEQ_MT,
    ADEQ_LP
ORDER BY
    TIPO_ESCOLA,
    ADEQ_MT,
    ADEQ_LP;
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
