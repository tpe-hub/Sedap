gc()
rm(list = ls())

pacman:: p_load(httr, jsonlite, ggplot2, stringr, tidyr, openxlsx, writexl)

API_URL <- "https://backend.rpinep2.prd.app.rnp.br/ide/send-process/d5cad842-4c33-415d-aed0-d8a33e0872d2"
FILEPATH_DIR <- "INEP_DATA"
TOKEN_IDE <- "TOKEN_IDE"
PROFILE_ID <- "PROFILE_ID"

# ENEM 2018 + SUP 2019 ----------------------------------------------------

#Tercil - 558.35999
#Quartil - 581.27997
#Quintil - 597.7



SQL_QUERY <- "
SELECT
  s.NU_ANO_INGRESSO,

  -- A) fez ENEM 2018 e ingressou em 2019
  COUNT(*) AS N_ENEM2018_ING2019,

  -- B) fez ENEM 2018 e ingressou em 2019 em licenciatura (TP_GRAU_ACADEMICO='2')
  SUM(CASE WHEN s.TP_GRAU_ACADEMICO = '2' THEN 1 ELSE 0 END) AS N_ENEM2018_ING2019_LIC,

  -- C) fez ENEM 2018, nota >= 650, e ingressou em 2019
  SUM(
    CASE
      WHEN ((e.NU_NOTA_CN + e.NU_NOTA_CH + e.NU_NOTA_LC + e.NU_NOTA_MT + e.NU_NOTA_REDACAO) / 5.0) >= 597.7
      THEN 1 ELSE 0
    END
  ) AS N_ENEM2018_650_ING2019,

  -- D) fez ENEM 2018, nota >= 650, e ingressou em 2019 em licenciatura
  SUM(
    CASE
      WHEN ((e.NU_NOTA_CN + e.NU_NOTA_CH + e.NU_NOTA_LC + e.NU_NOTA_MT + e.NU_NOTA_REDACAO) / 5.0) >= 597.7
       AND s.TP_GRAU_ACADEMICO = '2'
      THEN 1 ELSE 0
    END
  ) AS N_ENEM2018_650_ING2019_LIC

FROM raw.ENEM_2018_SEDAP e
INNER JOIN raw.SUP_ALUNO_2019 s
  ON e.CPF_MASC = s.CPF_MASC

WHERE
  -- garante que a nota média existe (se alguma NU_ for NULL, exclui)
  e.NU_NOTA_CN IS NOT NULL
  AND e.NU_NOTA_CH IS NOT NULL
  AND e.NU_NOTA_LC IS NOT NULL
  AND e.NU_NOTA_MT IS NOT NULL
  AND e.NU_NOTA_REDACAO IS NOT NULL

  -- restrição: ingressante no ensino superior em 2019
  AND s.TP_SITUACAO ='2'
  AND s.IN_INGRESSO_TOTAL = 1
  AND s.NU_ANO_INGRESSO =2019


GROUP BY
  s.NU_ANO_INGRESSO

ORDER BY
  s.NU_ANO_INGRESSO
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




# ENEM 2018 ---------------------------------------------------------------

SQL_QUERY <- "
SELECT
  e.NU_ANO,

  -- total de pessoas com nota_enem (todas as NU_ não nulas)
  COUNT(*) AS N_COM_NOTA_ENEM,

  -- total com nota_enem >= corte
  SUM(
    CASE
      WHEN ((e.NU_NOTA_CN + e.NU_NOTA_CH + e.NU_NOTA_LC + e.NU_NOTA_MT + e.NU_NOTA_REDACAO) / 5.0) >= 597.7
      THEN 1 ELSE 0
    END
  ) AS N_NOTA_ENEM_ACIMA_CORTE

FROM raw.ENEM_2018_SEDAP e
WHERE
  e.NU_NOTA_CN IS NOT NULL
  AND e.NU_NOTA_CH IS NOT NULL
  AND e.NU_NOTA_LC IS NOT NULL
  AND e.NU_NOTA_MT IS NOT NULL
  AND e.NU_NOTA_REDACAO IS NOT NULL
GROUP BY
  e.NU_ANO
ORDER BY
  e.NU_ANO
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




# Superior 2019 -----------------------------------------------------------



SQL_QUERY <- "
SELECT
  e.NU_ANO_INGRESSO,
  COUNT(*) AS N_ING2019_TOTAL,
  SUM(CASE WHEN e.TP_GRAU_ACADEMICO = '2' THEN 1 ELSE 0 END) AS N_ING2019_LIC
FROM raw.SUP_ALUNO_2019 e
WHERE
  e.IN_INGRESSO_TOTAL = 1
  AND e.NU_ANO_INGRESSO = 2019
GROUP BY
  e.NU_ANO_INGRESSO
ORDER BY
  e.NU_ANO_INGRESSO
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



# Acesso ENEM 2018 SUP 2019 - Areas ---------------------------------------




SQL_QUERY <- "
SELECT
  s.NU_ANO_INGRESSO,

  CASE
  WHEN s.CO_CINE_ROTULO = '0421D01' THEN 'Direito'
  WHEN s.CO_CINE_ROTULO = '0912M01' THEN 'Medicina'
    WHEN s.CO_CINE_ROTULO IN (
      '0811V01','0811A05','0811C01','0811E01','0811A03','0821E01','0831P01','0821S01','0811M01','0812H01',
      '0811Z01','0831A01','0811I01','0811A01','0831E01','0811A04','0888P01','0841M01','0811A02','0811E02'
    ) THEN 'Agricultura, silvicultura, pesca e veterinária'

    WHEN s.CO_CINE_ROTULO IN (
      '0215M01','0212D02','0231L09','0222A01','0231L16','0231L18','0231L20','0215A01','0211P02','0212D03',
      '0215T01','0231L10','0231L13','0231L03','0221C01','0221T01','0211D01','0231L01','0212M01','0215D01',
      '0231L06','0213A02','0288P01','0211F01','0231L22','0211A01','0231L14','0231L04','0231L21','0212D04',
      '0231L12','0211C01','0222H01','0231L19','0222C01','0223F01','0231L05','0213A01','0231L07','0231L17',
      '0211P05','0213A03','0211C02','0211P01','0211P03','0211P04','0212D01','0231L15','0231L11','0213H01',
      '0214F01','0231L08','0231L02'
    ) THEN 'Artes e humanidades'

    WHEN s.CO_CINE_ROTULO IN (
      '0511B01','0541M01','0532O01','0533F03','0532G01','0512B02','0588P01','0533F02','0532M01','0531Q02',
      '0533F01','0533A01','0531Q01','0542C01','0532G03','0542E01','0521C01','0512B01','0521E01','0541M02',
      '0532G02','0512T01'
    ) THEN 'Ciências naturais, matemática e estatística'

    WHEN s.CO_CINE_ROTULO IN (
      '0312R01','0321R01','0312A01','0388P01','0322G01','0311E01','0312C01','0313P01','0312C02','0312G01',
      '0322M01','0321J01','0322B01','0322A01','0321C01','0312S01','0321P01'
    ) THEN 'Ciências sociais, comunicação e informação'

    WHEN s.CO_CINE_ROTULO IN (
      '0616S01','0612G01','0688P01','0612R01','0615S03','0615S01','0616E01','0614C01','0615S02','0612B01',
      '0613E01','0613J01','0612D01'
    ) THEN 'Computação e TIC'

    WHEN s.CO_CINE_ROTULO IN (
      '0115L07','0113E03','0114A02','0114P01','0114M01','0114C03','0111P01','0114E05','0115L19','0115L08',
      '0114E07','0115L02','0114E02','0113P01','0115L20','0114C04','0114Q01','0113E01','0114C05','0114H01',
      '0115L12','0114B01','0114C01','0115L13','0114T01','0113E02','0114E04','0115L06','0115L14','0112E01',
      '0114M02','0115L10','0115L17','0115L18','0115L16','0115L11','0114D01','0115L15','0114G01','0114A01',
      '0114E06','0114F01','0115L05','0113F01','0114E03','0115L01','0114F02','0115L04','0111P02','0114C02',
      '0115L03','0188P01'
    ) THEN 'Educação'

    WHEN s.CO_CINE_ROTULO IN (
      '0713E03','0721A01','0713E02','0711B01','0721E01','0731E03','0725E01','0731A01','0721L01','0723E01',
      '0710E01','0732C01','0715P01','0714E04','0724E01','0714G01','0712G01','0723P02','0715S01','0715E03',
      '0714S02','0713E04','0724E02','0711E05','0711E03','0715E01','0712E02','0716E03','0722P02','0731E02',
      '0714T01','0713E01','0713S01','0713E06','0715F01','0722C02','0732E03','0732E05','0714E01','0716C01',
      '0722P01','0712S01','0725E03','0724M01','0725P02','0714E02','0716E02','0711E01','0725E02','0716E05',
      '0731E01','0721P04','0713E05','0714E08','0714A01','0714E03','0723P01','0711E02','0732C02','0714S01',
      '0722E01','0714R01','0716E01','0724R01','0714M01','0716M01','0714E05','0714E07','0721P02','0732E01',
      '0716E04','0725P01','0716S01','0714E06','0732E02','0715M02','0712E01','0716S02','0724P01','0721P03',
      '0715E02','0711E04','0788P01','0724E03','0713R01','0731A02','0714E09','0715M01'
    ) THEN 'Engenharia, produção e construção'

    WHEN s.CO_CINE_ROTULO IN (
      '0413G04','0413G12','0414P01','0413C01','0411C01','0414R01','0412G01','0413G02','0416G01','0413G03',
      '0413E01','0413A02','0413A01','0416N01','0411G01','0412S01','0414M01','0413G06','0413G11',
      '0488P01','0413G05','0413G10','0413L01','0413G08','0415S01','0413G07','0421S01','0413G09','0413G01'
    ) THEN 'Negócios, administração e direito'

    WHEN s.CO_CINE_ROTULO IN (
      '0011A03','0011A05','0011A06','0011A02','0011A09','0011A07','0011A01'
    ) THEN 'Programas básicos'

    WHEN s.CO_CINE_ROTULO IN (
      '0921G01','0915E01','0918S01','0914B01','0917M01','0915F02','0915N01','0915T01','0921A01','0915P01',
      '0918S02','0923S01','0988P01','0914O02','0914O01','0913E01','0914R01','0917P01','0911O01',
      '0916F01','0915F01'
    ) THEN 'Saúde e bem-estar'

    WHEN s.CO_CINE_ROTULO IN (
      '1015T01','1041G01','1014G01','1015H01','1015E01','1032S02','1011E01','1012E01','1032S01','1032I01',
      '1013G01','1041T01','1032S03','1032S04','1041C01','1041T02','1014F01','1022S01','1031C01'
    ) THEN 'Serviços'

    ELSE 'Outras/Não mapeada'
  END AS AREA_GERAL,

  SUM(
  CASE
    WHEN (s.TP_GRAU_ACADEMICO IS NULL OR s.TP_GRAU_ACADEMICO <> '2')
    THEN 1 ELSE 0
  END
) AS N_TOTAL_ING2019_NAO_LIC,

  SUM(
    CASE
      WHEN ((e.NU_NOTA_CN + e.NU_NOTA_CH + e.NU_NOTA_LC + e.NU_NOTA_MT + e.NU_NOTA_REDACAO) / 5.0) >= 597.7
       AND (s.TP_GRAU_ACADEMICO IS NULL OR s.TP_GRAU_ACADEMICO <> '2')
      THEN 1 ELSE 0
    END
  ) AS N_NOTA597_NAO_LIC

FROM raw.ENEM_2018_SEDAP e
INNER JOIN raw.SUP_ALUNO_2019 s
  ON e.CPF_MASC = s.CPF_MASC

WHERE
  e.NU_NOTA_CN IS NOT NULL
  AND e.NU_NOTA_CH IS NOT NULL
  AND e.NU_NOTA_LC IS NOT NULL
  AND e.NU_NOTA_MT IS NOT NULL
  AND e.NU_NOTA_REDACAO IS NOT NULL
  AND s.TP_SITUACAO = '2'
  AND s.IN_INGRESSO_TOTAL = 1
  AND s.NU_ANO_INGRESSO = 2019

GROUP BY
  s.NU_ANO_INGRESSO,
  AREA_GERAL

ORDER BY
  s.NU_ANO_INGRESSO,
  AREA_GERAL
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


