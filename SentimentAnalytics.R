# rm(list=ls())
# Connect to SQL Server Database - Control Panel - Administrative Tools - ODBC Data Source - Configure for the DB
# install.packages('RODBC') 
library('RODBC')

# Establish the Connection for EDW Database

# channel <- odbcConnect("ifs_prod", uid="SodexoDSViewer", pwd="R0q0Na#1531");

user_name = Sys.getenv('ifms_sql_server_user_name')
user_pass = Sys.getenv('ifms_sql_server_pass')

channel <- odbcConnect("EDW_SQL_SERVER", uid=user_name, pwd=user_pass);


# Run Query to Get the Data
df <- sqlQuery(channel, 
               
               "
               Select convert(date, convert(varchar(10), date_SID), 101) as Date, a.*
               FROM [dbo].[Fact_Service_Request] a 
               
               where 
               
               --convert(date, convert(varchar(10), date_SID), 101) >= '01-Jun-2018'
               --and convert(date, convert(varchar(10), date_SID), 101) <= '30-Jun-2018'
               --and service_request_number = N'SR-231386-X7X6'
               
               format(convert(date, convert(varchar(10), date_SID), 101),'yyyyMM') >= format(dateadd(month, -6, getdate()), 'yyyyMM')
               and classification in ('Compliment', 'Complaint','Feedback')

               ");

close(channel);



library(coreNLP)
library(syuzhet)
library(dplyr)
library(fortunes)
library(tidytext)
library(ggplot2)
library(tidytext)
library(stringr)
library(tm)
library(textdata)

# dataset=df

df$Service_Request_Description = as.character(df$Service_Request_Description)

df$text = gsub('[0-9]+', '', df$Service_Request_Description)
df$text = gsub('[^ -~]+', '', df$Service_Request_Description)

text = df$text

# library(tm)
text = VCorpus(VectorSource(text))
text = tm_map(text, content_transformer(tolower)) 
text <- tm_map(text, removeNumbers)
text = tm_map(text, removePunctuation)
text = tm_map(text, removeWords, stopwords("english"))
text <- tm_map(text, content_transformer(function(x){stringr::str_wrap(x)}) )

# text <- tm_map(text, content_transformer(function(x){stringr::str_wrap(stri_enc_toutf8(x,is_unknown_8bit=TRUE,validate=TRUE))}) )
text <- tm_map(text, content_transformer(function(x){stringr::str_replace_all(x, "\n", " ")}) )
text = tm_map(text, stripWhitespace)


# text

for (i in 1:length(df$Service_Request_Description)){
  df$text[i] = as.character(text[[i]])
  
  # print(i)
  # print(as.character(text[[i]]))
}


# head(df$text)


# Example
# df <- read.csv("https://goo.gl/mkDSD7", stringsAsFactors = F)


# Create an Affinity Data Frame to find positive and negative words

# install.packages('textdata')
# library(textdata)
# remove.packages('textdata')


# AFINN <- sentiments %>%
#   filter(lexicon == 'AFINN')

AFINN <- lexicon_afinn()


# Getting Positive and Negative Words

df_sentiments <- df %>% unnest_tokens(word, text) %>%
  anti_join(stop_words, by = 'word') %>%
  inner_join(AFINN, by = 'word')

df_sentiments = df_sentiments[with(df_sentiments,order(Service_Request_SID)),]


df$syuzhet <- get_sentiment(df$Service_Request_Description)
df$syuzhet_bing <- get_sentiment(df$Service_Request_Description, method = "bing")
df$syuzhet_afinn <- get_sentiment(df$Service_Request_Description, method = "afinn")
df$syuzhet_nrc <- get_sentiment(df$Service_Request_Description, method = "nrc")

# get the mood of the sentence

mood = get_nrc_sentiment(df$Service_Request_Description)
df = cbind(df,mood)

# library("RODBC")
# odbcDataSources(type = c("all", "user", "system"))


channel <- odbcConnect("Prod02-DS", uid=user_name, pwd=user_pass)


# getSqlTypeInfo()

columnTypes <- list('Date' = "varchar(255)", 
                    'Service_Request_SID' = "int",
                    'Service_Request_Number' = "varchar(255)",
                    'Date_SID' = "int", 
                    'Stream_SID' = "int",
                    'Location_SID' = "int",
                    'Classification' = "varchar(255)",
                    'Contract_SID' = "varchar(255)",
                    'syuzhet' = "float",
                    'syuzhet_bing' = "float",
                    'syuzhet_afinn' = "float",
                    'syuzhet_nrc' = "float",
                    'anger' = "float",
                    'anticipation' = "float",
                    'disgust' = "float",
                    'fear' = "float",
                    'joy' = "float",
                    'sadness' = "float",
                    'surprise' = "float",
                    'trust' = "float",
                    'negative' = "float",
                    'positive' = "float"
                    
)


keep = c("Date","Service_Request_SID","Service_Request_Number","Date_SID","Stream_SID","Location_SID","Classification","Contract_SID","syuzhet","syuzhet_bing","syuzhet_afinn",
         "syuzhet_nrc","anger","anticipation","disgust","fear","joy","sadness","surprise","trust","negative","positive")
FACT_Service_Request_Sentiment = df[keep]

date_from = Sys.Date() - 30

FACT_Service_Request_Sentiment = FACT_Service_Request_Sentiment[FACT_Service_Request_Sentiment$Date>=date_from,]
▲
# sqlQuery(channel,"Drop Table if Exists Fact_Service_Request_Sentiment")

tryCatch(
  
  {
    
    sqlQuery(channel,"Delete from FACT_Service_Request_Sentiment where convert(date,date) >= dateadd(day,-30,convert(date,getdate()))")
    sqlSave(channel, FACT_Service_Request_Sentiment, rownames = FALSE, varTypes = columnTypes,append = TRUE)
    
  }
  
  ,
  error = function(error_condition){
    sqlSave(channel, FACT_Service_Request_Sentiment, rownames = FALSE, varTypes = columnTypes,append = TRUE)
  }
)


columnTypes <- list('Date' = "varchar(255)", 
                    'Service_Request_SID' = "int", 
                    'Service_Request_NUmber' = "varchar(255)",
                    'Date_SID' = "int", 
                    'Stream_SID' = "int",
                    'Location_SID' = "int",
                    'Classification' = "varchar(255)",
                    'Contract_SID' = "varchar(255)",
                    'word' = "varchar(255)",
                    'value' = "float"
                     )

keep = c("Date","Service_Request_SID","Service_Request_Number","Date_SID","Stream_SID","Location_SID","Contract_SID","Classification","word","value")
FACT_Service_Request_Sentiment_Word = df_sentiments[keep]
FACT_Service_Request_Sentiment_Word = FACT_Service_Request_Sentiment_Word[FACT_Service_Request_Sentiment_Word$Date>=date_from,]

# sqlQuery(hannel,"Drop Table if Exists Fact_Service_Requst_Sentiment_Word")

tryCatch(
  
  {
    
    sqlQuery(channel,"Delete from FACT_Service_Request_Sentiment_Word where convert(date,date) >= dateadd(day,-30,convert(date,getdate()))")
    sqlSave(channel, FACT_Service_Request_Sentiment_Word, rownames = FALSE, varTypes = columnTypes,append = TRUE)
    
  }
  
  ,
  error = function(error_condition){
    sqlSave(channel, FACT_Service_Request_Sentiment_Word, rownames = FALSE, varTypes = columnTypes,append = TRUE)
  }
)



close(channel)








