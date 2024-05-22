import logging
import warnings
import pytz
import re
import hashlib
import pandas as pd
import numpy as np
import json
from google.cloud import bigquery
from datetime import datetime
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from nltk.tokenize import RegexpTokenizer
from gensim import corpora, models
from module.as_config import BQ_CONFIG, APP_STORE_SCRAPING_TABLE, APP_STORE_LOG_TABLE, APP_STORE_NEG_REASON_RESULT_TABLE, APP_STORE_TOPIC_LOG_TABLE, reason_model, reason_map
from module.bq_connection import BQConnection


logger = logging.getLogger("Negative Review Reason Generation")
warnings.filterwarnings("ignore")

class NegReasonGeneration(object):
    def __init__(self):
        self.credential_datamart = BQ_CONFIG["CRED"]
        self.project_id = BQ_CONFIG["PROJECT"]
        self.dataset = BQ_CONFIG["DB"]
        self.input_table = APP_STORE_SCRAPING_TABLE
        self.log_table = APP_STORE_LOG_TABLE
        self.log_topic_table = APP_STORE_TOPIC_LOG_TABLE
        self.output_table = APP_STORE_NEG_REASON_RESULT_TABLE
        self.model = reason_model
        self.reason_map = reason_map
        self.bq = BQConnection()
        self.stopword = StopWordRemoverFactory().create_stop_word_remover()
        self.stemmer = StemmerFactory().create_stemmer()
        self.regex = RegexpTokenizer(r'\w+')

    def clean_review(self,review):
        """
        Converts review to lowercase, removes non-alphabetic characters and stopwords, applies stemming
        """
        review = review.lower()
        review = re.sub(r'[^a-zA-Z]', ' ', review)
        review = re.sub(r'\s\s+', ' ', review)
        review = self.stopword.remove(review)
        review = self.stemmer.stem(review)
        logging.info(
                "Success: cleanse data at {}".format(
                    datetime.today()
                )
            )
        return review
    
    def tokenize_review(self, clean_review_df):
        """
        Tokenizes reviews, using a tokenizer object, and appends tokens to a list
        """
        texts = []
        for i in clean_review_df:
            tokens = self.regex.tokenize(i)
            texts.append(tokens)
        logging.info(
                "Success: generate token at {}".format(
                    datetime.today()
                )
            )
        return texts
    
    def convert_typo(self, text):
        # Typo conversion dictionary
        typo_indo = {
            "apk" : "aplikasi",
            "gak" : "tidak",
            "ga" : "tidak",
            "gk" : "tidak",
            "gabisa" : "tidak bisa",
            "claim" : "klaim",
            "smua" : "semua",
            "application" : "aplikasi",
            "bner" : "benar",
            "bener" : "benar",
            "mw" : "mau",
            "ngak" : "tidak",
            "udah" : "sudah",
            "udh" : "sudah",
            "kalo" : "kalau",
            "can" : "bisa",
            "cannot" : "tidak bisa",
            "cant" : "tidak bisa",
            "yg" : "yang",
            "tp" : "tapi",
            "gw" : "saya",
            "aq" : "saya",
            "aku" : "saya",
            "ak" : "saya",
            "gua" : "saya",
            "gue" : "saya",
            "apps" : "aplikasi",
            "app" : "aplikasi",
            "not" : "tidak",
            "login" : "buka",
            "logout" : "keluar",
            "eror" : "error",
            "aja" : "saja",
            "bikin" : "daftar",
            "buat" : "daftar"
        }
        
        # Split the text into words
        words = text.split()
        # Iterate through each word
        for i, word in enumerate(words):
            # Check if the word exists in the typo_indo dictionary
            if word.lower() in typo_indo:
                # Replace the word with its corresponding value from the dictionary
                words[i] = typo_indo[word.lower()]

        # Join the words back into a single string
        converted_text = ' '.join(words)
        logging.info(
                "Success: convert typo data at {}".format(
                    datetime.today()
                )
            )
        return converted_text

    def format_topics_sentences(self, ldamodel, corpus, mapping_dict):
        # Init output
        sent_topics_df = pd.DataFrame()

        # Get main topic in each document
        for i, row_list in enumerate(ldamodel[corpus]):
            row = row_list[0] if ldamodel.per_word_topics else row_list
            row = sorted(row, key=lambda x: (x[1]), reverse=True)
            # Get the Dominant topic, Perc Contribution and Keywords for each document
            for j, (topic_num, prop_topic) in enumerate(row):
                if j == 0:
                    topic_num = str(topic_num)
                    if topic_num in mapping_dict.keys():
                        value = mapping_dict[topic_num]
                    else:
                        value = "not found"
                    sent_topics_df = sent_topics_df._append(pd.Series([int(topic_num), round(prop_topic,4), value]), ignore_index=True)
                else:
                    break
        sent_topics_df.columns = ['Topic_Class', 'Perc_Contribution', 'Reason']
        logging.info(
                "Success: generate reason data at {}".format(
                    datetime.today()
                )
            )
        # Add original text to the end of the output
        return(sent_topics_df)
    

    def generate_reason(self):
        client = bigquery.Client(
                credentials=self.credential_datamart, project=self.project_id
            )
        jakarta_tz = pytz.timezone("Asia/Jakarta")
        jakarta_time = datetime.now(jakarta_tz)
        query = """
                SELECT  *
                FROM
                `{dataset}.{log_table}`
                WHERE
                created_at > date('{date}')
                qualify row_number() over (order by created_at desc)=1
                """.format(
            dataset = self.dataset, log_table = self.log_table, date = jakarta_time
        )
        query_job = client.query(query)
        log = query_job.result()
        log = log.to_dataframe()
        logging.info(
            "Success: load log data at {}".format(
                datetime.today()
            )
        )
        if len(log)>0:
            query = """
                SELECT  *
                FROM
                `{dataset}.{input_table}`
                WHERE
                job_id = '{id}'
                and rating<=2
                """.format(
            dataset = self.dataset, input_table = self.input_table, id = log["job_id"].values[0]
            )
            query_job = client.query(query)
            review = query_job.result()
            review = review.to_dataframe()
            logging.info(
                "Success: load log data at {}".format(
                    datetime.today()
                )
            )

            if len(review)>0:
                processed_review = review['review'].apply(lambda x: self.convert_typo(x))
                review['review_processed'] = processed_review
                review['review_processed'] = review['review_processed'].apply(lambda x: self.clean_review(x))
                # drop nan value
                review.dropna(subset=['review_processed'], inplace=True)
                if len(review)>0:
                    clean_token_neg = self.tokenize_review(review['review_processed'])
                    ldamodel = models.ldamodel.LdaModel.load(self.model)
                    dictionary = ldamodel.id2word
                    # convert tokenized documents into a document-term matrix
                    corpus = [dictionary.doc2bow(text) for text in clean_token_neg]
                    with open(self.reason_map, "r") as json_file:
                        loaded_data = json.load(json_file)
                    df_topic_sents_keywords = self.format_topics_sentences(ldamodel=ldamodel, corpus=corpus,  mapping_dict=loaded_data)
                    # Format
                    df_dominant_topic = df_topic_sents_keywords.reset_index()
                    df_dominant_topic.columns = ['Document_No', 'Topic_Class', 'Topic_Perc_Contrib', 'Reason']
                    review = review.reset_index().merge(df_dominant_topic, how = 'left', left_index = True, right_index = True).drop(columns=['index','Document_No', 'created_at'])
                    jakarta_tz = pytz.timezone("Asia/Jakarta")
                    jakarta_time = datetime.now(jakarta_tz)
                    review["created_at"] = jakarta_time
                    
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        schema=[
                            bigquery.SchemaField("created_at", "DATETIME"),
                            bigquery.SchemaField("job_id", "STRING")
                        ],
                    )
                    self.bq.to_bq(
                            review,
                            self.dataset + "." + self.output_table,
                            self.credential_datamart,
                            self.project_id,
                            job_config,
                        )
                    logging.info(
                        "Success: ingest reason data at {}".format(
                            datetime.today()
                        )
                    )
                    scrap_id = review["job_id"].values[0]
                    # Convert datetime to string
                    datetime_str = str(jakarta_time)

                    # Calculate the SHA-256 hash
                    hash_object = hashlib.sha256(datetime_str.encode())
                    hash_hex = hash_object.hexdigest()
                    log_data = {'created_at': [jakarta_time], 'scrap_job_id': [scrap_id], 'job_id': [hash_hex]}
                    log_data = pd.DataFrame(log_data)
                    

            else:
                logging.info(
                    "No data to predict the reason {}".format(
                        datetime.today()
                    )
                )
                jakarta_tz = pytz.timezone("Asia/Jakarta")
                jakarta_time = datetime.now(jakarta_tz)
                # Convert datetime to string
                datetime_str = str(jakarta_time)

                # Calculate the SHA-256 hash
                hash_object = hashlib.sha256(datetime_str.encode())
                hash_hex = hash_object.hexdigest()
                log_data = {'created_at': [jakarta_time], 'scrap_job_id': [''], 'job_id': [hash_hex]}
                log_data = pd.DataFrame(log_data)
        else:
            logging.info(
                "No data to predict the reason {}".format(
                    datetime.today()
                )
            )
            jakarta_tz = pytz.timezone("Asia/Jakarta")
            jakarta_time = datetime.now(jakarta_tz)
            # Convert datetime to string
            datetime_str = str(jakarta_time)

            # Calculate the SHA-256 hash
            hash_object = hashlib.sha256(datetime_str.encode())
            hash_hex = hash_object.hexdigest()
            log_data = {'created_at': [jakarta_time], 'scrap_job_id': [''], 'job_id': [hash_hex]}
            log_data = pd.DataFrame(log_data)


        job_config = bigquery.LoadJobConfig(
                            write_disposition="WRITE_APPEND",
                            schema=[
                                bigquery.SchemaField("created_at", "DATETIME"),
                                bigquery.SchemaField("job_id", "STRING")
                            ],
                        )
                    
        self.bq.to_bq(
                            log_data,
                            self.dataset + "." + self.log_topic_table,
                            self.credential_datamart,
                            self.project_id,
                            job_config,
                        )
        logging.info(
            "Finished inserting rows of log data at {}".format(
                datetime.today()
            )
        )
