import cloudscraper
from time import sleep
from array import ArrayType
import requests
from typing import List
import json
import time, random
from datetime import datetime
import sqlalchemy
from bs4 import BeautifulSoup
import warnings
import pandas as pd
import numpy as np
import concurrent.futures
import Module.Logs.logs as log
from Module.Persistence.connection import connect_to_postgreSQL as bdpostgre
from Module.Persistence.connection import connect_to_s3 as s3
import pathlib, os
from dotenv import load_dotenv


class ExchangeRates:
    """Class used for obtain exchange rates of the day.
    
    settings are obtained from S3 repository via request to bucket.
    Params:
        info_date (str): date of the day to extract exchange rate.
    """
    warnings.simplefilter(action="ignore", category=FutureWarning)
    def __init__(self, info_date, **kwargs) -> None:
        load_dotenv()
        self.structured = os.getenv("INTELICA_DEVOPS")
        info_settings = s3().get_object(
            self.structured, "app-interchange/config/proxy_settings.json", "", False
        )
        settings = json.loads(info_settings["Body"].read())
        self.HEADERS_VISA = settings.get("header_settings").get("HEADERS_VISA")
        self.brand_visa = "VISA"
        self.VERIFY: bool = True

        if info_date is None:
            info_date = datetime.now().strftime("%m/%d/%Y")
            self.info_date = info_date
        else:
            info_date = info_date.strftime("%m/%d/%Y")
            self.info_date = info_date

        super().__init__(**kwargs)

    def get_currency_list_visa(self) -> List:
        """List of exchange rates available on the date the process was executed of Visa"""
        visa = requests.get(
            "https://usa.visa.com/support/consumer/travel-support/exchange-rate-calculator.html",
            headers=self.HEADERS_VISA,
            verify=self.VERIFY,
        )
        body = BeautifulSoup(visa.text, "html.parser")
        calculator_data = body.find("dm-calculator").get("content")
        currency_list = json.loads(calculator_data)
        currency_list = currency_list["currencyList"]
        currency_keys_visa = [
            types["key"] for types in currency_list if types["key"] != "None"
        ]
        results_visa = []
        for type_1 in currency_keys_visa:
            for type_2 in currency_keys_visa:
                if type_1 != type_2:
                    results_visa.append([type_1, type_2])
        return results_visa

    def initial_exchange_conversor_visa(self, element_visa) -> ArrayType:
        """Exchange rate extractor from VISA information to be executed in multithreading
        
        Args:
            element_visa (list): list of currency for exchange
        Returns:
            type_1 (str): currency from
            type_2 (str): currency to
            value (str): value for combination of type 1 and type 2 currency.
        
        """
        type_1 = element_visa[0]
        type_2 = element_visa[1]
        visa_params = {
            "amount": "1",
            "fee": "0",
            "utcConvertedDate": f"{self.info_date}",
            "exchangedate": f"{self.info_date}",
            "fromCurr": f"{type_2}",
            "toCurr": f"{type_1}",
        }
        try:
            scraper = cloudscraper.create_scraper()
            visa = scraper.get(
                "https://usa.visa.com/cmsapi/fx/rates",
                params=visa_params,
                timeout=2,
            )
            sleep(1)  # D. Morales: Temporary to test if enough to beat Cloudfare rate limiting!
            response = json.loads(visa.content)

            if len(response.keys()) == 1:
                visa_params["fromCurr"] = type_1
                visa_params["toCurr"] = type_2

                visa = scraper.get(
                    "https://usa.visa.com/cmsapi/fx/rates",
                    params=visa_params,
                    timeout=2,
                )
                sleep(1)  # D. Morales: Temporary to test if enough to beat Cloudfare rate limiting!
                response = json.loads(visa.content)
                value = float(str(response["reverseAmount"]).replace(",", ""))

            else:
                value = float(
                    str(response["originalValues"]["fxRateVisa"]).replace(",", "")
                )
        except Exception:
            value = np.nan
            return type_1, type_2, value
        return type_1, type_2, value

    def reprocess_exchange_conversor_visa(self, list_visa, proxy_element) -> ArrayType:
        """Visa multiprocess that takes the time of mastercard but that is used once the initial price has been executed to reprocess exchange rates that were not successfully achieved in the initial process
        
        Args:
            list_visa (list):  list of currency for exchange
            proxy_element (list): list of proxy for beign used
        Returns:
            result_visa_list (list): result of obtained data.
            proxy_element (list): proxy element.
        
        """
        proxy = proxy_element["proxy"]
        proxy_dict = {"http": proxy, "https": proxy}

        if list_visa != []:
            result_visa_list = []
            for element_visa in list_visa:
                type_1 = element_visa[0]
                type_2 = element_visa[1]
                visa_params = {
                    "amount": "1",
                    "fee": "0",
                    "utcConvertedDate": f"{self.info_date}",
                    "exchangedate": f"{self.info_date}",
                    "fromCurr": f"{type_2}",
                    "toCurr": f"{type_1}",
                }

                try:
                    scraper = cloudscraper.create_scraper()
                    visa = scraper.get(
                        "https://usa.visa.com/cmsapi/fx/rates",
                        params=visa_params,
                        timeout=2,
                        proxies=proxy_dict,
                    )
                    sleep(1)  # D. Morales: Temporary to test if enough to beat Cloudfare rate limiting!
                    response = json.loads(visa.content)

                    if len(response.keys()) == 1:
                        visa_params["fromCurr"] = type_1
                        visa_params["toCurr"] = type_2

                        visa = scraper.get(
                            "https://usa.visa.com/cmsapi/fx/rates",
                            params=visa_params,
                            timeout=3,
                            proxies=proxy_dict,
                        )
                        sleep(1)  # D. Morales: Temporary to test if enough to beat Cloudfare rate limiting!
                        response = json.loads(visa.content)
                        value = float(str(response["reverseAmount"]).replace(",", ""))

                    else:
                        value = float(
                            str(response["originalValues"]["fxRateVisa"]).replace(
                                ",", ""
                            )
                        )
                except Exception:
                    value = np.nan
                    proxy_element["status"] = "inactive"

                data = (type_1, type_2, value)
                result_visa_list.append(data)

            return result_visa_list, proxy_element

        else:
            return [], proxy_element

    def run_full_initial_visa(self, available_list) -> pd.DataFrame:
        """Performer that requests the information from the VISA page of the available exchange rates that are an input to this function along with a proxy list
        
        Args:
           available_list (list): list of currency.
        Returns:
            df_conversor_visa (Dataframe): data extracted with the exchange rates.
        
        """
        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            results_visa = executor.map(
                self.initial_exchange_conversor_visa, available_list
            )

        header_date = datetime.strptime(str(self.info_date), "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )
        all_conversor_changes_visa = [
            [type_1, type_2, value, header_date, self.brand_visa]
            for (type_1, type_2, value) in results_visa
        ]
        df_conversor_visa = pd.DataFrame(
            all_conversor_changes_visa,
            columns=[
                "currency_from",
                "currency_to",
                "exchange_value",
                "date",
                "brand",
            ],
        )
        df_conversor_visa = df_conversor_visa.reindex(
            columns=["date", "brand", "currency_from", "currency_to", "exchange_value"]
        )
        return df_conversor_visa

    def run_full_visa(self, available_list, proxy_list) -> pd.DataFrame:
        """Performer that requests the information from the VISA page of the available exchange rates that are an input to this function along with a proxy list
        
        Args:
           available_list (list): list of currency.
           proxy_list (list): list of available proxy
        Returns:
            df_conversor_visa (Dataframe): data extracted with the exchange rates.
            inactive_proxies (list): list of inactive proxys.
        
        """
        active_proxy_list = [i for i in proxy_list if i["status"] == "active"]
        divider = np.array_split(available_list, len(active_proxy_list))

        with concurrent.futures.ProcessPoolExecutor() as executor:
            results_v = executor.map(
                self.reprocess_exchange_conversor_visa, divider, active_proxy_list
            )

        results_visa = []
        inactive_proxies = []
        for i in results_v:
            if i[1]["status"] == "inactive":
                inactive_proxies.append(i[1])
            for a in i[0]:
                results_visa.append(a)
        header_date = datetime.strptime(str(self.info_date), "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )
        all_conversor_changes_visa = [
            [type_1, type_2, value, header_date, self.brand_visa]
            for (type_1, type_2, value) in results_visa
        ]
        df_conversor_visa = pd.DataFrame(
            all_conversor_changes_visa,
            columns=[
                "currency_from",
                "currency_to",
                "exchange_value",
                "date",
                "brand",
            ],
        )
        df_conversor_visa = df_conversor_visa.reindex(
            columns=["date", "brand", "currency_from", "currency_to", "exchange_value"]
        )

        return df_conversor_visa, inactive_proxies

    def combiner_process(self, log_name) -> pd.DataFrame:
        """Executor of the process for the generation of all information on visa and mastercard exchange rates considering reprocesses
        
        Args:
           log_name (str): name of log file.
        Returns:
           all_data (pd.Dataframe): processed data
        """
        module_name = "EXCHANGE RATE"
        all_visa = self.get_currency_list_visa()
        info_settings_visa = s3().get_object(
            self.structured, "app-interchange/config/proxy_settings.json", "", False
        )
        settings_visa = json.loads(info_settings_visa["Body"].read())
        proxy_list_visa = settings_visa.get("proxy_settings").get("proxy_list")

        log.logs().exist_file(
            "EXCHANGE_RATE",
            "INTELICA",
            "VISA",
            log_name,
            "PROCESSING EXCHANGE RATES",
            "INFO",
            "amount of exchange rates to processing : " + str(len(all_visa)),
            module_name,
        )

        visa_process = self.run_full_initial_visa(all_visa)
        visa_data = pd.DataFrame(visa_process)
        visa_reprocess = visa_data.query("exchange_value.isnull()", engine="python")

        counter_visa = 0
        initial_proxy_list_visa = proxy_list_visa
        r_analyze_proxy_visa = [
            proxy for proxy in initial_proxy_list_visa if proxy["status"] == "active"
        ]

        original_info_settings_visa = s3().get_object(
            self.structured, "app-interchange/config/proxy_settings.json", "", False
        )
        original_settings_visa = json.loads(original_info_settings_visa["Body"].read())
        original_proxy_list_visa = original_settings_visa.get("proxy_settings").get(
            "proxy_list"
        )
        quality_proxies_visa = (
            len(r_analyze_proxy_visa) / len(initial_proxy_list_visa)
        ) * 100
        quantity_missing_visa = (len(visa_reprocess) / len(visa_data)) * 100

        if visa_reprocess.empty:
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "VISA",
                log_name,
                "NO UNPPROCESSED TYPES HAVE BEEN DETECTED",
                "INFO",
                "amount of exchange rates to reprocess : " + str(len(visa_reprocess)),
                module_name,
            )
        else:
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "VISA",
                log_name,
                f"PROCESS IDENTIFIES A QUALITY OF PROXIES OF THE {str(round(quality_proxies_visa,2))}% AND A NUMBER OF MISSING OF THE {str(len(visa_reprocess))} ({str(round(quantity_missing_visa,2))}%)",
                "INFO",
                f"Analyzing to respond to the process",
                module_name,
            )

            if quality_proxies_visa < 50:
                if quantity_missing_visa <= 3:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "VISA",
                        log_name,
                        f"PROCESS NEEDS MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE ONLY {len(r_analyze_proxy_visa)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Giving a 20 minutes timeout to restart proxies",
                        module_name,
                    )
                    time.sleep(1200)
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "VISA",
                        log_name,
                        f"PROCESS WITH PROXIES ENABLED",
                        "INFO",
                        f"Starting reprocess, time out completed",
                        module_name,
                    )

                    initial_proxy_list_visa = original_proxy_list_visa
                else:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "VISA",
                        log_name,
                        f"PROCESS NEEDS MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE ONLY {len(r_analyze_proxy_visa)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Giving a 25 minutes timeout to restart proxies",
                        module_name,
                    )
                    time.sleep(1500)
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "VISA",
                        log_name,
                        f"PROCESS WITH PROXIES ENABLED",
                        "INFO",
                        f"Starting reprocess, time out completed",
                        module_name,
                    )
                    initial_proxy_list_visa = original_proxy_list_visa

            else:
                if quantity_missing_visa <= 3:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "VISA",
                        log_name,
                        f"PROCESS HAS THE MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE {len(r_analyze_proxy_visa)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Starting reprocess",
                        module_name,
                    )
                    initial_proxy_list_visa = original_proxy_list_visa

                else:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "VISA",
                        log_name,
                        f"PROCESS HAS THE MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE {len(r_analyze_proxy_visa)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Giving a 5 minutes timeout to restart proxies and start reprocess",
                        module_name,
                    )
                    time.sleep(300)
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "VISA",
                        log_name,
                        f"PROCESS WITH PROXIES ENABLED",
                        "INFO",
                        f"Starting reprocess, time out completed",
                        module_name,
                    )
                    initial_proxy_list_visa = original_proxy_list_visa

        while not visa_reprocess.empty:
            counter_visa += 1
            r_available_proxy_visa = [
                proxy
                for proxy in initial_proxy_list_visa
                if proxy["status"] == "active"
            ]

            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "VISA",
                log_name,
                "COUNTER REPROCESSING UNPROCESSED EXCHANGE RATES: "
                + str(int(counter_visa)),
                "INFO",
                "amount of exchange rates to reprocess : "
                + str(len(visa_reprocess))
                + f" with {len(r_available_proxy_visa)} active proxies",
                module_name,
            )

            if len(r_available_proxy_visa) == 0:
                missing_exchange_rates_visa = len(visa_reprocess)
                log.logs().exist_file(
                    "EXCHANGE_RATE",
                    "INTELICA",
                    "VISA",
                    log_name,
                    "PROCESS UNABLE TO CONTINUE WITH 0 PROXIES ENABLED FOR REPROCESSING",
                    "ERROR",
                    f"Finishing obtaining exchange rates from VISA, missing {missing_exchange_rates_visa} exchange rates",
                    module_name,
                )
                break

            reprocess_visa_data = visa_reprocess[
                ["currency_from", "currency_to"]
            ].values.tolist()

            visa_r = self.run_full_visa(reprocess_visa_data, r_available_proxy_visa)
            visa_r_data = pd.DataFrame(visa_r[0])
            r_inactive_proxy_visa = visa_r[1]

            if len(r_inactive_proxy_visa) > 0:
                for i in initial_proxy_list_visa:
                    for r in r_inactive_proxy_visa:
                        if i["proxy"] == r["proxy"]:
                            i["status"] = r["status"]

            for index, row in visa_r_data.iterrows():
                a = row["currency_from"]
                b = row["currency_to"]
                value = row["exchange_value"]
                visa_data.loc[
                    (visa_data["currency_from"] == a) & (visa_data["currency_to"] == b),
                    "exchange_value",
                ] = value

            visa_reprocess = visa_data.query("exchange_value.isnull()", engine="python")

        log.logs().exist_file(
            "EXCHANGE_RATE",
            "INTELICA",
            "VISA",
            log_name,
            "PROCESSING EXCHANGE RATES",
            "INFO",
            "completed",
            module_name,
        )
        
        frames = [visa_data]
        all_data = pd.concat(frames, ignore_index=True)

        return all_data

    def updater_process(self) -> None:
        """Process that connects to the database and updates the exchange rates."""
        module_name = "EXCHANGE RATE"
        log_name = log.logs().new_log(
            "EXCHANGE_RATE",
            "",
            "INTELICA",
            "GET VISA EXCHANGE RATES",
            "SYSTEM",
            module_name,
        )

        date_input = datetime.strptime(str(self.info_date), "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )

        log.logs().exist_file(
            "EXCHANGE_RATE",
            "INTELICA",
            "VISA",
            log_name,
            "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
            "INFO",
            "in process",
            module_name,
        )

        try:
            df = pd.DataFrame(self.combiner_process(log_name))
            check_codes = pd.DataFrame(bdpostgre().select("operational.m_currency"))

            for index, row in check_codes.iterrows():
                a = row["currency_alphabetic_code"]
                b = row["currency_numeric_code"]

                df.loc[
                    (df["currency_from"] == a),
                    "currency_from_code",
                ] = b
                df.loc[
                    (df["currency_to"] == a),
                    "currency_to_code",
                ] = b

            file_date_standard = datetime.strptime(self.info_date, "%m/%d/%Y")
            file_date = file_date_standard.strftime("%Y%m%d")
            file_date_format = file_date_standard.strftime("%Y-%m-%d")
            execution_detail = int(float(datetime.now().timestamp()))
            file_name = f"{file_date}_{execution_detail}"
            local_route = f"FILES/EXCHANGE_RATE/"
            pathlib.Path(local_route).mkdir(parents=True, exist_ok=True)
            s3_route = f"EXCHANGE_RATE/{file_name}.parquet"
            local_file_parquet = f"{local_route}{file_name}.parquet"
            df.to_parquet(local_file_parquet)
            structured = os.getenv("STRUCTURED_BUCKET")
            upload = s3().upload_object(structured, local_file_parquet, s3_route)
            db = bdpostgre().prepare_engine()
            db.execution_options(autocommit=False)
            table_new = f"tmp_exchange_rate_{file_name}"
            schem = "temporal"
            tran = None
            df = pd.read_parquet(
                path=local_file_parquet, engine="fastparquet", storage_options=None
            )
            df.index = np.arange(1, len(df) + 1)
            df["app_id"] = df.index
            df["app_type_file"] = "EXCHANGE_RATE"
            df["app_processing_date"] = df["date"]
            list_of_columns = list(df.columns)
            list_of_columns = ",".join(list_of_columns)
            
            rows_inserted = bdpostgre().insert_from_dataframe(
                table_new,
                schem,
                df,
                if_exists="replace",
                dtype={
                    "date": sqlalchemy.DateTime,
                    "exchange_value": sqlalchemy.Numeric,
                    "app_processing_date": sqlalchemy.Date,
                },
            )
            check_reprocces = bdpostgre().select(
                "OPERATIONAL.DH_EXCHANGE_RATE",
                f"""WHERE date = '{file_date_format}' and brand = 'VISA'""",
                "count(date)",
            )

            if check_reprocces[0]["count"] > 1:
                log.logs().exist_file(
                    "EXCHANGE_RATE",
                    "INTELICA",
                    "VISA",
                    log_name,
                    "THE INFORMATION HAS ALREADY BEEN LOADED WITH THAT DATE",
                    "WARNING",
                    "clear the data with that date in the operational table",
                    module_name,
                )
                sql3 = f"drop table {schem}.{table_new};"
                bdpostgre().execute_block(sql3)

            else:
                sql2 = f"""
                    insert into operational.dh_exchange_rate({list_of_columns}) select {list_of_columns} from
                    {schem}.{table_new} """
                rs = 0
                rs = bdpostgre().execute_block(sql2,True)

                log.logs().exist_file(
                    "EXCHANGE_RATE",
                    "INTELICA",
                    "VISA",
                    log_name,
                    "UPDATING OPERATIONAL EXCHANGE RATE DATA TABLE",
                    "INFO",
                    "inserted rows : " + str(rs[1]),
                    module_name,
                )

                sql3 = f"drop table {schem}.{table_new};"
                bdpostgre().execute_block(sql3)

            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "VISA",
                log_name,
                "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
                "INFO",
                "finished",
                module_name,
            )

        except Exception as e:
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "VISA",
                log_name,
                "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
                "ERROR",
                "A critical error has been detected, review or update the exchange rate configuration file: "
                + str(e),
                module_name,
            )
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "VISA",
                log_name,
                "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
                "ERROR",
                "Closing process with error",
                module_name,
            )