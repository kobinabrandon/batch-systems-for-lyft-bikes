"""
Much of the code in this module is concerned with downloading the zip files
that contain raw data, extracting their contents, and loading said contents
as dataframes.

I have since decided to restrict my training data to data from 2024 so that I
won't have to deal with the demands on my memory and year_and_month that preprocessing
that data would have required (not to speak of the training and testing of
models).
"""
from collections.abc import Generator
import re
import os
from typing import final
import requests
import pandas as pd

from loguru import logger
from zipfile import ZipFile
from datetime import datetime as dt

from src.setup.paths import RAW_DATA_DIR, make_fundamental_paths


@final
class DataDownloader:
    def __init__(self, city_name: str, year: int):
        self.city_name: str = city_name.lower()
        self.year = year

        self.service_names = {
            "bay area": "bay-wheels",
            "chicago": "divvybikes",
            "new york": "citibikenyc",
            "columbus": "cogobikeshare",
            "washington dc": "capitalbikeshare",
            "portland": "biketownpdx"
        }
    
    def load_raw_data(self, months: list[int] | None, file_name: str) -> Generator[pd.DataFrame]:
        """
        Download or load the data for either the specified months of the year in question, 
        or for all months up to the present month (if the data being sought is from this year).

        Args:
            year (int): the year whose data we want to load
            months (list[int] | None, optional): the months for which we want data

        Yields:
            Generator[pd.DataFrame]: the requested datasets.
        """
        make_fundamental_paths()
        
        is_current_year = True if self.year == dt.now().year else False
        end_month = dt.now().month if is_current_year else 12
        months_to_query_for = range(1, end_month + 1) if months is None else months

        for month in months_to_query_for:
            try:
                self.check_for_file_or_download(month=month, file_name=file_name)
                yield self.get_dataframe_from_folder(file_name=file_name)
            except Exception as error:
                logger.error(error)


    def city_has_data(self) -> bool:
        """
        Check whether the Lyft has provdided data for the city in question

        Returns:
            bool: True if the data exists, and false if it doesn't
        """
        if self.city_name in self.service_names.keys():
            service_name = self.service_names[self.city_name]

            # The Bay Area site has a different format
            if self.city_name == "bay area":
                system_data_url = f"lyft.com/bikes/{service_name}"
            else:
                system_data_url = f"{service_name}.com/system-data"

            response = requests.get(url=system_data_url)
            return True if response.status_code == 200 else False

        else:
            raise Exception(
                f"Lyft has not published any data for {self.city_name.title()}"
            )

    def get_data_file_name(self, month: int) -> str:
        
        year_and_month = f"{self.year}{month:02d}"

        city_name_and_url = {
            "chicago": f"{year_and_month}-divvy-tripdata",
            "new york": f"{year_and_month}-citibike-tripdata",
            "columbus": f"{year_and_month}-cogo-tripdata",
            "washington dc": f"{year_and_month}-capitalbikeshare-tripdata",
            "portland": f"{year_and_month}.csv"
        }

        assert self.city_name in city_name_and_url.keys(), "The named city is not part of Lyft's system"
        return city_name_and_url[self.city_name]

    def get_url_for_city_data(self, month: int) -> str | None:

        city_name_and_url_head: dict[str, str] = {
            "chicago": f"divvy-tripdata.s3.amazonaws.com/",
            "new york": f"s3.amazonaws.com/tripdata/",
            "columbus": f"cogo-sys-data.s3.amazonaws.com/",
            "washington dc": f"s3.amazonaws.com/capitalbikeshare-data/",
            "portland": f"s3.amazonaws.com/biketown-tripdata-public/"
        }

        if self.city_name == "portland" and self.year > 2020:
            logger.error("Lyft doesn't provide data on Portland after 2020.")
        else:
            file_name: str = self.get_data_file_name(month=month)
            return city_name_and_url_head[self.city_name] + file_name

    def check_for_file_or_download(self, file_name: str, month: int | None):
        """
        Checks for the presence of a data file in the file system, and downloads it
        if necessary.

        Args:
            year (int): the year whose data we are looking to potentially download
            file_name (str): the name of the file to be saved to disk
            month (list[int] | None, optional): the month for which we seek data
        """
        if month is not None:
            local_file = RAW_DATA_DIR/file_name
            if not local_file.exists():
                try:
                    logger.info(f"Downloading and extracting {file_name}.zip")
                    self.download_one_file_of_raw_data(month=month)
                except Exception as error:
                    logger.error(error)
            else:
                logger.success(f"{file_name}.zip is already saved")


    def download_one_file_of_raw_data(self, month: int) -> None:
        """
        Download the data for a given year, specifying the month if necessary,
        and the file name for the downloaded file.

        Args:
            year (int): the year in question
            month (int, optional): the month we want data for. Defaults to None.
        """
        url: str = self.get_url_for_city_data(month=month) 
        zipfile_name = self.get_zipfile_name(url=url)

        self.download_and_extract_zipfile(
            zipfile_names_and_urls={zipfile_name: url}
        )
    
    @staticmethod
    def write_and_extract_zipfile(
            zipfile_name: str,
            response: requests.Response,
            keep_zipfile: bool = False
     ) -> None:
        """
        If the HTTP request for the data is successful, download the zipfile
        containing the data, and extract the .csv file into a folder of the
        same name. The zipfile will be deleted by default, unless otherwise
        specified.

        Args:
            zipfile_name (str): the name of the zipfile that we're downloading
            response (requests.Response): HTTP response from the requests object
            keep_zipfile (bool, optional): whether to keep the zipfile. .
        """
        file_name = zipfile_name[:-4]  # Remove ".zip" from the name of the zipfile
        folder_path = RAW_DATA_DIR / file_name
        zipfile_path = RAW_DATA_DIR / zipfile_name

        # Write the zipfile to the file system
        with open(file=zipfile_path, mode="wb") as directory:
            _ = directory.write(response.content)

        with ZipFile(file=zipfile_path, mode="r") as zipfile:
            _ = zipfile.extract(f"{file_name}.csv", folder_path)  # Extract csv file

        if not keep_zipfile:
            os.remove(zipfile_path)

    def download_and_extract_zipfile(
            self, zipfile_names_and_urls: dict[str, str]
    ) -> None:

        for zipfile_name, url in zipfile_names_and_urls.items():
            response = requests.get(url)
            self.write_and_extract_zipfile(zipfile_name=zipfile_name, response=response)

    @staticmethod
    def get_zipfile_name(url: str) -> str:
        """
        Use a regular expression to extract the name of the data file from its associated URL

        Args:
            url (str): the link to the data file being downloaded

        Returns:
            str: the name of the zipfile/raw data file to be downloaded
        """
        pattern = r"[^/]+(.*)"  # Exclude all the characters before the first slash
        match = re.search(pattern=pattern, string=url)

        if match:
            return match.group(1)
        else:
            raise Exception("Invalid URL")


    def get_dataframe_from_folder(self, file_name: str) -> pd.DataFrame:
        """
        Load a requested data file which has been downloaded and return it as dataframes.

        Args:
            file_name (str): the name of the file to be loaded.

        Returns:
            pd.DataFrame: the loaded dataframe
        """
        data = pd.DataFrame()
        data_one_month: pd.DataFrame = pd.read_csv(RAW_DATA_DIR/f"{file_name}/{file_name}.csv")
        data = pd.concat([data, data_one_month], axis=0)
        return data

