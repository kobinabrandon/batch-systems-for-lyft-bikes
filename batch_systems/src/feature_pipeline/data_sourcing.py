"""
Much of the code in this module is concerned with downloading the zip files
that contain raw data, extracting their contents, and loading said contents
as dataframes.

I have since decided to restrict my training data to data from 2024 so that I
won't have to deal with the demands on my memory and year_and_month that preprocessing
that data would have required (not to speak of the training and testing of
models).
"""
import re
import os
from typing import final
import requests
import pandas as pd

from loguru import logger
from zipfile import ZipFile
from datetime import datetime as dt
from argparse import ArgumentParser

from src.setup.paths import RAW_DATA_DIR, make_needed_directories


@final
class DataDownloader:
    def __init__(self, city_name: str, year: int):  
        
        self.city_name: str = city_name.lower()
        self.year = year

        self.service_names = {
            "bay_area": "bay-wheels",
            "chicago": "divvybikes",
            "new_york": "citibikenyc",
            "columbus": "cogobikeshare",
            "washington_dc": "capitalbikeshare",
            "portland": "biketownpdx"
        }
  
        assert self.city_name in self.service_names.keys(), "The named city is not part of Lyft's system"

    def load_raw_data(self, just_download: bool, months: list[int] | None = None) -> pd.DataFrame | None:
        """
        Download or load the data for either the specified months of the year in question, 
        or for all months up to the present month (if the data being sought is from this year).

        Args:
            year (int): the year whose data we want to load
            months (list[int] | None, optional): the months for which we want data
        """
        make_needed_directories()
        is_current_year = True if self.year == dt.now().year else False
        end_month = dt.now().month if is_current_year else 12
        months_to_query_for = range(1, end_month + 1) if months is None else months
 
        data = pd.DataFrame()
        for month in months_to_query_for:
            file_name = self.get_data_file_name(month=month)
            
            if just_download:
                if self.data_file_exists(file_name=file_name):     
                    logger.success(f"{file_name} is already saved to disk")
                else:
                    data_for_the_month = self.download_one_file_of_raw_data(month=month, keep_zipfile=False)
            else:
                if self.data_file_exists(file_name=file_name):
                    logger.success(f"{file_name} is already saved to disk")
                    data_for_the_month: pd.DataFrame = pd.read_csv(RAW_DATA_DIR/f"{file_name}/{file_name}.csv")  
                else:
                    data_for_the_month = self.download_one_file_of_raw_data(month=month, keep_zipfile=False)
        
                data = pd.concat([data, data_for_the_month], axis=0)
                return data

    def city_has_data(self) -> bool:
        """
        Check whether the Lyft has provdided data for the city in question

        Returns:
            bool: True if the data exists, and false if it doesn't
        """
        if self.city_name in self.service_names.keys():
            service_name = self.service_names[self.city_name]

            # The page with the Bay Area data has a different URL format
            if self.city_name == "bay_area":
                system_data_url = f"https://lyft.com/bikes/{service_name}"
            else:
                system_data_url = f"https://{service_name}.com/system-data"
            
            logger.info(f"Checking whether Lyft has published any data for {self.city_name}")
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
            "new_york": f"{year_and_month}-citibike-tripdata",
            "columbus": f"{year_and_month}-cogo-tripdata",
            "washington_dc": f"{year_and_month}-capitalbikeshare-tripdata",
            "portland": f"{year_and_month}.csv"
        }

        return city_name_and_url[self.city_name]

    def get_url_for_city_data(self, month: int) -> str | None:

        city_name_and_url_head: dict[str, str] = {
            "chicago": f"divvy-tripdata.s3.amazonaws.com/",
            "new_york": f"s3.amazonaws.com/tripdata/",
            "columbus": f"cogo-sys-data.s3.amazonaws.com/",
            "washington_dc": f"s3.amazonaws.com/capitalbikeshare-data/",
            "portland": f"s3.amazonaws.com/biketown-tripdata-public/"
        }

        if self.city_name == "portland" and self.year > 2020:
            logger.error("Lyft doesn't provide data on Portland after 2020.")
        else:
            file_name: str = self.get_data_file_name(month=month)
            return "https://" + city_name_and_url_head[self.city_name] + file_name
   
    def data_file_exists(self, file_name: str):
       file_path = RAW_DATA_DIR/self.city_name/file_name
       return True if file_path.exists() else False

    def download_one_file_of_raw_data(self, month: int, keep_zipfile: bool = False) -> pd.DataFrame:
        """
        Download the data for a given year, specifying the month if necessary,
        and the file name for the downloaded file.

        If the HTTP request for the data is successful, download the zipfile
        containing the data, and extract the .csv file into a folder of the
        same name. The zipfile will be deleted by default, unless otherwise
        specified.

        Args:
            year (int): the year in question
            keep_zipfile (bool, optional): whether to keep the zipfile. . 
            month (int, optional): the month we want data for. Defaults to None.
        """
        url: str = self.get_url_for_city_data(month=month) 
        zipfile_name = self.get_zipfile_name(url=url)
       
        # Make request for the zipfile  
        response = requests.get(url) 
        
        # Prepare paths for the download and extraction of the zipfile 
        file_name = zipfile_name[:-4]
        folder_path = RAW_DATA_DIR / file_name  # Remove ".zip" from the name of the zipfile 
        zipfile_path = RAW_DATA_DIR / zipfile_name
        
        # Write the zipfile to the file system
        with open(file=zipfile_path, mode="wb") as directory:
            _ = directory.write(response.content)

        # Extract the zipfile
        with ZipFile(file=zipfile_path, mode="r") as zipfile:
            _ = zipfile.extract(f"{file_name}.csv", folder_path)  # Extract csv file
        
        if not keep_zipfile:
            os.remove(zipfile_path)
 
        data_for_the_month: pd.DataFrame = pd.read_csv(RAW_DATA_DIR/f"{file_name}/{file_name}.csv")
        return data_for_the_month
       
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


if __name__ == "__main__":

    parser = ArgumentParser()
    _ = parser.add_argument("--cities", nargs="+", type=str)
    _ = parser.add_argument("--year", type=int)
    args = parser.parse_args()
    
    for city in args.cities:
        processor = DataDownloader(city_name=city, year=args.year)
        _= processor.load_raw_data(just_download=True)        

