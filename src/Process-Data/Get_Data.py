import os
import random
import sqlite3
import time
import sys
from datetime import date, datetime, timedelta

from tqdm import tqdm

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from Utils.tools import get_json_data, to_data_frame

url = 'https://stats.nba.com/stats/' \
      'leaguedashteamstats?Conference=&' \
      'DateFrom=10%2F01%2F{2}&DateTo={0}%2F{1}%2F{3}' \
      '&Division=&GameScope=&GameSegment=&LastNGames=0&' \
      'LeagueID=00&Location=&MeasureType=Base&Month=0&' \
      'OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&' \
      'PerMode=PerGame&Period=0&PlayerExperience=&' \
      'PlayerPosition=&PlusMinus=N&Rank=N&' \
      'Season={4}' \
      '&SeasonSegment=&SeasonType={5}&ShotClockRange=&' \
      'StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='

year = [2022, 2023]
season = ["2022-23"]
month = [10, 11, 12, 1, 2, 3, 4, 5, 6]
days = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]

begin_year_pointer = year[0]
end_year_pointer = year[0]
count = 0

con = sqlite3.connect("../../Data/teams.sqlite")

for season1 in tqdm(season):
    for month1 in tqdm(month):
        if month1 == 1:
            count += 1
            end_year_pointer = year[count]
        for day1 in tqdm(days):
            if month1 == 10 and day1 < 19:
                continue
            if month1 in [4, 6, 9, 11] and day1 > 30:
                continue
            if month1 == 2 and day1 > 28:
                continue
            if end_year_pointer == datetime.now().year:
                if month1 == datetime.now().month and day1 > datetime.now().day:
                    continue
                if month1 > datetime.now().month:
                    continue

            if month1 < 10:
                season_type = "Regular+Season"
            else:
                season_type = "Playoffs"

            if month1 == 6:
                season_type = "Finals"

            general_data = get_json_data(url.format(month1, day1, begin_year_pointer, end_year_pointer, season1, season_type))
            general_df = to_data_frame(general_data)
            real_date = date(year=end_year_pointer, month=month1, day=day1) + timedelta(days=1)
            general_df['Date'] = str(real_date)

            x = str(real_date).split('-')
            general_df.to_sql(f"teams_{season1}-{str(int(x[1]))}-{str(int(x[2]))}", con, if_exists="replace")

            time.sleep(random.randint(1, 3))
    begin_year_pointer = year[count]

con.close()
