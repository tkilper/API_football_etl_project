"""Connector and methods accessing Football API"""
import requests
import pandas as pd
import time
import logging

class FootballAPIConnector_Players():
    """
    Class for interacting with FootballAPI - Players Dataset
    """
    def __init__(self, url, querystring, headers):
        """
        Constructor for FootballAPIConnector

        :param url: url to access FootballAPI
        :param querystring: query string for FootballAPI call
        :param headers: headers for FootballAPI call
        """
        self.logger = logging.getLogger(__name__)
        self.url = url
        self.querystring = querystring
        self.headers = headers

    def read_json_to_df(self, raw_json):
        df = pd.DataFrame()
        for player in raw_json["response"]:
            df_info = pd.json_normalize(player["player"])
            df_stats = pd.json_normalize(player["statistics"][0])
            df_stats["id"] = player["player"]["id"]
            df_delta = pd.merge(df_info, df_stats, on='id', how='left')
            df = pd.concat([df, df_delta], ignore_index=True)
        return df

    def return_objects(self):
        dict_obj = requests.get(self.url, headers=self.headers, params=self.querystring)
        dict_obj = dict_obj.json()
        qs_temp = self.querystring
        max_page = dict_obj["paging"]["total"]
        current_page = 1
        objects = []
        while current_page <= max_page:
            objects.append(dict_obj)
            current_page = current_page + 1
            if current_page <= max_page:
                qs_temp = self.querystring
                qs_temp.update({'page':str(current_page)})
                dict_obj = requests.get(self.url, headers=self.headers, params=qs_temp)
                dict_obj = dict_obj.json()
        return objects
    
class FootballAPIConnector_Teams():
    """
    Class for interacting with FootballAPI - Teams Dataset
    """
    def __init__(self, url, querystring, headers):
        """
        Constructor for FootballAPIConnector

        :param url: url to access FootballAPI
        :param querystring: query string for FootballAPI call
        :param headers: headers for FootballAPI call
        """
        self.logger = logging.getLogger(__name__)
        self.url = url
        self.querystring = querystring
        self.headers = headers

    def read_json_to_df(self, raw_json):
        df = pd.DataFrame()
        for team in raw_json["response"]:
            df_info = pd.json_normalize(team["team"])
            df_ven = pd.json_normalize(team["venue"])
            df_ven["id_team"] = team["team"]["id"]
            df_delta = pd.merge(df_info, df_ven, left_on='id', right_on='id_team', how='left')
            df = pd.concat([df, df_delta], ignore_index=True)
        return df

    def return_objects(self):
        dict_obj = requests.get(self.url, headers=self.headers, params=self.querystring)
        dict_obj = dict_obj.json()
        qs_temp = self.querystring
        max_page = dict_obj["paging"]["total"]
        current_page = 1
        objects = []
        while current_page <= max_page:
            objects.append(dict_obj)
            current_page = current_page + 1
            if current_page <= max_page:
                qs_temp = self.querystring
                qs_temp.update({'page':str(current_page)})
                dict_obj = requests.get(self.url, headers=self.headers, params=qs_temp)
                dict_obj = dict_obj.json()
        return objects
    
class FootballAPIConnector_Venues():
    """
    Class for interacting with FootballAPI - Venues Dataset
    """
    def __init__(self, url, querystring, headers):
        """
        Constructor for FootballAPIConnector

        :param url: url to access FootballAPI
        :param querystring: query string for FootballAPI call
        :param headers: headers for FootballAPI call
        """
        self.logger = logging.getLogger(__name__)
        self.url = url
        self.querystring = querystring
        self.headers = headers

    def read_json_to_df(self, raw_json):
        df = pd.DataFrame()
        for ven in raw_json["response"]:
            df_delta = pd.json_normalize(ven)
            df = pd.concat([df, df_delta], ignore_index=True)
        return df

    def return_objects(self):
        dict_obj = requests.get(self.url, headers=self.headers, params=self.querystring)
        dict_obj = dict_obj.json()
        qs_temp = self.querystring
        max_page = dict_obj["paging"]["total"]
        current_page = 1
        objects = []
        while current_page <= max_page:
            objects.append(dict_obj)
            current_page = current_page + 1
            if current_page <= max_page:
                qs_temp = self.querystring
                qs_temp.update({'page':str(current_page)})
                dict_obj = requests.get(self.url, headers=self.headers, params=qs_temp)
                dict_obj = dict_obj.json()
        return objects
    
class FootballAPIConnector_Fixtures():
    """
    Class for interacting with FootballAPI - Fixtures Dataset
    """
    def __init__(self, url, querystring, headers):
        """
        Constructor for FootballAPIConnector

        :param url: url to access FootballAPI
        :param querystring: query string for FootballAPI call
        :param headers: headers for FootballAPI call
        """
        self.logger = logging.getLogger(__name__)
        self.url = url
        self.querystring = querystring
        self.headers = headers

    def read_json_to_df(self, raw_json):
        # create master fixtures dataframe
        df = pd.DataFrame()
        for fix in raw_json["response"]:
            df_delta = pd.json_normalize(fix)
            df = pd.concat([df, df_delta], ignore_index=True)
        return df

    def read_json_to_df_level_2_events(self, raw_json):
        df = pd.json_normalize(raw_json["response"])
        df['fixture_id'] = raw_json["parameters"]["fixture"]
        return df

    def read_json_to_df_level_2_lineups(self, raw_json):
        df = pd.DataFrame()
        for linup in raw_json["response"]:
            df_full = pd.json_normalize(linup)
            df_start = pd.json_normalize(linup["startXI"])
            df_subs = pd.json_normalize(linup["substitutes"])
            df_start['team.id'] = linup["team"]["id"]
            df_subs['team.id'] = linup["team"]["id"]
            df_start_full = pd.merge(left=df_full, right=df_start, on="team.id", how='left')
            df_subs_full = pd.merge(left=df_full, right=df_subs, on="team.id", how='left')
            df_start_full['place'] = 'starting_xi'
            df_subs_full['place'] = 'substitute'
            df_delta = pd.concat([df_start_full, df_subs_full], ignore_index=True)
            df_delta['fixture_id'] = raw_json["parameters"]["fixture"]
            df = pd.concat([df, df_delta], ignore_index=True)
        return df

    def read_json_to_df_level_2_stats(self, raw_json):
        df = pd.DataFrame()
        for stat in raw_json["response"]:
            df_stats = pd.json_normalize(stat["statistics"])
            df_stats['key'] = 1
            df_info = pd.json_normalize(stat)
            df_info['key'] = 1
            df_delta = pd.merge(left=df_info, right=df_stats, on='key', how='left')
            df_delta['fixture_id'] = raw_json["parameters"]["fixture"]
            df = pd.concat([df, df_delta], ignore_index=True)
        return df

    def return_objects_level_2(self, df):
        # loop on master fixtures df
        querystring = {}
        objects = []
        cntr = 0
        for id in df[df.columns[0]]:
            querystring.update({'fixture':str(id)})
            dict_obj = requests.get(self.url, headers=self.headers, params=querystring)
            dict_obj = dict_obj.json()
            objects.append(dict_obj)
            cntr = cntr + 1
            if cntr > 200:
                time.sleep(60)
                cntr = 0
        return objects

    def return_objects(self):
        dict_obj = requests.get(self.url, headers=self.headers, params=self.querystring)
        dict_obj = dict_obj.json()
        qs_temp = self.querystring
        max_page = dict_obj["paging"]["total"]
        current_page = 1
        objects = []
        cntr = 0
        while current_page <= max_page:
            objects.append(dict_obj)
            current_page = current_page + 1
            if current_page <= max_page:
                qs_temp = self.querystring
                qs_temp.update({'page':str(current_page)})
                dict_obj = requests.get(self.url, headers=self.headers, params=qs_temp)
                dict_obj = dict_obj.json()
            cntr = cntr + 1
            if cntr > 200:
                time.sleep(60)
                cntr = 0
        return objects