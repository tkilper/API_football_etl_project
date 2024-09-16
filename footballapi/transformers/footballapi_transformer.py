"""FootballAPI ETL class and methods"""
from footballapi.common.footballapi import FootballAPIConnector_Players, FootballAPIConnector_Teams, \
FootballAPIConnector_Venues, FootballAPIConnector_Fixtures
from footballapi.common.postgres import PostgresConnector
import pandas as pd
from datetime import datetime
import logging

class FootballAPIETL_Players():
    """
    Reads the FootballAPI data, transforms and writes the transformed to target in postgres - Players
    """

    def __init__(self, footballapi_src: FootballAPIConnector_Players,
                 postgres_trg: PostgresConnector,):
        """
        Constructor for FootballAPITransformer - Players

        :param footballapi_src: connection to source S3 bucket
        :param postgres_trg: connection to target postgres data warehouse
        """
        self.logger = logging.getLogger(__name__)
        self.footballapi_src = footballapi_src
        self.postgres_trg = postgres_trg
    
    def extract(self, objects):
        df = pd.concat([self.footballapi_src.read_json_to_df(raw_json) for raw_json in objects], ignore_index=True)
        return df

    def transform(self, df):
        df = df.drop(columns=['team.name','team.logo'])
        df = df.rename(columns={'id':'player_id'})
        ct = datetime.now()
        df['load_timestamp'] = ct
        return df

    def load(self, df):
        self.postgres_trg.write_df_to_postgres(df)
        return True

    def etl(self, apiconnector):
        objects = apiconnector.return_objects()
        df = self.extract(objects)
        df = self.transform(df)
        self.load(df)
        return True

class FootballAPIETL_Teams():
    """
    Reads the FootballAPI data, transforms and writes the transformed to target in postgres - Teams
    """

    def __init__(self, footballapi_src: FootballAPIConnector_Teams,
                 postgres_trg: PostgresConnector,):
        """
        Constructor for FootballAPITransformer - Teams

        :param footballapi_src: connection to source S3 bucket
        :param postgres_trg: connection to target postgres data warehouse
        """
        self.logger = logging.getLogger(__name__)
        self.footballapi_src = footballapi_src
        self.postgres_trg = postgres_trg
    
    def extract(self, objects):
        df = pd.concat([self.footballapi_src.read_json_to_df(raw_json) for raw_json in objects], ignore_index=True)
        return df

    def transform(self, df):
        df = df.drop(columns=['name_y', 'address', 'city', 'capacity', 'surface', 'image',
        'id_team'])
        df = df.rename(columns={'id_x':'team_id','name_x':'name','id_y':'venue_id'})
        ct = datetime.now()
        df['load_timestamp'] = ct
        return df

    def load(self, df):
        self.postgres_trg.write_df_to_postgres(df)
        return True

    def etl(self, apiconnector):
        objects = apiconnector.return_objects()
        df = self.extract(objects)
        df = self.transform(df)
        self.load(df)
        return True

class FootballAPIETL_Venues():
    """
    Reads the FootballAPI data, transforms and writes the transformed to target in postgres - Venues
    """

    def __init__(self, footballapi_src: FootballAPIConnector_Venues,
                 postgres_trg: PostgresConnector,):
        """
        Constructor for FootballAPITransformer - Venues

        :param footballapi_src: connection to source S3 bucket
        :param postgres_trg: connection to target postgres data warehouse
        """
        self.logger = logging.getLogger(__name__)
        self.footballapi_src = footballapi_src
        self.postgres_trg = postgres_trg
    
    def extract(self, objects):
        df = pd.concat([self.footballapi_src.read_json_to_df(raw_json) for raw_json in objects], ignore_index=True)
        return df

    def transform(self, df):
        df = df.rename(columns={'id':'venue_id'})
        ct = datetime.now()
        df['load_timestamp'] = ct
        return df

    def load(self, df):
        self.postgres_trg.write_df_to_postgres(df)
        return True

    def etl(self, apiconnector):
        objects = apiconnector.return_objects()
        df = self.extract(objects)
        df = self.transform(df)
        self.load(df)
        return True

class FootballAPIETL_Fixtures():
    """
    Reads the FootballAPI data, transforms and writes the transformed to target in postgres - Fixtures
    """

    def __init__(self, footballapi_src: FootballAPIConnector_Fixtures,
                 postgres_trg: PostgresConnector,):
        """
        Constructor for FootballAPITransformer - Fixtures

        :param footballapi_src: connection to source S3 bucket
        :param postgres_trg: connection to target postgres data warehouse
        """
        self.logger = logging.getLogger(__name__)
        self.footballapi_src = footballapi_src
        self.postgres_trg = postgres_trg
    
    def extract_1(self, objects):
        df = pd.concat([self.footballapi_src.read_json_to_df(raw_json) for raw_json in objects], ignore_index=True)
        return df

    def extract_events_2(self, objects):
        df = pd.concat([self.footballapi_src.read_json_to_df_level_2_events(raw_json) for raw_json in objects], ignore_index=True)
        return df

    def extract_lineups_3(self, objects):
        df = pd.concat([self.footballapi_src.read_json_to_df_level_2_lineups(raw_json) for raw_json in objects], ignore_index=True)
        return df

    def extract_stats_4(self, objects):
        df = pd.concat([self.footballapi_src.read_json_to_df_level_2_stats(raw_json) for raw_json in objects], ignore_index=True)
        return df

    def transform_fixture_1(self, df):
        df = df.drop(columns=['teams.away.logo', 'teams.home.logo', 'fixture.venue.city'], axis=1)
        df = df.rename(columns={'fixture.id':'fixture_id','teams.home.id':'home_team_id','fixture.venue.id':'venue_id','teams.away.id':'away_team_id'})
        ct = datetime.now()
        df["fixture.date"] = pd.to_datetime(df["fixture.date"])
        df['load_timestamp'] = ct
        return df

    def transform_fixture_events_2(self, df):
        df = df.drop(columns=['team.logo'], axis=1)
        df['time.extra'] = df['time.extra'].fillna(0)
        ct = datetime.now()
        df['load_timestamp'] = ct
        return df

    def transform_fixture_lineups_3(self, df):
        df = df.drop(columns=['startXI','substitutes','team.logo']).reindex()
        ct = datetime.now()
        df['load_timestamp'] = ct
        return df

    def transform_fixture_stats_4(self, df):
        df = df.drop(columns=['statistics','team.logo','key'], axis=1)
        return df

    def load(self, df):
        self.postgres_trg.write_df_to_postgres(df)
        return True

    def etl_fixtures_1(self):
        objects = self.footballapi_src.return_objects()
        df = self.extract_1(objects)
        df = self.transform_fixture_1(df)
        self.load(df)
        return df

    def etl_fixture_events_2(self, fixtures_master_df):
        objects = self.footballapi_src.return_objects_level_2(fixtures_master_df)
        df = self.extract_events_2(objects)
        df = self.transform_fixture_events_2(df)
        self.load(df)
        return True

    def etl_fixture_lineups_3(self, fixtures_master_df):
        objects = self.footballapi_src.return_objects_level_2(fixtures_master_df)
        df = self.extract_lineups_3(objects)
        df = self.transform_fixture_lineups_3(df)
        self.load(df)
        return True

    def etl_fixture_stats_4(self, fixtures_master_df):
        objects = self.footballapi_src.return_objects_level_2(fixtures_master_df)
        df = self.extract_stats_4(objects)
        df = self.transform_fixture_stats_4(df)
        self.load(df)
        return True