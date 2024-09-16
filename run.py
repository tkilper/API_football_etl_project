"""Running the Football API ETL Application"""
import logging
import logging.config
import time
import sys
import threading
import argparse

import yaml

from footballapi.common.footballapi import FootballAPIConnector_Players, FootballAPIConnector_Teams, \
                                            FootballAPIConnector_Venues, FootballAPIConnector_Fixtures
from footballapi.common.postgres import PostgresConnector
from footballapi.transformers.footballapi_transformer import FootballAPIETL_Players, FootballAPIETL_Teams, \
                                                            FootballAPIETL_Venues, FootballAPIETL_Fixtures
from footballapi.common.constants import Players, Teams, Venues, Fixtures

def main():
    """
    entry point to run the Football API ETL application
    """
    # Parsing YML file
    config_path = '/code/configs/footballapi_logging_config.yml'
    config = yaml.safe_load(open(config_path))
    """
    parser = argparse.ArgumentParser(description='Run the Xetra ETL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args(args)
    config = yaml.safe_load(open(args.config))
    """
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)

    # creating the FootballAPIConnector classes for source
    footballapi_src1 = FootballAPIConnector_Players(Players.url.value, Players.querystring.value, Players.headers.value)
    footballapi_src2 = FootballAPIConnector_Teams(Teams.url.value, Teams.querystring.value, Teams.headers.value)
    footballapi_src3 = FootballAPIConnector_Venues(Venues.url.value, Venues.querystring.value, Venues.headers.value)
    footballapi_src4_a = FootballAPIConnector_Fixtures(Fixtures.url.value, Fixtures.querystring.value, Fixtures.headers.value)
    footballapi_src4_b = FootballAPIConnector_Fixtures(Fixtures.url_events.value, Fixtures.querystring.value, Fixtures.headers.value)
    footballapi_src4_c = FootballAPIConnector_Fixtures(Fixtures.url_lineups.value, Fixtures.querystring.value, Fixtures.headers.value)
    footballapi_src4_d = FootballAPIConnector_Fixtures(Fixtures.url_stats.value, Fixtures.querystring.value, Fixtures.headers.value)

    # creating the PostgresConnector classes for target
    postgres_trg1 = PostgresConnector(Players.conn_string.value, Players.table_name.value, Players.schema_name.value)
    postgres_trg2 = PostgresConnector(Teams.conn_string.value, Teams.table_name.value, Teams.schema_name.value)
    postgres_trg3 = PostgresConnector(Venues.conn_string.value, Venues.table_name.value, Venues.schema_name.value)
    postgres_trg4_a = PostgresConnector(Fixtures.conn_string.value, Fixtures.table_name_1.value, Fixtures.schema_name.value)
    postgres_trg4_b = PostgresConnector(Fixtures.conn_string.value, Fixtures.table_name_2.value, Fixtures.schema_name.value)
    postgres_trg4_c = PostgresConnector(Fixtures.conn_string.value, Fixtures.table_name_3.value, Fixtures.schema_name.value)
    postgres_trg4_d = PostgresConnector(Fixtures.conn_string.value, Fixtures.table_name_4.value, Fixtures.schema_name.value)

    # creating FootballAPI ETL classes
    logger = logging.getLogger(__name__)
    logger.info('FootballAPI ETL job started.')
    football_apiETL_players = FootballAPIETL_Players(footballapi_src1, postgres_trg1)
    football_apiETL_teams = FootballAPIETL_Teams(footballapi_src2, postgres_trg2)
    football_apiETL_venues = FootballAPIETL_Venues(footballapi_src3, postgres_trg3)
    football_apiETL_fixtures_a = FootballAPIETL_Fixtures(footballapi_src4_a, postgres_trg4_a)
    football_apiETL_fixtures_b = FootballAPIETL_Fixtures(footballapi_src4_b, postgres_trg4_b)
    football_apiETL_fixtures_c = FootballAPIETL_Fixtures(footballapi_src4_c, postgres_trg4_c)
    football_apiETL_fixtures_d = FootballAPIETL_Fixtures(footballapi_src4_d, postgres_trg4_d)

    # running etl jobs
    logger.info('PLAYERS table job started...')
    football_apiETL_players.etl(footballapi_src1)
    logger.info('PLAYERS table job finished.')
    logger.info('TEAMS table job started...')
    football_apiETL_teams.etl(footballapi_src2)
    logger.info('TEAMS table job finished.')
    logger.info('VENUES table job started...')
    football_apiETL_venues.etl(footballapi_src3)
    logger.info('VENUES table job finished.')
    logger.info('FIXTURES table job started...')
    fixtures_master_df = football_apiETL_fixtures_a.etl_fixtures_1()
    football_apiETL_fixtures_b.etl_fixture_events_2(fixtures_master_df)
    football_apiETL_fixtures_c.etl_fixture_lineups_3(fixtures_master_df)
    football_apiETL_fixtures_d.etl_fixture_stats_4(fixtures_master_df)
    logger.info('FIXTURES tables job finished.') # order is fixture events -> lineups -> stats

if __name__ == '__main__':
    main()