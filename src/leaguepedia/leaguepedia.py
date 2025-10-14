import os
from datetime import timedelta
from time import sleep

from mwrogue.auth_credentials import AuthCredentials
from mwrogue.esports_client import EsportsClient


class LeaguepediaSite:
    """A ghost loaded class that handles Leaguepedia connection and some caching.

    Full documentation: https://lol.fandom.com/Help:API_Documentation
    """

    def __init__(self, limit=500, delay_between: timedelta = timedelta(milliseconds=250)):
        self._site = None
        self.limit = limit
        self.delay_between = delay_between

    @property
    def site(self):
        if not self._site:
            self._load_site()

        return self._site

    def _load_site(self):
        """Creates site class fields.

        Used for ghost loading the class during package import.
        """
        # If not, we create the self.client object as our way to interact with the wiki
        leaguepedia_username = os.environ['LEAGUEPEDIA_USERNAME']
        leaguepedia_password = os.environ['LEAGUEPEDIA_PASSWORD']

        self._site = EsportsClient("lol", credentials=AuthCredentials(username=leaguepedia_username, password=leaguepedia_password))

    def query(self, **kwargs) -> list:
        """Issues a cargo query to leaguepedia.

        Params are usually:
            tables, join_on, fields, order_by, where

        Returns:
            List of rows from the query.
        """
        result = []

        # We check if we hit the API limit
        while len(result) % self.limit == 0:
            result.extend(
                self.site.cargo_client.query(
                    limit=self.limit, offset=len(result), **kwargs
                )
            )

            # If the cargoquery is empty, we stop the loop
            if not result:
                break
            sleep(self.delay_between.total_seconds())

        return result


# Ghost loaded instance shared by all other classes
leaguepedia = LeaguepediaSite()
