"""
sage_data_client - Official Sage Python data API client.
========================================================

sage_data_client goals are to make writing queries and working with the results easy. It does this by:

* Providing a simple query function which talks to the data API.
* Providing the results in an easy to use [Pandas](https://pandas.pydata.org) data frame.
"""
from .query import query, load
