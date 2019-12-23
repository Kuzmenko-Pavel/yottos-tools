# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

import socket
from os import environ, getpid

from sqlalchemy import engine_from_config

from .ParentAccountsExchangeRates import ParentAccountExchangeRate
from .ParentAccountsStatistic import ParentAccountStatistic
from .ParentAccountsStatisticArray import ParentAccountStatisticArray
from .ParentCampaignsStatistic import ParentCampaignStatistic
from .ParentCampaignsStatisticArray import ParentCampaignStatisticArray
from .ParentSite import ParentSite
from .ParentSiteStatistic import ParentSiteStatistic
from .ParentSiteStatisticArray import ParentSiteStatisticArray
from .ParentBlock import ParentBlock
from .ParentBlockStatistic import ParentBlockStatistic
from .ParentBlockStatisticArray import ParentBlockStatisticArray
from .ParentAccounts import ParentAccount
from .ParentCampaigns import ParentCampaign
from .meta import metadata, DBScopedSession

server_name = socket.gethostname()


def get_engine(settings, prefix='main.sqlalchemy.', **kwargs):
    if 'connect_args' not in kwargs.keys():
        application_name = 'ReportGenerator %s on %s pid=%s' % (environ.get('project', ''), server_name, getpid())
        kwargs['connect_args'] = {"application_name": application_name}
    return engine_from_config(settings, prefix, echo=False, **kwargs)
