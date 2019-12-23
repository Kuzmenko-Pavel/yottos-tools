# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import (Column, Float, Integer, text)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.hybrid import hybrid_property


class AggrArrayImpClickCtr(object):

    # Array
    array_impressions_block = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                                     server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_impressions_block_valid = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                                           server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_impressions_block_paid = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                                          server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_impressions_block_social = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                                            server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_impressions_offer = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                     server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_impressions_offer_valid = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                           server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_impressions_offer_paid = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                          server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_impressions_offer_social = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                            server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                          server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_suspicious = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                     server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_filtered = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                   server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_banned = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                 server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_valid = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_paid = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                               server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_social = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                                 server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_time = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                               server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    array_clicks_cost = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                               server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_impressions_cost = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                                    server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_clicks_remainder_cost = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                                         server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_impressions_remainder_cost = Column(ARRAY(Float(precision=4)), default=[0.0 for x in range(0, 94)],
                                              server_default='{%s}' % (','.join(['0.0' for x in range(0, 94)])))

    array_goal = Column(ARRAY(Integer), default=[0 for x in range(0, 94)],
                        server_default='{%s}' % (','.join(['0' for x in range(0, 94)])))

    # last 7 day
    impressions_block_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_valid_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_paid_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_social_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_offer_7 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_valid_7 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_paid_7 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_social_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_suspicious_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_filtered_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_banned_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_valid_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_paid_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_social_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_time_7 = Column(Integer, server_default=text("0"), default=0)

    clicks_cost_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_cost_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    clicks_remainder_cost_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_remainder_cost_7 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    goal_7 = Column(Integer, server_default=text("0"), default=0)

    # last 30 day
    impressions_block_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_valid_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_paid_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_social_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_offer_30 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_valid_30 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_paid_30 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_social_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_suspicious_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_filtered_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_banned_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_valid_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_paid_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_social_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_time_30 = Column(Integer, server_default=text("0"), default=0)

    clicks_cost_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_cost_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    clicks_remainder_cost_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_remainder_cost_30 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    goal_30 = Column(Integer, server_default=text("0"), default=0)

    # last 90 day
    impressions_block_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_valid_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_paid_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_social_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_offer_90 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_valid_90 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_paid_90 = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_social_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_suspicious_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_filtered_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_banned_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_valid_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_paid_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_social_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_time_90 = Column(Integer, server_default=text("0"), default=0)

    clicks_cost_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_cost_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    clicks_remainder_cost_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_remainder_cost_90 = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    goal_90 = Column(Integer, server_default=text("0"), default=0)

    # current month
    impressions_block_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_valid_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_paid_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_social_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_offer_current_month = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_valid_current_month = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_paid_current_month = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_social_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_suspicious_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_filtered_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_banned_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_valid_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_paid_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_social_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_time_current_month = Column(Integer, server_default=text("0"), default=0)

    clicks_cost_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_cost_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    clicks_remainder_cost_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_remainder_cost_current_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    goal_current_month = Column(Integer, server_default=text("0"), default=0)

    # last month
    impressions_block_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_valid_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_paid_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_block_social_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_offer_last_month = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_valid_last_month = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_paid_last_month = Column(Integer, server_default=text("0"), default=0)

    impressions_offer_social_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_suspicious_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_filtered_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_banned_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_valid_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_paid_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_social_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_time_last_month = Column(Integer, server_default=text("0"), default=0)

    clicks_cost_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_cost_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    clicks_remainder_cost_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    impressions_remainder_cost_last_month = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    goal_last_month = Column(Integer, server_default=text("0"), default=0)

    @hybrid_property
    def payments_7(self):
        return self.clicks_cost_7 + self.impressions_cost_7

    @hybrid_property
    def payments_30(self):
        return self.clicks_cost_30 + self.impressions_cost_30

    @hybrid_property
    def payments_90(self):
        return self.clicks_cost_90 + self.impressions_cost_90
