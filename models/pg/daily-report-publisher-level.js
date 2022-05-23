"use strict";

const { DataTypes, Model } = require("sequelize");
const { sequelize } = require("../../helpers/pg-connection");

class DailyReportPublisherLevel extends Model { }

DailyReportPublisherLevel.init({
  publisher_id: {
    type: DataTypes.STRING(255),
    primaryKey: true
  },
  date: {
    type: DataTypes.STRING(11),
    primaryKey: true
  },
  unique_users_count: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  unique_users_list: {
    type: DataTypes.STRING(50)
  },
  total_bid_request: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_bid_response: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_start_impression: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_first_quartile_impression: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_second_quartile_impression: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_third_quartile_impression: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_complete_impression: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_click: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_mute: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_pause: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_play: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_revenue_impressions: {
    type: DataTypes.DECIMAL,
    defaultValue: 0
  },
  total_revenue_clicks: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  total_revenue_installs: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  currency: {
    type: DataTypes.STRING(10)
  }
}, {
  sequelize,
  tableName: "daily_rep_publisher_level",
  timestamps: true,
  createdAt: "created_at",
  updatedAt: "updated_at"
});

module.exports = DailyReportPublisherLevel;