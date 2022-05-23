"use strict";

const { Sequelize } = require("sequelize");
const pg = require("pg");

const DIALECT = "postgres";

const {
  POSTGRES_DATABASE: database,
  POSTGRES_USER: user,
  POSTGRES_PASSWORD: password,
  POSTGRES_HOST: host,
  POSTGRES_PORT: port,
  debug
} = process.env;

const sequelize = new Sequelize({
  dialect: DIALECT,
  dialectModule: pg,
  username: user,
  password,
  // database,
  host,
  port,
  logging: function (str) {
    if (debug === "true") {
      console.log(str);
    }
  }
});

module.exports = {
  sequelize
};
