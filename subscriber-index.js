"use strict";

(() => {
    require("dotenv").config({ path: `./.${process.env.NODE_ENV || "local"}.env` });
})();

const { sequelize } = require("./helpers/pg-connection");
const DailyReportPublisherLevel = require("./models/pg/daily-report-publisher-level");
const DailyReportPublisherAppLevel = require("./models/pg/daily-report-publisher-app-level");

const db = require('./helpers/mongoose');

db.connect().then(() => {
    console.log("mongo connected......");
}).catch(e => {
    console.log(e);
    console.log("error in connection of db");
});

sequelize.sync({ force: false }).then(async () => {
    await DailyReportPublisherLevel.sync({ force: false });
    await DailyReportPublisherAppLevel.sync({ force: false });
    require("./helpers/zeromq-subscriber");
}).catch(e => {
    console.log(e);
});
