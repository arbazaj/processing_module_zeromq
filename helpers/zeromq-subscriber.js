var zmq = require('zeromq')
  , sock = zmq.socket('sub');

const DailyReportPublisherLevel = require("../models/pg/daily-report-publisher-level");
const DailyReportPublisherAppLevel = require("../models/pg/daily-report-publisher-app-level");
const { sequelize } = require("./pg-connection");
const { PublisherUser } = require("../models/mongo/daily-publisher-user");
const { PublisherAppUser } = require("../models/mongo/daily-publisher-app-user");
const mongoose = require('mongoose');

const { getDateFromEpochTimestamp } = require("./util");

const PUBLISHER_CONFIG = {
  username: process.env.SSH_USER_NAME,
  password: process.env.SSH_PASSWORD,
  host: process.env.TCP_IP_PUBLISHER,
  port: 22,
  dstHost: process.env.TCP_IP_PUBLISHER,
  dstPort: process.env.TCP_PORT_PUBLISHER,
  localHost: '127.0.0.1',
  localPort: process.env.TCP_PORT_LOCAL_PUBLISHER
};

const BIDDER_CONFIG = {
  username: process.env.SSH_USER_NAME,
  password: process.env.SSH_PASSWORD,
  host: process.env.TCP_IP_BIDDER,
  port: 22,
  dstHost: process.env.TCP_IP_BIDDER,
  dstPort: process.env.TCP_PORT_BIDDER,
  localHost: '127.0.0.1',
  localPort: process.env.TCP_PORT_LOCAL_BIDDER
};

const fieldMappingConfig = {
  bid_request_id: () => "total_bid_request",
  bid_response_id: () => "total_bid_response",
  event_name: (event) => {
    const eventFieldMapping = {
      start: "total_start_impression",
      "first quartile": "total_first_quartile_impression",
      "second quartile": "total_second_quartile_impression",
      "third quartile": "total_third_quartile_impression",
      complete: "total_complete_impression",
      mute: "total_mute",
      click: "total_click",
      pause: "total_pause",
      play: "total_play"
    };
    return eventFieldMapping[event.toLowerCase()];
  }
};

var tunnel = require('tunnel-ssh');
tunnel(PUBLISHER_CONFIG, function (error, server) {
  if (error) {
    console.log(error);
  }
  server.on('error', function (err) {
    console.log(err);
    console.log("========");
  });
  const ip = "127.0.0.1";
  const port = process.env.TCP_PORT_LOCAL_PUBLISHER;
  const address = `tcp://${ip}:${port}`;
  sock.connect(address);
  console.log("connected---------");
  sock.subscribe('impressions');
  console.log('Subscriber connected to', address);
  sock.on('message', async function (topic, message) {
    console.log('received a message related to:', topic.toString(), 'containing message:', message.toString());
    topic = topic.toString();
    if (topic === "impressions") {
      await onImpressions();
    }
  });
});

var tunnel = require('tunnel-ssh');
tunnel(BIDDER_CONFIG, function (error, server) {
  if (error) {
    console.log(error);
  }
  server.on('error', function (err) {
    console.log(err);
    console.log("========");
  });
  const ip = "127.0.0.1";
  const port = process.env.TCP_PORT_LOCAL_BIDDER;
  const address = `tcp://${ip}:${port}`;
  sock.connect(address);
  console.log("connected---------");
  sock.subscribe('impressions');
  console.log('Subscriber connected to', address);
  sock.on('message', async function (topic, message) {
    console.log('received a message related to:', topic.toString(), 'containing message:', message.toString());
    topic = topic.toString();
    const data = JSON.parse(message.toString());
    if (topic === "impressions") {
      await onImpressions(data);
    }
  });
});


const onImpressions = async (data) => {
  try {
    if (data.publisher_id) {
      await handleDailyReportPublisherLevel(data);
      if (data.app_id) {
        await handleDailyReportPublisherAppLevel(data);
      }
      if (data.bid_request_id) {
        const update = {
          bid_request_id: data.bid_request_id
        };
        await mongoose.connection.db.collection("bid_requests").updateOne(update, {
          $set: update
        }, {
          upsert: true
        });
      }
      if (data.bid_response_id) {
        const update = {
          bid_response_id: data.bid_response_id
        };
        await mongoose.connection.db.collection("bid_response").updateOne(update, {
          $set: update
        }, {
          upsert: true
        });
      }
    }
  } catch (e) {
    console.log(e);
  }
}

const handleDailyReportPublisherLevel = async (data) => {
  const date = getDateFromEpochTimestamp(data.timestamp, "DD-MM-YYYY");
  const where = { publisher_id: data.publisher_id, date };
  let existingReport = await DailyReportPublisherLevel.findOne({
    where,
    attributes: ['publisher_id', 'date']
  });
  if (!existingReport) {
    existingReport = await DailyReportPublisherLevel.create({ publisher_id: data.publisher_id, date });
  }
  const incrementFields = [];
  Object.keys(data).forEach(key => {
    if (typeof fieldMappingConfig[key] === "function") {
      const field = fieldMappingConfig[key](data[key]);
      if (field) {
        incrementFields.push(field);
      }
    }
  });
  await sequelize.transaction(async t => {
    const updateData = {}
    if (data.currency) {
      updateData.currency = data.currency;
    }
    const promises = [];
    if (data.price) {
      promises.push(existingReport.increment("total_revenue_impressions",
        {
          by: (+data.price / 1000), where, transaction: t
        }
      ));
    }
    if (data.user_id) {
      const publisherUserFilter = {
        publisher_id: data.publisher_id,
        date
      };
      const resp = await PublisherUser.updateOne(publisherUserFilter, {
        $set: publisherUserFilter,
        $addToSet: {
          users: data.user_id
        }
      }, {
        upsert: true
      });
      const upsertedId = resp.upsertedId;
      if (upsertedId || resp.modifiedCount > 0) {
        if (!upsertedId) {
          const pubUser = await PublisherUser.findOne(publisherUserFilter, {
            _id: 1
          });
          updateData.unique_users_list = `${pubUser._id}`;
        } else {
          updateData.unique_users_list = `${upsertedId}`;
        }
        incrementFields.push("unique_users_count");
      }
      // const existingUser = await PublisherUsers.findOne({
      //   where: {
      //     user_id: data.user_id,
      //     publisher_id: data.publisher_id
      //   }
      // }, {
      //   attributes: ["user_id"]
      // });
      // if (!existingUser) {
      //   incrementFields.push("unique_users_count");
      // }
      // if (typeof exists.unique_users_list === "string") {
      //     exists.unique_users_list = JSON.parse(exists.unique_users_list);
      // }
      // updateData.unique_users_list = Array.from(new Set([...exists.unique_users_list, data.user_id]));
    }
    // if (exists.unique_users_list.length < updateData.unique_users_list.length) {
    //     incrementFields.push("unique_users_count");
    // }
    if (incrementFields.length) {
      promises.push(existingReport.increment(incrementFields,
        {
          by: 1, where, transaction: t
        }
      ));
    }
    if (Object.keys(updateData).length) {
      promises.push(existingReport.update(updateData, {
        where, transaction: t
      }));
    }
    await Promise.all(promises);
  });
}

const handleDailyReportPublisherAppLevel = async (data) => {
  const date = getDateFromEpochTimestamp(data.timestamp, "DD-MM-YYYY");
  const where = {
    publisher_id: data.publisher_id,
    app_id: data.app_id,
    date
  };
  let existingReport = await DailyReportPublisherAppLevel.findOne({
    where,
    attributes: ['publisher_id', 'app_id', 'date']
  });
  if (!existingReport) {
    existingReport = await DailyReportPublisherAppLevel.create({
      publisher_id: data.publisher_id,
      app_id: data.app_id,
      date
    });
  }
  const incrementFields = [];
  Object.keys(data).forEach(key => {
    if (typeof fieldMappingConfig[key] === "function") {
      const field = fieldMappingConfig[key](data[key]);
      if (field) {
        incrementFields.push(field);
      }
    }
  });
  await sequelize.transaction(async t => {
    const updateData = {};
    if (data.currency) {
      updateData.currency = data.currency;
    }
    const promises = [];
    if (data.price) {
      promises.push(existingReport.increment("total_revenue_impressions",
        {
          by: (+data.price / 1000), where, transaction: t, returning: false
        }
      ));
    }
    if (data.user_id) {
      const publisherAppUserFilter = {
        publisher_id: data.publisher_id,
        app_id: data.app_id,
        date
      };
      const resp = await PublisherAppUser.updateOne(publisherAppUserFilter, {
        $set: publisherAppUserFilter,
        $addToSet: {
          users: data.user_id
        }
      }, {
        upsert: true
      });
      const upsertedId = resp.upsertedId;
      if (upsertedId || resp.modifiedCount > 0) {
        if (!upsertedId) {
          const pubUser = await PublisherAppUser.findOne(publisherAppUserFilter, {
            _id: 1
          });
          updateData.unique_users_list = `${pubUser._id}`;
        } else {
          updateData.unique_users_list = `${upsertedId}`;
        }
        incrementFields.push("unique_users_count");
      }
    }
    if (incrementFields.length) {
      promises.push(existingReport.increment(incrementFields,
        {
          by: 1, where, transaction: t, returning: false
        }
      ));
    }
    promises.push(existingReport.update(updateData, {
      where, transaction: t, returning: false
    }));
    await Promise.all(promises);
  });
}
