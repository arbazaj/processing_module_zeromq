/* eslint-disable linebreak-style */
const mongoose = require('mongoose');
const MONGO_URI = 'mongodb://localhost:27017/zeromq';
PORT = 4000;


// Fixing deprication warnings
const options = {
  useNewUrlParser: true,
  useUnifiedTopology: true,
};

function connect() {
  return mongoose.connect(MONGO_URI, options)
      .then((mongodb) => {
        console.log('Connected to mongodb');
        return mongodb;
      })
      .catch((err) => {
        console.log(err);
        console.log('Couldnt connect to mongodb:', err.message);
        process.exit(1);
      });
}

function close() {
  return mongoose.connection.close();
}

module.exports = {
  connect,
  close,
};
