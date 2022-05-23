const mongoose = require('mongoose');
const timestamps = require('mongoose-timestamp');

const collection = 'daily_publisher_app_users';

const publisherAppUsers = new mongoose.Schema({
    publisher_id: {
        type: String,
        required: [true]
    },
    app_id: {
        type: String,
        required: [true]
    },
    date: {
        type: String,
        required: [true]
    },
    users: [{
        type: String
    }]
}, {
    collection
});

publisherAppUsers.plugin(timestamps);
const PublisherAppUser = mongoose.model(collection, publisherAppUsers);
module.exports.PublisherAppUser = PublisherAppUser;