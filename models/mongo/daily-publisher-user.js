const mongoose = require('mongoose');
const timestamps = require('mongoose-timestamp');

const collection = 'daily_publisher_users';

const publisherUsers = new mongoose.Schema({
    publisher_id: {
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

publisherUsers.plugin(timestamps);
const PublisherUser = mongoose.model(collection, publisherUsers);
module.exports.PublisherUser = PublisherUser