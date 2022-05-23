"use strict";

const crypto = require('crypto');
const ALGORITHM = 'aes256';
const BUFFER_BIT = 32;
const HEX = "hex";

module.exports = {
  getDateFromEpochTimestamp: (ts, format) => {
    const d = new Date(ts);
    function toLocaleString(value) {
      return value.toLocaleString('en-US', {
        minimumIntegerDigits: 2,
        useGrouping: false
      });
    }
    const date = toLocaleString(d.getDate());

    const monthNames = [
      "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ];
    format = format.replace(/DD/g, date);
    format = format.replace(/MM/g, monthNames[d.getMonth()]);
    format = format.replace(/YYYY/g, d.getFullYear());
    return format;
  },
  decrypt: (iv, encryptedText, password) => {
    const secret = password;
    const KEY = Buffer.alloc(BUFFER_BIT, secret, "base64");
    iv = Buffer.from(iv, HEX);
    encryptedText = Buffer.from(encryptedText, HEX);
    const decipher = crypto.createDecipheriv(ALGORITHM, Buffer.from(KEY), iv);
    let decrypted = decipher.update(encryptedText);
    decrypted = Buffer.concat([decrypted, decipher.final()]);
    return decrypted.toString();
  }
};
