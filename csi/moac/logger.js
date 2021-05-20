// Common logger instance which is configured once and can be included in
// all files where logging is needed.

'use strict';

const winston = require('winston');

const monthShortNames = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec'
];

// This will convert ISO timestamp string to following format:
// Oct 10 19:49:29.027
function toLocalTime (isoTs) {
  const dt = new Date(Date.parse(isoTs));
  const pad = function (num) {
    return (num < 10 ? '0' : '') + num;
  };
  const pad2 = function (num) {
    if (num < 10) {
      return '00' + num;
    } else if (num < 100) {
      return '0' + num;
    } else {
      return num;
    }
  };
  return (
    pad(monthShortNames[dt.getMonth()]) +
    ' ' +
    pad(dt.getDate()) +
    ' ' +
    pad(dt.getHours()) +
    ':' +
    pad(dt.getMinutes()) +
    ':' +
    pad(dt.getSeconds()) +
    '.' +
    pad2(dt.getMilliseconds())
  );
}

const myFormat = winston.format.printf(
  ({ level, message, label, timestamp }) => {
    const result = [toLocalTime(timestamp)];

    // silly -> trace
    if (level.match(/silly/)) {
      level = level.replace(/silly/, 'trace');
    }
    result.push(level);

    if (label) {
      result.push('[' + label + ']:');
    } else {
      result[result.length - 1] += ':';
    }
    result.push(message);
    return result.join(' ');
  }
);

const formats = [winston.format.timestamp(), myFormat];
if (process.stdout.isTTY) {
  formats.unshift(winston.format.colorize());
}
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(...formats),
  transports: [new winston.transports.Console()]
});

function setLevel (level) {
  logger.level = level;
}

// Purpose of the wrapper is to add component prefix to each log message
function Logger (component) {
  const obj = Object.create(Logger.prototype);
  obj.component = component;
  obj.logger = logger;
  return obj;
}

const levels = ['debug', 'info', 'warn', 'error'];
levels.forEach((lvl) => {
  Logger.prototype[lvl] = function (msg) {
    logger[lvl].call(this.logger, {
      label: this.component,
      message: msg
    });
  };
});
// rename trace to silly
Logger.prototype.trace = function (msg) {
  logger.silly.call(this.logger, {
    component: this.component,
    message: msg
  });
};

module.exports = {
  Logger,
  setLevel
};
