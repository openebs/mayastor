// Common logger instance which is configured once and can be included in
// all files where logging is needed.

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
function toLocalTime (isoTs: string) {
  const dt = new Date(Date.parse(isoTs));
  const pad = function (num: number) {
    return (num < 10 ? '0' : '') + num;
  };
  const pad2 = function (num: number) {
    if (num < 10) {
      return '00' + num;
    } else if (num < 100) {
      return '0' + num;
    } else {
      return num;
    }
  };
  return (
    monthShortNames[dt.getMonth()] +
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

type PrintfArg = {
  level: string;
  message: string;
  label: string;
  timestamp: string;
};

const myFormat = winston.format.printf((arg: PrintfArg) => {
  const result = [toLocalTime(arg.timestamp)];

  // silly -> trace
  if (arg.level.match(/silly/)) {
    arg.level = arg.level.replace(/silly/, 'trace');
  }
  result.push(arg.level);

  if (arg.label) {
    result.push('[' + arg.label + ']:');
  } else {
    result[result.length - 1] += ':';
  }
  result.push(arg.message);
  return result.join(' ');
});

const formats = [winston.format.timestamp(), myFormat];
if (process.stdout.isTTY) {
  formats.unshift(winston.format.colorize());
}
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(...formats),
  transports: [new winston.transports.Console()]
});

export function setLevel (level: string) {
  logger.level = level;
}

// Purpose of the wrapper is to add component prefix to each log message
export function Logger (component?: string): any {
  const obj = Object.create(Logger.prototype);
  obj.component = component;
  obj.logger = logger;
  return obj;
}

const levels = ['debug', 'info', 'warn', 'error'];
levels.forEach((lvl) => {
  Logger.prototype[lvl] = function (msg: string) {
    logger[lvl].call(this.logger, {
      label: this.component,
      message: msg
    });
  };
});
// rename trace to silly
Logger.prototype.trace = function (msg: string) {
  logger.silly.call(this.logger, {
    component: this.component,
    message: msg
  });
};