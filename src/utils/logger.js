const winston = require('winston');
const path = require('path');
const fs = require('fs');

// LOG_FILE=false: console only (containers, where stdout is collected by the runtime)
const logToFile = process.env.LOG_FILE !== 'false';
// Colors only on an interactive terminal unless LOG_COLOR says otherwise
const colorize = process.env.LOG_COLOR ? process.env.LOG_COLOR === 'true' : !!process.stdout.isTTY;
const logsDir = process.env.LOG_DIR || path.join(process.cwd(), 'logs');

const logFormat = winston.format.combine(
  winston.format.timestamp({
    format: 'YYYY-MM-DD HH:mm:ss'
  }),
  winston.format.errors({ stack: true }),
  winston.format.printf(({ timestamp, level, message, stack, ...meta }) => {
    let log = `${timestamp} [${level.toUpperCase()}]: ${message}`;
    
    if (Object.keys(meta).length > 0) {
      log += ` ${JSON.stringify(meta)}`;
    }
    
    if (stack) {
      log += `\n${stack}`;
    }
    
    return log;
  })
);

const transports = [
  new winston.transports.Console({
    format: colorize ? winston.format.combine(winston.format.colorize(), logFormat) : logFormat
  })
];

if (logToFile) {
  try {
    fs.mkdirSync(logsDir, { recursive: true });
    transports.push(
      // All logs
      new winston.transports.File({
        filename: path.join(logsDir, 'connector.log'),
        maxsize: 10 * 1024 * 1024, // 10MB
        maxFiles: 5
      }),
      // Errors only
      new winston.transports.File({
        filename: path.join(logsDir, 'error.log'),
        level: 'error',
        maxsize: 10 * 1024 * 1024, // 10MB
        maxFiles: 5
      })
    );
  } catch (error) {
    // Read-only filesystem or no permission: keep logging to the console
    console.error(`Log files disabled (${logsDir}: ${error.message})`);
  }
}

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: logFormat,
  transports
});

// Add metadata helper
logger.withMeta = (meta) => {
  return {
    info: (message) => logger.info(message, meta),
    error: (message) => logger.error(message, meta),
    warn: (message) => logger.warn(message, meta),
    debug: (message) => logger.debug(message, meta)
  };
};

module.exports = logger;
