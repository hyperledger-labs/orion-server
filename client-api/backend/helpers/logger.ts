import log4js from 'log4js';
import type { Logger } from 'log4js';
import { Record } from 'immutable';

// Configure log4js
log4js.configure({
  appenders: {
    out: { type: 'stdout' },
    // clientApp: { type: 'file', filename: 'logs/clientApp.log' }
  },
  categories: {
    default: { appenders: ['out'], level: 'debug' },
    // clientApp: { appenders: ['out', 'clientApp'], level: 'debug' }
  }
});

// Define the shape of the logger record
export interface LoggerRecordShape {
  inst: Logger | null;
  initLogger: (category: string) => any;
  trace: (msg: string) => void;
  debug: (msg: string) => void;
  info: (msg: string) => void;
  warn: (msg: string) => void;
  error: (msg: string | Error) => void;
  fatal: (msg: string | Error) => void;
}

// Create and export the Logger Record
const LoggerRecord = Record<LoggerRecordShape>({
  inst: null,
  initLogger(this: any, category: string) {
    return this.set('inst', log4js.getLogger(category));
  },
  trace(this: any, msg: string) {
    this.inst?.trace(msg);
  },
  debug(this: any, msg: string) {
    this.inst?.debug(msg);
  },
  info(this: any, msg: string) {
    this.inst?.info(msg);
  },
  warn(this: any, msg: string) {
    this.inst?.warn(msg);
  },
  error(this: any, msg: string | Error) {
    this.inst?.error(msg);
  },
  fatal(this: any, msg: string | Error) {
    this.inst?.fatal(msg);
  }
});

export default LoggerRecord;
