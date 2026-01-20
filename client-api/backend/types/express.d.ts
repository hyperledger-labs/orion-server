import type { ClientRecordShape } from '../helpers/utils.js';

declare global {
  namespace Express {
    interface Request {
      clientRecord: ClientRecordShape;
    }
  }
}

export {};
