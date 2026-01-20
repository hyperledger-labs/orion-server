import { Record } from 'immutable';

// Define the shape of the RAG record
export interface ClientRecordShape {
  orionUrls: { [key: string]: string };
  logger: any;
  getLogger: () => any;
}

// Create and export the Record with default values and methods
const ClientRecord = Record<ClientRecordShape>({
  orionUrls: {},
  logger: null,
  getLogger(this: any) {
    return this.logger;
  }
});

export default ClientRecord;
