import type { Routes } from '../types/index.js';
import {
  health,
  commitTx,
  queryTx,
} from '../middlewares/controllers.js';
import {
  healthDoc
} from '../middlewares/swaggerDocs.js';

const coreRoutes: Routes = {
  '/health': {
    method: 'get',
    controller: health,
    // validator: [],
    swagger: healthDoc
  },
  '/commit_tx': {
    method: 'post',
    controller: commitTx,
    // validator: [],
    // swagger: commitTxDoc
  },
  '/query_tx/:dbName': {
    method: 'get',
    controller: queryTx,
    // validator: [],
    // swagger: queryTxDoc
  }
}

export default coreRoutes
