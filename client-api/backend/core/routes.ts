import type { Request, Response } from 'express';
import type { Routes } from '../types/index.js';

import {
  health,
  commitTx,
  queryTx,
} from '../middlewares/controllers.js';
// import {
//   initializeValidator,
//   reporter,
//   mintTokenValidator,
//   burnTokenValidator,
//   getBalanceValidator,
//   getOwnerValidator,
//   getTokenValidator,
//   uploadDataValidator
// } from '../middlewares/validators.js';
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
  },
  // '/burn': {
  //   method: 'post',
  //   controller: burnToken,
  //   validator: [burnTokenValidator, reporter],
  //   swagger: burnTokenDoc
  // },
  // '/balance/:owner': {
  //   method: 'get',
  //   controller: getBalanceOf,
  //   validator: [getBalanceValidator, reporter],
  //   swagger: getBalanceDoc
  // },
  // '/owner/:token_id': {
  //   method: 'get',
  //   controller: getOwnerOf,
  //   validator: [getOwnerValidator, reporter],
  //   swagger: getOwnerDoc
  // },
  // '/token/:token_id': {
  //   method: 'get',
  //   controller: getTokenURI,
  //   validator: [getTokenValidator, reporter],
  //   swagger: getTokenURIDoc
  // },
  // '/total_supply': {
  //   method: 'get',
  //   controller: getTotalSupply,
  //   swagger: getTotalSupplyDoc
  // },
  // '/client_balance': {
  //   method: 'get',
  //   controller: getClientAccountBalance,
  //   swagger: getClientAccountBalanceDoc
  // },
  // '/client_account_id': {
  //   method: 'get',
  //   controller: getClientAccountID,
  //   swagger: getClientAccountIDDoc
  // },
  // '/symbol': {
  //   method: 'get',
  //   controller: getSymbol,
  //   swagger: getSymbolDoc
  // },
  // '/name': {
  //   method: 'get',
  //   controller: getName,
  //   swagger: getNameDoc
  // },
  // '/uploadData': {
  //   method: 'post',
  //   controller: uploadData,
  //   validator: [uploadDataValidator, reporter],
  //   swagger: uploadDataDoc
  // }
}

export default coreRoutes
