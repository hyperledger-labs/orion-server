import type { Request, Response } from 'express';
import customFetch from '../helpers/api.js';
import { createResponse, createErrorResponse } from '../helpers/responses.js';
import type { RequestHandler } from '../types/index.js';

export const health: RequestHandler = async (req: Request, res: Response) => {
  try {
    // Access clientRecord directly from request - fully type-safe!
    req.clientRecord.logger.info('Health check endpoint called');

    createResponse(res, 'OK', {
      status: 'ok',
      timestamp: new Date().toISOString()
    });
  }
  catch (error) {
    const message = error instanceof Error ? error.message : 'An unknown error occurred';
    createErrorResponse(res, 500, message);
  }
}

export const commitTx: RequestHandler = async (req: Request, res: Response) => {
  try {
    req.clientRecord.logger.info('Commit transaction endpoint called');

    const { payload, signature } = req.body;
    const orionUrls = req.clientRecord.orionUrls;
    let response;
    for (const [index, url] of orionUrls.entries()) {
      console.log(`Attempt ${index + 1} of ${orionUrls.length}: ${url}`)

      try {
        response = await customFetch(
          `${url}/db/tx`,
          'POST',
          {
            body: { payload, signature },
            headers: {
              'Content-Type': 'application/json',
              'TxTimeout': '20s'
            }
          }
        );

        break;
      }
      catch (error: any) {
        if (index === orionUrls.length - 1) {
          throw error;
        }
        continue;
      }
    }

    createResponse(res, 'Transaction committed successfully', response);
  }
  catch (error: any) {
    const message = error instanceof Error ? error.message : 'An unknown error occurred';
    req.clientRecord.logger.error(`${error}`);

    createErrorResponse(res, 500, message);
  }

}

export const queryTx: RequestHandler = async (req: Request, res: Response) => {
  try {
    req.clientRecord.logger.info('Query transaction endpoint called');

    const { dbName } = req.params;
    const userId = req.headers['userid'] as string;
    const signature = req.headers['signature'] as string;
    const orionUrls = req.clientRecord.orionUrls;
    let response;

    for (const [index, url] of orionUrls.entries()) {
      console.log(`Attempt ${index + 1} of ${orionUrls.length}: ${url}`)

      try {
        response = await customFetch(
          `${url}/db/${dbName}`,
          'GET',
          {
            headers: {
              'Content-Type': 'application/json',
              'UserID': userId,
              'Signature': signature
            }
          }
        );
        break;
      }
      catch (error: any) {
        if (index === orionUrls.length - 1) {
          throw error;
        }
        continue;
      }
    }

    createResponse(res, 'Query executed successfully', response);
  }
  catch (error: any) {
    const message = error instanceof Error ? error.message : 'An unknown error occurred';
    req.clientRecord.logger.error(`${error}`);

    createErrorResponse(res, 500, message);
  }
}
