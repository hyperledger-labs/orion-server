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
    let response;

    try {
      response = await customFetch(
        `${req.clientRecord.orionUrls['orion-server1']}/db/tx`,
        'POST',
        {
          body: { payload, signature },
          headers: {
            'Content-Type': 'application/json',
            'TxTimeout': '20s'
          }
        }
      ); 
    }
    catch (error: any) {
      if (error.cause?.code === 'ENOTFOUND') {
        const hostname = error.cause?.hostname;

        if (hostname && req.clientRecord.orionUrls[hostname]) {
          req.clientRecord.logger.info(`Redirect failed for ${hostname}, retrying with mapped URL`);

          // Retry with mapped URL
          response = await customFetch(
            `${req.clientRecord.orionUrls[hostname]}/db/tx`,
            'POST',
            {
              body: { payload, signature },
              headers: {
                'Content-Type': 'application/json',
                'TxTimeout': '20s'
              }
            }
          );
        } else {
          console.log('first')
          throw error;  // Re-throw if hostname not in map
        }
      } else {
        console.log('second')
        throw error;  // Re-throw if not a DNS error
      }
    }

    createResponse(res, 'Transaction committed successfully', response);
  }
  catch (error: any) {
    console.log('third')
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

    let response;
    try {
      response = await customFetch(
        `http://127.0.0.1:6001/db/${dbName}`,
        'GET',
        {
          headers: {
            'Content-Type': 'application/json',
            'UserID': userId,
            'Signature': signature
          }
        }
      );
    }
    catch (error: any) {
      if (error.cause?.code === 'ENOTFOUND') {
        const hostname = error.cause?.hostname;

        if (hostname && req.clientRecord.orionUrls[hostname]) {
          req.clientRecord.logger.info(`Redirect failed for ${hostname}, retrying with mapped URL`);

          // Extract path from original URL
          const newUrl = `${req.clientRecord.orionUrls[hostname]}/db/${dbName}`;

          // Retry with mapped URL
          response = await customFetch(
            newUrl,
            'GET',
            {
              headers: {
                'Content-Type': 'application/json',
                'UserID': userId,
                'Signature': signature
              }
            }
          );
        } else {
          throw error;  // Re-throw if hostname not in map
        }
      } else {
        throw error;  // Re-throw if not a DNS error
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
