import '../bin/dotenv.js';
import express, { Router } from 'express';
import type { Application, Request, Response, NextFunction } from 'express';
import swaggerUi from 'swagger-ui-express';
import cors from 'cors';
import setupRoutes from './router.js';
import type { RouterWithSwagger, Routes } from '../types/index.js';
import { generateOpenApiSpec } from '../helpers/generateOpenApiSpec.js';
import Logger from '../helpers/logger.js';
import type { LoggerRecordShape } from '../helpers/logger.js';
import type { ClientRecordShape } from '../helpers/utils.js';
import ClientRecord from '../helpers/utils.js';

export const initConnection = async (): Promise<ClientRecordShape> => {
  const logger : LoggerRecordShape = (new Logger()).initLogger(`${process.env.CLIENT_NAME}`)

  const clientRecord: ClientRecordShape = new ClientRecord({
    logger: logger,
    orionUrls: JSON.parse(process.env.ORION_URLS || '[]')
  });

  return clientRecord;
};

export default async (routes: Routes, clientRecord: ClientRecordShape): Promise<Application> => {
  const port = process.env.PORT || 4001;
  const router: RouterWithSwagger = setupRoutes(Router(), routes);
  const app = express();

  const openApiSpec = generateOpenApiSpec(router.swaggerPaths);

  // Enable CORS for frontend
  app.use(cors({
    origin: process.env.FRONTEND_URL || 'http://localhost:5173',
    credentials: true
  }));

  app.use(express.json({ limit: '50mb' }));
  
  // Middleware to inject clientRecord into every request
  app.use((req: Request, res: Response, next: NextFunction) => {
    req.clientRecord = clientRecord;
    next();
  });
  
  app.use(router);

  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(openApiSpec));

  app.listen(port, () => {
    console.log(`App listening on port ${port}`);
  });

  return app;
};
