import createApp, { initConnection } from '../core/server.js';
import type { Application } from 'express';
import coreRoutes from '../core/routes.js';

const startServer = async (): Promise<Application> => {
  const clientRecord = await initConnection();
  return createApp(coreRoutes, clientRecord);
};

export default startServer();
