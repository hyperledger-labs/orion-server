import {components} from './schema.js';

export const generateOpenApiSpec = (swaggerPaths: any) => ({
  openapi: '3.0.0',
  info: {
    title: `Raido ${process.env.CLIENT_NAME || 'Client'} app`,
    version: '1.0.0',
    description: `API documentation Raido ${process.env.CLIENT_NAME || 'Client'} app`
  },
  servers: [
    {
      url: `http://${process.env.HOST}:${process.env.PORT}`,
      description: 'API server'
    }
  ],
  paths: swaggerPaths,
  components
});
