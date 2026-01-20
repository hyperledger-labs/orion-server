import type { Router, Request, Response, NextFunction } from 'express';

/**
 * Express request handler type
 */
export type RequestHandler = (
  req: Request,
  res: Response,
  next?: NextFunction
) => void | Promise<void>;

/**
 * Swagger/OpenAPI definition
 */
interface SwaggerDefinition {
  [key: string]: any;
}

/**
 * Route endpoint definition
 */
export interface EndpointDefinition {
  method: 'get' | 'post' | 'put' | 'delete' | 'patch';
  controller: RequestHandler;
  validator?: any[];
  swagger?: SwaggerDefinition;
}

/**
 * Routes configuration object
 */
export interface Routes {
  [endpoint: string]: EndpointDefinition;
}

/**
 * Swagger paths for OpenAPI spec
 */
export interface SwaggerPaths {
  [endpoint: string]: {
    [method: string]: SwaggerDefinition;
  };
}

/**
 * Express Router extended with Swagger paths
 */
export interface RouterWithSwagger extends Router {
  swaggerPaths?: SwaggerPaths;
}
