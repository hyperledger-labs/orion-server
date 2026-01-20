import type {
  Routes,
  RouterWithSwagger,
  SwaggerPaths
} from '../types/index.js';

export default (router: RouterWithSwagger, routes: Routes): RouterWithSwagger => {
  const swaggerPaths: SwaggerPaths = {};

  Object.entries(routes).forEach(([endpoint, endpointDef]) => {
    const {
      method,
      controller,
      validator,
      swagger
    } = endpointDef;

    router[method](
      endpoint,
      validator || [],
      controller
    );
    
    const swaggerEndpoint = endpoint.replace(/:([^/]+)/g, '{$1}');

    if (swagger) {
      if (!swaggerPaths[swaggerEndpoint]) {
        swaggerPaths[swaggerEndpoint] = {};
      }
      swaggerPaths[swaggerEndpoint][method] = {
        ...swagger
      };
    }
  });

  router.swaggerPaths = swaggerPaths;

  return router;
};
