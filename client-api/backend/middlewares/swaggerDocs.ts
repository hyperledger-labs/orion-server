export const healthDoc = {
  summary: 'Health check',
  description: 'Check the health of the server',
  tags: ['Health'],
  responses: {
    200: {
      description: 'Server is healthy',
      content: {
        'application/json': {
          schema: {
            type: 'object',
            properties: {
              message: {
                type: 'string',
                example: 'Initiated ledger'
              }
            }
          }
        }
      }
    }
  }
};
