export const components = {
  schemas: {
    Token: {
      type: 'object',
      properties: {
        tokenId: {type: 'string', description: 'Unique identifier for the token'},
        owner: {type: 'string', description: 'Owner of the token'},
        tokenURI: {type: 'string', description: 'URI containing metadata of the token'}
      },
      required: ['tokenId', 'owner', 'tokenURI']
    },
    Balance: {
      type: 'object',
      properties: {
        owner: {type: 'string', description: 'Address of the token owner'},
        balance: {type: 'integer', description: 'Number of tokens owned'}
      }
    },
    ContractInfo: {
      type: 'object',
      properties: {
        name: {type: 'string', description: 'Name of the token contract'},
        symbol: {type: 'string', description: 'Symbol of the token contract'}
      }
    }
  }
};
