# RAG Backend - ESM + TypeScript + node_modules

✅ **Setup Complete!** Your backend now uses:
- **ES Modules (ESM)** - Same as your frontend
- **TypeScript** - Full type safety
- **Traditional node_modules** - No Yarn PnP

## Project Structure

```
backend/
├── src/                      ← TypeScript source files (ESM)
│   ├── index.ts             ← Main Express server
│   ├── types/
│   │   └── index.ts         ← TypeScript interfaces
│   ├── services/
│   │   ├── embedding.ts     ← Ollama embeddings (nomic-embed-text)
│   │   ├── vectordb.ts      ← pgvector queries
│   │   └── llm.ts           ← Ollama LLM (llama3.2)
│   └── routes/
│       └── chat.ts          ← POST /api/chat endpoint
├── node_modules/            ← Traditional package folder (172 packages)
├── dist/                    ← Compiled JavaScript (after build)
├── .yarnrc.yml              ← Config: nodeLinker: node-modules
├── package.json             ← "type": "module" for ESM
└── tsconfig.json            ← "module": "ES2022" for ESM
```

## Configuration

### package.json
```json
{
  "type": "module",           // ← ESM enabled
  "scripts": {
    "dev": "tsx watch src/index.ts",
    "build": "tsc",
    "start": "node dist/index.js"
  }
}
```

### tsconfig.json
```json
{
  "compilerOptions": {
    "module": "ES2022",              // ← ESM compilation
    "moduleResolution": "bundler",   // ← Modern resolution
    "rootDir": "./src",
    "outDir": "./dist"
  }
}
```

### .yarnrc.yml
```yaml
nodeLinker: node-modules  # ← No Yarn PnP
```

## Environment Variables

Create a `.env` file in this directory:

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=admin
DB_USER=admin
DB_PASSWORD=admin

# Ollama
OLLAMA_URL=http://localhost:11434

# Server
PORT=8000
FRONTEND_URL=http://localhost:5173

# Environment
NODE_ENV=development
```

## Running the Backend

### Development (with hot reload):
```bash
yarn dev
```

### Build TypeScript to JavaScript:
```bash
yarn build
```

### Run Production Build:
```bash
yarn start
```

## API Endpoints

### Health Check
```bash
GET http://localhost:8000/health
```

### Chat (RAG)
```bash
POST http://localhost:8000/api/chat
Content-Type: application/json

{
  "question": "What does SiDroCo do?"
}
```

Response:
```json
{
  "answer": "SiDroCo is a deep tech company...",
  "sources": ["Hero", "About"]
}
```

## How It Works (RAG Flow)

1. **User asks a question** → POST /api/chat
2. **Convert question to embedding** → Ollama (nomic-embed-text)
3. **Search vector database** → pgvector finds similar content
4. **Build context** → Retrieve relevant text chunks
5. **Generate answer** → Ollama (llama3.2) with context
6. **Return response** → Answer + sources

## ESM Import Rules

### ✅ Always include `.js` extension:
```typescript
import { something } from './utils.js';  // ✅ Correct
import { something } from './utils';     // ❌ Wrong
```

### ✅ Use `import type` for types:
```typescript
import type { Request, Response } from 'express';
```

### ✅ Top-level await works:
```typescript
const data = await fetch('...');
```

## Dependencies

### Production:
- `express` - Web server
- `pg` - PostgreSQL client
- `pgvector` - Vector extension
- `dotenv` - Environment variables
- `immutable` - Immutable data structures

### Development:
- `typescript` - TypeScript compiler
- `tsx` - TypeScript execution
- `@types/*` - Type definitions
- `nodemon` - File watcher

## Comparison with Frontend

| Feature | Frontend | Backend | Match? |
|---------|----------|---------|--------|
| Module System | ESM | ESM | ✅ |
| `"type": "module"` | ✅ | ✅ | ✅ |
| TypeScript | ✅ | ✅ | ✅ |
| Module Resolution | bundler | bundler | ✅ |
| Package Manager | Yarn 1.22 | Yarn 4.9 | ⚠️ Different versions |
| node_modules | ✅ | ✅ | ✅ |

**Consistent module system across frontend and backend!** 🎉

## Next Steps

1. ✅ Create `.env` file with your configuration
2. ✅ Start Ollama and pull models:
   ```bash
   ollama pull nomic-embed-text
   ollama pull llama3.2
   ```
3. ✅ Start PostgreSQL with pgvector
4. ✅ Run `yarn dev` to start development server
5. ✅ Test endpoints
6. 🚀 Build your RAG features!

## Troubleshooting

### "Cannot find module" errors
- Make sure you include `.js` extension in imports
- Check that `"type": "module"` is in package.json

### TypeScript errors
- Run `yarn install` to ensure all types are installed
- Check `tsconfig.json` has `"module": "ES2022"`

### Database connection errors
- Verify `.env` file exists with correct credentials
- Ensure PostgreSQL is running
- Check pgvector extension is installed

---

**Your backend is ready for RAG development!** 🚀
