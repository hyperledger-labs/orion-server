ARG NODE_VER=22
ARG ALPINE_VER=3.23

FROM node:${NODE_VER}-alpine${ALPINE_VER} AS builder

# Enable Corepack for Yarn
RUN corepack enable

WORKDIR /app

# Copy package files
COPY backend/package.json ./
COPY backend/yarn.lock ./
COPY backend/.yarnrc.yml ./

# Install dependencies
RUN yarn install --immutable

# Copy source code
COPY backend/ ./

# Build TypeScript
RUN yarn build

# Production stage
FROM node:${NODE_VER}-alpine${ALPINE_VER}

# Enable Corepack for Yarn
RUN corepack enable

WORKDIR /app

# Copy package files
COPY backend/package.json ./
COPY backend/yarn.lock ./
COPY backend/.yarnrc.yml ./

# Install production dependencies only
RUN yarn install

# Copy built application from builder
COPY --from=builder /app/dist ./dist

# Set environment variables
ENV NODE_ENV=production
ENV PORT=4001

# Expose port
EXPOSE 4001

# Start the application
CMD ["yarn", "start"]
