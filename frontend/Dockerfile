FROM node:18-alpine

WORKDIR /app

COPY package.json /app/package.json
RUN npm install --silent

# Copy source
COPY pages /app/pages

EXPOSE 3000

ENV NODE_ENV=production
ENV PORT=3000

RUN npm run build || true

CMD ["npm", "run", "start"]
