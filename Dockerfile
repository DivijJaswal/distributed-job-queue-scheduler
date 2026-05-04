FROM node:22-slim

WORKDIR /app

COPY package.json yarn.lock* ./
RUN yarn install --frozen-lockfile --production=false || yarn install --production=false

COPY . .

ENV HOST=0.0.0.0
ENV PORT=4230
ENV JOBQ_DB=/data/jobq.sqlite

EXPOSE 4230

CMD ["yarn", "start"]
