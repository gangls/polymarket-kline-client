# Jenkins Deployment Guide

This document describes how to deploy `@polymarket/kline-client` with Jenkins using Node.js on a Linux server without Docker.

## Deployment Overview

The deployment flow is:

1. Jenkins pulls code from the Git repository.
2. Jenkins installs dependencies.
3. Jenkins builds the TypeScript project.
4. Jenkins packages the runtime files.
5. Jenkins uploads the build artifacts to the target server.
6. The target server installs production dependencies.
7. The service is restarted with `pm2`.

## Prerequisites

### Jenkins server

Make sure Jenkins has:

- Git configured
- Node.js configured in Jenkins tools
- Yarn available
- SSH credentials configured for the target server

### Target server

Make sure the target server has:

- Node.js installed
- Yarn installed
- `pm2` installed globally
- A deployment directory created, for example:

```bash
mkdir -p /opt/polymarket-kline-client
```

Install `pm2` if needed:

```bash
npm install -g pm2
```

## Suggested Environment File

Create a production environment file on the target server, for example:

```bash
/opt/polymarket-kline-client/.env.production
```

Example:

```env
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_QUEUE_DB=2
WS_URL=wss://your-websocket-endpoint
```

Notes:

- `REDIS_QUEUE_DB` is used by the Bull queue Redis connection.
- This project does not define a custom queue prefix or namespace in code.
- If Redis is shared with other applications, use a dedicated Redis DB for queues.

## Suggested Jenkinsfile

Create a `Jenkinsfile` in the repository root or paste this pipeline into a Jenkins Pipeline job.

```groovy
pipeline {
  agent any

  tools {
    nodejs 'node18'
  }

  environment {
    APP_NAME = 'polymarket-kline-client'
    DEPLOY_DIR = '/opt/polymarket-kline-client'
    REMOTE_HOST = 'your-server'
    REMOTE_USER = 'deploy'
  }

  stages {
    stage('Checkout') {
      steps {
        git branch: 'main',
            credentialsId: 'git-credentials',
            url: 'https://github.com/gangls/polymarket-kline-client.git'
      }
    }

    stage('Install Dependencies') {
      steps {
        sh 'yarn install --frozen-lockfile'
      }
    }

    stage('Build') {
      steps {
        sh 'yarn build'
      }
    }

    stage('Package') {
      steps {
        sh '''
          rm -rf release
          mkdir -p release
          cp -r dist package.json yarn.lock release/
        '''
      }
    }

    stage('Deploy') {
      steps {
        sshagent(credentials: ['server-ssh']) {
          sh '''
            scp -r release/* ${REMOTE_USER}@${REMOTE_HOST}:${DEPLOY_DIR}/
            ssh ${REMOTE_USER}@${REMOTE_HOST} "
              cd ${DEPLOY_DIR} &&
              yarn install --production --frozen-lockfile &&
              cp -f .env.production .env &&
              pm2 restart ${APP_NAME} || pm2 start dist/index.js --name ${APP_NAME}
            "
          '''
        }
      }
    }
  }
}
```

## Jenkins Job Configuration

Recommended Jenkins configuration:

- **Job type**: Pipeline
- **Repository URL**: `https://github.com/gangls/polymarket-kline-client.git`
- **Branch**: `main`
- **Credentials**:
  - `git-credentials` for repository access
  - `server-ssh` for server login

If you use a Multibranch Pipeline, store the `Jenkinsfile` in the repository root.

## First-Time Server Setup

Run these commands on the target server before the first deployment:

```bash
mkdir -p /opt/polymarket-kline-client
cd /opt/polymarket-kline-client
npm install -g pm2
```

Create the environment file:

```bash
vi /opt/polymarket-kline-client/.env.production
```

## PM2 Management Commands

Start manually if needed:

```bash
cd /opt/polymarket-kline-client
cp .env.production .env
pm2 start dist/index.js --name polymarket-kline-client
```

Check status:

```bash
pm2 status
```

View logs:

```bash
pm2 logs polymarket-kline-client
```

Restart service:

```bash
pm2 restart polymarket-kline-client
```

Enable startup on server reboot:

```bash
pm2 startup
pm2 save
```

## Deployment Verification

After Jenkins deployment, verify:

```bash
pm2 status
pm2 logs polymarket-kline-client --lines 100
```

Also verify that:

- the process is online
- Redis connection succeeds
- the WebSocket connection succeeds
- expected queue activity appears in Redis

## Notes About This Project

- Build command: `yarn build` (`package.json:33`)
- Start command: `node dist/index.js` (`package.json:34`)
- Queue Redis DB is controlled by `REDIS_QUEUE_DB` (`src/config/queue.ts:12`)
- Main Redis clients use `db: 0` (`src/config/redis.ts:11`, `src/config/redis.ts:39`)

## Optional Improvement

If you want more stable releases later, you can extend this deployment process with:

- a dedicated `Jenkinsfile` committed to the repository
- a separate deploy script such as `scripts/deploy.sh`
- rollback support
- environment-specific Jenkins jobs such as `test` and `prod`
