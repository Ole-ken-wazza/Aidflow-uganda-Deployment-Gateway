/*
 * AidFlow API Gateway
 * Central entry point for all microservices
 * Handles authentication, rate limiting, and request routing
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');
const { createProxyMiddleware } = require('http-proxy-middleware');
const jwt = require('jsonwebtoken');
const Redis = require('redis');
const winston = require('winston');
const { metricsMiddleware, metricsEndpoint } = require('../shared/metrics');
require('dotenv').config();

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3000;

// Winston logger configuration
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/error.log', level: 'error' }),
    new winston.transports.File({ filename: 'logs/combined.log' })
  ]
});

// Redis client for session management
const redisClient = Redis.createClient({
  socket: {
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379
  },
  password: process.env.REDIS_PASSWORD
});

redisClient.on('error', (err) => logger.error('Redis Client Error:', err));
redisClient.connect().catch(console.error);

// Middleware
app.use(helmet());
app.use(cors({
  origin: process.env.ALLOWED_ORIGINS?.split(',') || ['http://localhost:3000'],
  credentials: true
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(morgan('combined', { stream: { write: message => logger.info(message.trim()) } }));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    logger.warn(`Rate limit exceeded for IP: ${req.ip}`);
    res.status(429).json({
      success: false,
      error: 'Too many requests, please try again later'
    });
  }
});
app.use(limiter);

// Stricter rate limiting for auth endpoints
const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 5,
  skipSuccessfulRequests: true
});

// JWT Authentication middleware
const authenticateToken = async (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({
      success: false,
      error: 'Access token required'
    });
  }

  try {
    // Check if token is blacklisted
    const isBlacklisted = await redisClient.get(`blacklist:${token}`);
    if (isBlacklisted) {
      return res.status(401).json({
        success: false,
        error: 'Token has been revoked'
      });
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    req.user = decoded;
    next();
  } catch (error) {
    logger.error('JWT verification failed:', error.message);
    return res.status(403).json({
      success: false,
      error: 'Invalid or expired token'
    });
  }
};

// Role-based authorization middleware
const authorize = (...roles) => {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: 'Authentication required'
      });
    }

    if (!roles.includes(req.user.role)) {
      logger.warn(`Unauthorized access attempt by user ${req.user.id} with role ${req.user.role}`);
      return res.status(403).json({
        success: false,
        error: 'Insufficient permissions'
      });
    }

    next();
  };
};

// Service URLs from environment
const SERVICES = {
  token: process.env.TOKEN_SERVICE_URL || 'http://localhost:3001',
  payment: process.env.PAYMENT_SERVICE_URL || 'http://localhost:3002',
  notification: process.env.NOTIFICATION_SERVICE_URL || 'http://localhost:3003',
  document: process.env.DOCUMENT_SERVICE_URL || 'http://localhost:3004',
  analytics: process.env.ANALYTICS_SERVICE_URL || 'http://localhost:3005'
};

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    success: true,
    service: 'AidFlow API Gateway',
    status: 'healthy',
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
});

// Public routes
app.get('/api/v1/status', (req, res) => {
  res.json({
    success: true,
    message: 'AidFlow API Gateway is running',
    version: '1.0.0',
    documentation: '/api/v1/docs'
  });
});

// Authentication routes
app.post('/api/v1/auth/login', authLimiter, async (req, res) => {
  try {
    const { email, password, mfaToken } = req.body;

    // TODO: Validate credentials against database
    // This is a placeholder - implement actual authentication
    
    // Mock user for demonstration
    const user = {
      id: 'user-001',
      email: email,
      role: 'ADMIN',
      orgId: 'org:system:admin',
      permissions: ['read', 'write', 'admin']
    };

    const token = jwt.sign(
      { id: user.id, email: user.email, role: user.role, orgId: user.orgId },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRY || '24h' }
    );

    const refreshToken = jwt.sign(
      { id: user.id, type: 'refresh' },
      process.env.JWT_SECRET,
      { expiresIn: '7d' }
    );

    // Store refresh token in Redis
    await redisClient.setEx(`refresh:${user.id}`, 7 * 24 * 60 * 60, refreshToken);

    logger.info(`User ${user.id} logged in successfully`);

    res.json({
      success: true,
      message: 'Login successful',
      data: {
        user: {
          id: user.id,
          email: user.email,
          role: user.role,
          orgId: user.orgId
        },
        token,
        refreshToken,
        expiresIn: 86400 // 24 hours in seconds
      }
    });
  } catch (error) {
    logger.error('Login error:', error);
    res.status(500).json({
      success: false,
      error: 'Authentication failed'
    });
  }
});

app.post('/api/v1/auth/logout', authenticateToken, async (req, res) => {
  try {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    // Blacklist the token
    const decoded = jwt.decode(token);
    const expiryTime = decoded.exp - Math.floor(Date.now() / 1000);
    
    if (expiryTime > 0) {
      await redisClient.setEx(`blacklist:${token}`, expiryTime, 'true');
    }

    // Remove refresh token
    await redisClient.del(`refresh:${req.user.id}`);

    logger.info(`User ${req.user.id} logged out`);

    res.json({
      success: true,
      message: 'Logout successful'
    });
  } catch (error) {
    logger.error('Logout error:', error);
    res.status(500).json({
      success: false,
      error: 'Logout failed'
    });
  }
});

app.post('/api/v1/auth/refresh', async (req, res) => {
  try {
    const { refreshToken } = req.body;

    if (!refreshToken) {
      return res.status(401).json({
        success: false,
        error: 'Refresh token required'
      });
    }

    const decoded = jwt.verify(refreshToken, process.env.JWT_SECRET);
    
    // Verify refresh token in Redis
    const storedToken = await redisClient.get(`refresh:${decoded.id}`);
    if (storedToken !== refreshToken) {
      return res.status(403).json({
        success: false,
        error: 'Invalid refresh token'
      });
    }

    // Issue new access token
    const newToken = jwt.sign(
      { id: decoded.id },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRY || '24h' }
    );

    res.json({
      success: true,
      token: newToken,
      expiresIn: 86400
    });
  } catch (error) {
    logger.error('Token refresh error:', error);
    res.status(403).json({
      success: false,
      error: 'Invalid refresh token'
    });
  }
});

// Proxy middleware configuration
const createServiceProxy = (serviceName, targetUrl) => {
  return createProxyMiddleware({
    target: targetUrl,
    changeOrigin: true,
    pathRewrite: {
      [`^/api/v1/${serviceName}`]: ''
    },
    onProxyReq: (proxyReq, req, res) => {
      // Add user info to proxied request
      if (req.user) {
        proxyReq.setHeader('X-User-Id', req.user.id);
        proxyReq.setHeader('X-User-Role', req.user.role);
        proxyReq.setHeader('X-Org-Id', req.user.orgId);
      }
      logger.info(`Proxying ${req.method} ${req.path} to ${serviceName} service`);
    },
    onProxyRes: (proxyRes, req, res) => {
      logger.info(`Response from ${serviceName}: ${proxyRes.statusCode}`);
    },
    onError: (err, req, res) => {
      logger.error(`Proxy error for ${serviceName}:`, err.message);
      res.status(503).json({
        success: false,
        error: `${serviceName} service unavailable`
      });
    }
  });
};

// Protected service routes
app.use('/api/v1/tokens', authenticateToken, createServiceProxy('tokens', SERVICES.token));
app.use('/api/v1/payments', authenticateToken, createServiceProxy('payments', SERVICES.payment));
app.use('/api/v1/notifications', authenticateToken, createServiceProxy('notifications', SERVICES.notification));
app.use('/api/v1/documents', authenticateToken, createServiceProxy('documents', SERVICES.document));
app.use('/api/v1/analytics', authenticateToken, createServiceProxy('analytics', SERVICES.analytics));

// Admin-only routes
app.use('/api/v1/admin', authenticateToken, authorize('ADMIN', 'SYSTEM_ADMIN'));

// Webhook routes (public but with signature verification)
app.use('/webhooks', (req, res, next) => {
  // TODO: Implement webhook signature verification
  next();
});

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Unhandled error:', err);
  res.status(500).json({
    success: false,
    error: process.env.NODE_ENV === 'production' 
      ? 'Internal server error' 
      : err.message
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found'
  });
});

// Start server
app.listen(PORT, () => {
  logger.info(`AidFlow API Gateway running on port ${PORT}`);
  logger.info(`Environment: ${process.env.NODE_ENV || 'development'}`);
});

module.exports = app;
