/*
 * AidFlow Analytics Service
 * ClickHouse integration for dashboards and anomaly detection
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { createClient } = require('@clickhouse/client');
const Joi = require('joi');
const winston = require('winston');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3005;

// Logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/analytics-service.log' })
  ]
});

// ClickHouse client
const clickhouse = createClient({
  host: `${process.env.CLICKHOUSE_HOST || 'localhost'}:${process.env.CLICKHOUSE_PORT || 8123}`,
  username: process.env.CLICKHOUSE_USER || 'aidflow',
  password: process.env.CLICKHOUSE_PASSWORD || 'aidflow_analytics_2024',
  database: process.env.CLICKHOUSE_DB || 'aidflow_analytics'
});

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(morgan('combined', { stream: { write: message => logger.info(message.trim()) } }));

// Health check
app.get('/health', async (req, res) => {
  try {
    await clickhouse.ping();
    res.json({
      success: true,
      service: 'Analytics Service',
      clickhouse: 'connected',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(503).json({
      success: false,
      service: 'Analytics Service',
      clickhouse: 'disconnected',
      error: error.message
    });
  }
});

// Dashboard stats
app.get('/dashboard/stats', async (req, res) => {
  try {
    const { progId, days = 30 } = req.query;
    
    // Total redemptions
    const redemptionsQuery = `
      SELECT 
        count() as total_redemptions,
        sum(amount) as total_amount,
        uniq(beneficiary_id) as unique_beneficiaries
      FROM redemption_events
      WHERE event_date >= today() - ${days}
      ${progId ? `AND prog_id = '${progId}'` : ''}
    `;
    
    const redemptionsResult = await clickhouse.query({
      query: redemptionsQuery,
      format: 'JSONEachRow'
    }).then(result => result.json());

    // Token events
    const tokensQuery = `
      SELECT 
        event_type,
        count() as count,
        sum(amount) as total_amount
      FROM token_events
      WHERE event_date >= today() - ${days}
      ${progId ? `AND prog_id = '${progId}'` : ''}
      GROUP BY event_type
    `;
    
    const tokensResult = await clickhouse.query({
      query: tokensQuery,
      format: 'JSONEachRow'
    }).then(result => result.json());

    // Anomalies
    const anomaliesQuery = `
      SELECT 
        count() as total_anomalies,
        sum(severity = 'CRITICAL') as critical,
        sum(severity = 'HIGH') as high
      FROM anomaly_events
      WHERE event_date >= today() - ${days}
      ${progId ? `AND prog_id = '${progId}'` : ''}
      AND status = 'OPEN'
    `;
    
    const anomaliesResult = await clickhouse.query({
      query: anomaliesQuery,
      format: 'JSONEachRow'
    }).then(result => result.json());

    res.json({
      success: true,
      data: {
        redemptions: redemptionsResult[0],
        tokens: tokensResult,
        anomalies: anomaliesResult[0]
      }
    });
  } catch (error) {
    logger.error('Dashboard stats error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Time series data
app.get('/timeseries/:metric', async (req, res) => {
  try {
    const { metric } = req.params;
    const { progId, days = 30, interval = 'day' } = req.query;

    const intervalMap = {
      hour: 'toStartOfHour',
      day: 'toStartOfDay',
      week: 'toStartOfWeek',
      month: 'toStartOfMonth'
    };

    const timeGroup = intervalMap[interval] || 'toStartOfDay';

    let query;
    
    if (metric === 'redemptions') {
      query = `
        SELECT 
          ${timeGroup}(event_time) as time_bucket,
          count() as count,
          sum(amount) as total_amount,
          uniq(beneficiary_id) as unique_beneficiaries
        FROM redemption_events
        WHERE event_date >= today() - ${days}
        ${progId ? `AND prog_id = '${progId}'` : ''}
        GROUP BY time_bucket
        ORDER BY time_bucket
      `;
    } else if (metric === 'tokens') {
      query = `
        SELECT 
          ${timeGroup}(event_time) as time_bucket,
          event_type,
          count() as count,
          sum(amount) as total_amount
        FROM token_events
        WHERE event_date >= today() - ${days}
        ${progId ? `AND prog_id = '${progId}'` : ''}
        GROUP BY time_bucket, event_type
        ORDER BY time_bucket
      `;
    } else {
      return res.status(400).json({
        success: false,
        error: 'Invalid metric'
      });
    }

    const result = await clickhouse.query({
      query,
      format: 'JSONEachRow'
    }).then(result => result.json());

    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    logger.error('Time series error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Geographic distribution
app.get('/geo/distribution', async (req, res) => {
  try {
    const { progId, days = 30 } = req.query;

    const query = `
      SELECT 
        district,
        uniqMerge(beneficiary_count) as beneficiaries,
        sumMerge(total_amount) as total_redeemed
      FROM geographic_distribution
      WHERE event_date >= today() - ${days}
      ${progId ? `AND prog_id = '${progId}'` : ''}
      GROUP BY district
      ORDER BY total_redeemed DESC
      LIMIT 50
    `;

    const result = await clickhouse.query({
      query,
      format: 'JSONEachRow'
    }).then(result => result.json());

    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    logger.error('Geo distribution error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Fraud detection patterns
app.get('/fraud/patterns', async (req, res) => {
  try {
    const { days = 7 } = req.query;

    // Velocity check - multiple redemptions in short time
    const velocityQuery = `
      SELECT 
        beneficiary_id,
        count() as redemption_count,
        sum(amount) as total_amount,
        max(event_time) - min(event_time) as time_span_seconds
      FROM redemption_events
      WHERE event_time >= now() - INTERVAL ${days} DAY
      GROUP BY beneficiary_id
      HAVING redemption_count > 5 OR total_amount > 500000
      ORDER BY redemption_count DESC
      LIMIT 100
    `;

    const velocityResult = await clickhouse.query({
      query: velocityQuery,
      format: 'JSONEachRow'
    }).then(result => result.json());

    // Duplicate detection - same amount, same day
    const duplicateQuery = `
      SELECT 
        beneficiary_id,
        toDate(event_time) as redemption_date,
        amount,
        count() as duplicate_count
      FROM redemption_events
      WHERE event_time >= now() - INTERVAL ${days} DAY
      GROUP BY beneficiary_id, redemption_date, amount
      HAVING duplicate_count > 1
      ORDER BY duplicate_count DESC
      LIMIT 100
    `;

    const duplicateResult = await clickhouse.query({
      query: duplicateQuery,
      format: 'JSONEachRow'
    }).then(result => result.json());

    res.json({
      success: true,
      data: {
        velocityAlerts: velocityResult,
        duplicateAlerts: duplicateResult
      }
    });
  } catch (error) {
    logger.error('Fraud patterns error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Program performance report
app.get('/reports/programs', async (req, res) => {
  try {
    const { days = 30 } = req.query;

    const query = `
      SELECT 
        prog_id,
        count() as redemption_count,
        sum(amount) as total_redeemed,
        uniq(beneficiary_id) as unique_beneficiaries,
        avg(amount) as avg_redemption_amount
      FROM redemption_events
      WHERE event_date >= today() - ${days}
      GROUP BY prog_id
      ORDER BY total_redeemed DESC
    `;

    const result = await clickhouse.query({
      query,
      format: 'JSONEachRow'
    }).then(result => result.json());

    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    logger.error('Program report error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Channel performance
app.get('/reports/channels', async (req, res) => {
  try {
    const { days = 30 } = req.query;

    const query = `
      SELECT 
        channel,
        provider,
        count() as redemption_count,
        sum(amount) as total_amount,
        avg(processing_time_ms) as avg_processing_time,
        sum(status = 'FAILED') / count() * 100 as failure_rate
      FROM redemption_events
      WHERE event_date >= today() - ${days}
      GROUP BY channel, provider
      ORDER BY total_amount DESC
    `;

    const result = await clickhouse.query({
      query,
      format: 'JSONEachRow'
    }).then(result => result.json());

    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    logger.error('Channel report error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Error handling
app.use((err, req, res, next) => {
  logger.error('Unhandled error:', err);
  res.status(500).json({
    success: false,
    error: 'Internal server error'
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found'
  });
});

app.listen(PORT, () => {
  logger.info(`Analytics Service running on port ${PORT}`);
});

module.exports = app;
