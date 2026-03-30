/*
 * AidFlow Notification Service
 * Handles SMS, USSD, and email notifications via AfricasTalking API
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const axios = require('axios');
const Joi = require('joi');
const winston = require('winston');
const Redis = require('redis');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3003;

// Logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/notification-service.log' })
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

// AfricasTalking configuration
const AT_CONFIG = {
  apiKey: process.env.AFRICASTALKING_API_KEY,
  username: process.env.AFRICASTALKING_USERNAME || 'sandbox',
  smsShortcode: process.env.AFRICASTALKING_SMS_SHORTCODE,
  ussdShortcode: process.env.AFRICASTALKING_USSD_SHORTCODE || '*123*AIDFLOW#'
};

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(morgan('combined', { stream: { write: message => logger.info(message.trim()) } }));

// Validation schemas
const sendSMSSchema = Joi.object({
  phoneNumber: Joi.string().pattern(/^\+?256[0-9]{9}$/).required(),
  message: Joi.string().max(1600).required(),
  senderId: Joi.string().optional()
});

const sendBulkSMSSchema = Joi.object({
  phoneNumbers: Joi.array().items(Joi.string().pattern(/^\+?256[0-9]{9}$/)).min(1).max(100).required(),
  message: Joi.string().max(1600).required(),
  senderId: Joi.string().optional()
});

const ussdSessionSchema = Joi.object({
  sessionId: Joi.string().required(),
  phoneNumber: Joi.string().required(),
  text: Joi.string().allow('').default(''),
  serviceCode: Joi.string().required()
});

// Health check
app.get('/health', async (req, res) => {
  try {
    await redisClient.ping();
    res.json({
      success: true,
      service: 'Notification Service',
      redis: 'connected',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(503).json({
      success: false,
      service: 'Notification Service',
      redis: 'disconnected'
    });
  }
});

// Send SMS
app.post('/sms/send', async (req, res) => {
  try {
    const { error, value } = sendSMSSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        success: false,
        error: error.details[0].message
      });
    }

    const { phoneNumber, message, senderId } = value;

    // Call AfricasTalking API
    const response = await axios.post(
      'https://api.africastalking.com/version1/messaging',
      new URLSearchParams({
        username: AT_CONFIG.username,
        to: phoneNumber,
        message: message,
        from: senderId || AT_CONFIG.smsShortcode || ''
      }),
      {
        headers: {
          'apiKey': AT_CONFIG.apiKey,
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json'
        }
      }
    );

    const smsMessage = response.data.SMSMessageData.Message[0];
    
    logger.info(`SMS sent to ${phoneNumber}: ${smsMessage.status}`);

    res.json({
      success: true,
      message: 'SMS sent successfully',
      data: {
        messageId: smsMessage.messageId,
        status: smsMessage.status,
        number: smsMessage.number,
        cost: response.data.SMSMessageData.Cost
      }
    });
  } catch (error) {
    logger.error('SMS send error:', error.response?.data || error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to send SMS'
    });
  }
});

// Send bulk SMS
app.post('/sms/bulk', async (req, res) => {
  try {
    const { error, value } = sendBulkSMSSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        success: false,
        error: error.details[0].message
      });
    }

    const { phoneNumbers, message, senderId } = value;

    const response = await axios.post(
      'https://api.africastalking.com/version1/messaging',
      new URLSearchParams({
        username: AT_CONFIG.username,
        to: phoneNumbers.join(','),
        message: message,
        from: senderId || AT_CONFIG.smsShortcode || ''
      }),
      {
        headers: {
          'apiKey': AT_CONFIG.apiKey,
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json'
        }
      }
    );

    logger.info(`Bulk SMS sent to ${phoneNumbers.length} recipients`);

    res.json({
      success: true,
      message: 'Bulk SMS sent',
      data: {
        recipients: phoneNumbers.length,
        results: response.data.SMSMessageData.Message
      }
    });
  } catch (error) {
    logger.error('Bulk SMS error:', error.response?.data || error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to send bulk SMS'
    });
  }
});

// USSD callback handler
app.post('/ussd/callback', async (req, res) => {
  try {
    const { sessionId, phoneNumber, text, serviceCode } = req.body;

    logger.info(`USSD request: session=${sessionId}, phone=${phoneNumber}, text=${text}`);

    // Get or create session
    const sessionKey = `ussd:session:${sessionId}`;
    let session = await redisClient.get(sessionKey);
    
    if (!session) {
      session = {
        sessionId,
        phoneNumber,
        step: 0,
        data: {}
      };
    } else {
      session = JSON.parse(session);
    }

    // Parse user input
    const input = text.split('*').pop();
    
    let response = '';
    let endSession = false;

    // USSD Menu Flow
    switch (session.step) {
      case 0: // Main menu
        response = `CON Welcome to AidFlow\n`;
        response += `1. Check Balance\n`;
        response += `2. Redeem Payment\n`;
        response += `3. Confirm Delivery\n`;
        response += `4. Help`;
        session.step = 1;
        break;

      case 1: // Menu selection
        switch (input) {
          case '1': // Check Balance
            // TODO: Fetch actual balance from blockchain
            response = `END Your AidFlow balance:\nUGX 0\nNo active entitlements.`;
            endSession = true;
            break;

          case '2': // Redeem Payment
            response = `CON Enter your AidFlow PIN (4 digits):`;
            session.step = 2;
            session.data.action = 'redeem';
            break;

          case '3': // Confirm Delivery
            response = `CON Enter delivery reference number:`;
            session.step = 3;
            session.data.action = 'confirm_delivery';
            break;

          case '4': // Help
            response = `END AidFlow Help:\n`;
            response += `For assistance, call:\n`;
            response += `0800-AIDFLOW\n`;
            response += `or visit aidflow.ug`;
            endSession = true;
            break;

          default:
            response = `END Invalid option. Please try again.`;
            endSession = true;
        }
        break;

      case 2: // PIN entry for redemption
        if (input.length === 4 && /^\d{4}$/.test(input)) {
          // TODO: Validate PIN against database
          session.data.pin = input;
          response = `CON Enter amount to redeem:\n(Max: UGX 100,000)`;
          session.step = 4;
        } else {
          response = `END Invalid PIN. Must be 4 digits.`;
          endSession = true;
        }
        break;

      case 3: // Delivery confirmation
        session.data.deliveryRef = input;
        // TODO: Validate delivery reference
        response = `END Delivery ${input} confirmed.\nThank you for using AidFlow.`;
        endSession = true;
        break;

      case 4: // Amount entry
        const amount = parseInt(input);
        if (isNaN(amount) || amount <= 0 || amount > 100000) {
          response = `END Invalid amount. Please try again.`;
        } else {
          // TODO: Process redemption via blockchain
          response = `END Redemption request sent:\n`;
          response += `Amount: UGX ${amount}\n`;
          response += `You will receive an SMS confirmation shortly.`;
        }
        endSession = true;
        break;

      default:
        response = `END Session expired. Please try again.`;
        endSession = true;
    }

    // Save or delete session
    if (endSession) {
      await redisClient.del(sessionKey);
    } else {
      await redisClient.setEx(sessionKey, 300, JSON.stringify(session)); // 5 min expiry
    }

    // Send USSD response
    res.set('Content-Type', 'text/plain');
    res.send(response);

  } catch (error) {
    logger.error('USSD callback error:', error);
    res.set('Content-Type', 'text/plain');
    res.send('END An error occurred. Please try again.');
  }
});

// Send OTP
app.post('/otp/send', async (req, res) => {
  try {
    const { phoneNumber, purpose } = req.body;

    // Generate 6-digit OTP
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    
    // Store OTP in Redis (5 minute expiry)
    const otpKey = `otp:${phoneNumber}:${purpose || 'default'}`;
    await redisClient.setEx(otpKey, 300, otp);

    // Send SMS with OTP
    const message = `Your AidFlow verification code is: ${otp}. Valid for 5 minutes. Do not share this code.`;
    
    await axios.post(
      'https://api.africastalking.com/version1/messaging',
      new URLSearchParams({
        username: AT_CONFIG.username,
        to: phoneNumber,
        message: message
      }),
      {
        headers: {
          'apiKey': AT_CONFIG.apiKey,
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      }
    );

    logger.info(`OTP sent to ${phoneNumber}`);

    res.json({
      success: true,
      message: 'OTP sent successfully',
      data: {
        phoneNumber: phoneNumber.replace(/(\+\d{3})\d{6}(\d{3})/, '$1******$2'),
        expiresIn: 300
      }
    });
  } catch (error) {
    logger.error('OTP send error:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to send OTP'
    });
  }
});

// Verify OTP
app.post('/otp/verify', async (req, res) => {
  try {
    const { phoneNumber, otp, purpose } = req.body;

    const otpKey = `otp:${phoneNumber}:${purpose || 'default'}`;
    const storedOtp = await redisClient.get(otpKey);

    if (!storedOtp) {
      return res.status(400).json({
        success: false,
        error: 'OTP expired or not found'
      });
    }

    if (storedOtp !== otp) {
      return res.status(400).json({
        success: false,
        error: 'Invalid OTP'
      });
    }

    // Delete OTP after successful verification
    await redisClient.del(otpKey);

    res.json({
      success: true,
      message: 'OTP verified successfully'
    });
  } catch (error) {
    logger.error('OTP verify error:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to verify OTP'
    });
  }
});

// Send notification (generic)
app.post('/notify', async (req, res) => {
  try {
    const { type, recipient, template, data } = req.body;

    let message = '';

    // Template messages
    switch (template) {
      case 'ENTITLEMENT_ASSIGNED':
        message = `You have been assigned UGX ${data.amount} in AidFlow program ${data.program}. Dial ${AT_CONFIG.ussdShortcode} to redeem.`;
        break;
      
      case 'PAYMENT_RECEIVED':
        message = `You have received UGX ${data.amount} via ${data.provider}. Transaction ID: ${data.transactionId}`;
        break;
      
      case 'REDEMPTION_CONFIRMED':
        message = `Your redemption of UGX ${data.amount} has been confirmed. Reference: ${data.reference}`;
        break;
      
      case 'EXPIRY_WARNING':
        message = `Your AidFlow entitlement of UGX ${data.amount} expires in ${data.days} days. Redeem now: ${AT_CONFIG.ussdShortcode}`;
        break;

      default:
        message = data.message || 'Notification from AidFlow';
    }

    if (type === 'SMS') {
      await axios.post(
        'https://api.africastalking.com/version1/messaging',
        new URLSearchParams({
          username: AT_CONFIG.username,
          to: recipient,
          message: message
        }),
        {
          headers: {
            'apiKey': AT_CONFIG.apiKey,
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );
    }

    logger.info(`Notification sent to ${recipient}: ${template}`);

    res.json({
      success: true,
      message: 'Notification sent'
    });
  } catch (error) {
    logger.error('Notification error:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to send notification'
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
  logger.info(`Notification Service running on port ${PORT}`);
});

module.exports = app;
