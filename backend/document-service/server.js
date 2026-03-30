/*
 * AidFlow Document Service
 * Handles document upload, IPFS storage, and hash anchoring to blockchain
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const multer = require('multer');
const crypto = require('crypto');
const axios = require('axios');
const FormData = require('form-data');
const Joi = require('joi');
const winston = require('winston');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3004;

// Logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/document-service.log' })
  ]
});

// PostgreSQL connection
const pool = new Pool({
  host: process.env.POSTGRES_HOST || 'localhost',
  port: process.env.POSTGRES_PORT || 5432,
  user: process.env.POSTGRES_USER || 'aidflow',
  password: process.env.POSTGRES_PASSWORD || 'aidflow_secure_2024',
  database: process.env.POSTGRES_DB || 'aidflow_pii'
});

// IPFS configuration
const IPFS_CONFIG = {
  host: process.env.IPFS_HOST || 'localhost',
  port: process.env.IPFS_PORT || 5001,
  gateway: process.env.IPFS_GATEWAY || 'http://localhost:8080'
};

// Multer configuration for file uploads
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'application/pdf',
      'image/jpeg',
      'image/png',
      'image/jpg',
      'application/msword',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    ];
    
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Allowed: PDF, JPEG, PNG, DOC, DOCX'));
    }
  }
});

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(morgan('combined', { stream: { write: message => logger.info(message.trim()) } }));

// Validation schemas
const anchorSchema = Joi.object({
  documentType: Joi.string().valid(
    'MOU', 'GRN', 'INVOICE', 'ATTENDANCE', 'AUDIT_REPORT', 
    'DELIVERY_NOTE', 'PROCUREMENT_DOC', 'CONFIRMATION_SLIP'
  ).required(),
  entityType: Joi.string().valid('program', 'commodity', 'beneficiary', 'audit').required(),
  entityId: Joi.string().required(),
  metadata: Joi.object().optional()
});

// Health check
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    
    // Check IPFS connection
    const ipfsHealth = await axios.post(
      `${IPFS_CONFIG.host}:${IPFS_CONFIG.port}/api/v0/id`,
      {},
      { timeout: 5000 }
    ).catch(() => ({ data: null }));

    res.json({
      success: true,
      service: 'Document Service',
      database: 'connected',
      ipfs: ipfsHealth.data ? 'connected' : 'disconnected',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(503).json({
      success: false,
      service: 'Document Service',
      error: error.message
    });
  }
});

// Calculate SHA-256 hash
function calculateHash(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

// Upload file to IPFS
async function uploadToIPFS(buffer, filename) {
  try {
    const formData = new FormData();
    formData.append('file', buffer, { filename });

    const response = await axios.post(
      `${IPFS_CONFIG.host}:${IPFS_CONFIG.port}/api/v0/add`,
      formData,
      {
        headers: formData.getHeaders(),
        timeout: 60000
      }
    );

    return {
      cid: response.data.Hash,
      size: response.data.Size
    };
  } catch (error) {
    logger.error('IPFS upload error:', error.message);
    throw new Error('Failed to upload to IPFS');
  }
}

// Upload and anchor document
app.post('/upload', upload.single('document'), async (req, res) => {
  try {
    const { error, value } = anchorSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        success: false,
        error: error.details[0].message
      });
    }

    if (!req.file) {
      return res.status(400).json({
        success: false,
        error: 'No document provided'
      });
    }

    const { documentType, entityType, entityId, metadata } = value;
    const file = req.file;

    // Calculate hash
    const docHash = calculateHash(file.buffer);

    // Check if document already exists
    const existingDoc = await pool.query(
      'SELECT * FROM documents WHERE doc_hash = $1',
      [docHash]
    );

    if (existingDoc.rows.length > 0) {
      return res.json({
        success: true,
        message: 'Document already exists',
        data: {
          documentId: existingDoc.rows[0].document_id,
          docHash,
          cid: existingDoc.rows[0].ipfs_cid,
          url: `${IPFS_CONFIG.gateway}/ipfs/${existingDoc.rows[0].ipfs_cid}`
        }
      });
    }

    // Upload to IPFS
    const ipfsResult = await uploadToIPFS(file.buffer, file.originalname);

    // Store in database
    const docId = `doc:${Date.now()}:${Math.random().toString(36).substr(2, 9)}`;
    
    await pool.query(
      `INSERT INTO documents 
       (document_id, doc_hash, ipfs_cid, filename, mime_type, size, 
        document_type, entity_type, entity_id, metadata, uploaded_at) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())`,
      [
        docId, docHash, ipfsResult.cid, file.originalname, 
        file.mimetype, file.size, documentType, entityType, 
        entityId, JSON.stringify(metadata || {})
      ]
    );

    logger.info(`Document uploaded: ${docId}, CID: ${ipfsResult.cid}`);

    res.json({
      success: true,
      message: 'Document uploaded and anchored',
      data: {
        documentId: docId,
        docHash,
        cid: ipfsResult.cid,
        url: `${IPFS_CONFIG.gateway}/ipfs/${ipfsResult.cid}`,
        size: file.size,
        documentType,
        entityType,
        entityId
      }
    });
  } catch (error) {
    logger.error('Document upload error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Verify document hash
app.post('/verify', upload.single('document'), async (req, res) => {
  try {
    if (!req.file && !req.body.docHash) {
      return res.status(400).json({
        success: false,
        error: 'Provide either a document file or docHash'
      });
    }

    let docHash;
    
    if (req.file) {
      docHash = calculateHash(req.file.buffer);
    } else {
      docHash = req.body.docHash;
    }

    // Query database
    const result = await pool.query(
      `SELECT d.*, b.tx_id as anchored_tx_id, b.anchor_timestamp 
       FROM documents d 
       LEFT JOIN blockchain_anchors b ON d.document_id = b.document_id 
       WHERE d.doc_hash = $1`,
      [docHash]
    );

    if (result.rows.length === 0) {
      return res.json({
        success: true,
        verified: false,
        message: 'Document not found in system'
      });
    }

    const doc = result.rows[0];

    res.json({
      success: true,
      verified: true,
      data: {
        documentId: doc.document_id,
        docHash: doc.doc_hash,
        cid: doc.ipfs_cid,
        url: `${IPFS_CONFIG.gateway}/ipfs/${doc.ipfs_cid}`,
        documentType: doc.document_type,
        entityType: doc.entity_type,
        entityId: doc.entity_id,
        anchored: !!doc.anchored_tx_id,
        anchorTxId: doc.anchored_tx_id,
        anchorTimestamp: doc.anchor_timestamp,
        uploadedAt: doc.uploaded_at
      }
    });
  } catch (error) {
    logger.error('Document verification error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Get document by hash
app.get('/document/:docHash', async (req, res) => {
  try {
    const { docHash } = req.params;

    const result = await pool.query(
      'SELECT * FROM documents WHERE doc_hash = $1',
      [docHash]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Document not found'
      });
    }

    const doc = result.rows[0];

    res.json({
      success: true,
      data: {
        documentId: doc.document_id,
        docHash: doc.doc_hash,
        cid: doc.ipfs_cid,
        url: `${IPFS_CONFIG.gateway}/ipfs/${doc.ipfs_cid}`,
        filename: doc.filename,
        mimeType: doc.mime_type,
        size: doc.size,
        documentType: doc.document_type,
        entityType: doc.entity_type,
        entityId: doc.entity_id,
        uploadedAt: doc.uploaded_at
      }
    });
  } catch (error) {
    logger.error('Get document error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Get documents by entity
app.get('/documents/:entityType/:entityId', async (req, res) => {
  try {
    const { entityType, entityId } = req.params;

    const result = await pool.query(
      `SELECT document_id, doc_hash, ipfs_cid, filename, document_type, 
              uploaded_at, size 
       FROM documents 
       WHERE entity_type = $1 AND entity_id = $2 
       ORDER BY uploaded_at DESC`,
      [entityType, entityId]
    );

    res.json({
      success: true,
      data: {
        entityType,
        entityId,
        count: result.rows.length,
        documents: result.rows.map(doc => ({
          ...doc,
          url: `${IPFS_CONFIG.gateway}/ipfs/${doc.ipfs_cid}`
        }))
      }
    });
  } catch (error) {
    logger.error('Get documents error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Anchor document to blockchain
app.post('/anchor', async (req, res) => {
  try {
    const { documentId, txId } = req.body;

    if (!documentId || !txId) {
      return res.status(400).json({
        success: false,
        error: 'documentId and txId are required'
      });
    }

    // Update document with blockchain anchor
    await pool.query(
      `INSERT INTO blockchain_anchors (document_id, tx_id, anchor_timestamp) 
       VALUES ($1, $2, NOW())
       ON CONFLICT (document_id) DO UPDATE 
       SET tx_id = $2, anchor_timestamp = NOW()`,
      [documentId, txId]
    );

    logger.info(`Document anchored: ${documentId} -> ${txId}`);

    res.json({
      success: true,
      message: 'Document anchored to blockchain',
      data: {
        documentId,
        txId
      }
    });
  } catch (error) {
    logger.error('Anchor error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Get IPFS content
app.get('/ipfs/:cid', async (req, res) => {
  try {
    const { cid } = req.params;

    // Validate CID format
    if (!/^[a-zA-Z0-9]+$/.test(cid)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid CID format'
      });
    }

    // Redirect to IPFS gateway
    res.redirect(`${IPFS_CONFIG.gateway}/ipfs/${cid}`);
  } catch (error) {
    logger.error('IPFS redirect error:', error);
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
    error: err.message || 'Internal server error'
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
  logger.info(`Document Service running on port ${PORT}`);
});

module.exports = app;
