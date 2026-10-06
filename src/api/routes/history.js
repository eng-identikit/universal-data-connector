const express = require('express');
const router = express.Router();
const logger = require('../../utils/logger');
const history = require('../../storage/TimescaleHistory');

function getEngine() {
  return global.connector?.getEngine() || null;
}

function handleError(res, error, action) {
  const status = error.status || 500;
  if (status >= 500) {
    logger.error(`History API: failed to ${action}:`, error);
  }
  res.status(status).json({
    error: status === 400 ? 'Bad Request' : status === 409 ? 'Not Configured' : 'Internal Server Error',
    message: error.message
  });
}

// TimescaleDB availability and data range
router.get('/status', async (req, res) => {
  try {
    res.json({ timestamp: new Date().toISOString(), ...(await history.status(getEngine())) });
  } catch (error) {
    handleError(res, error, 'get history status');
  }
});

// Sources with recorded data
router.get('/sources', async (req, res) => {
  try {
    const sources = await history.sources(getEngine(), req.query);
    res.json({ timestamp: new Date().toISOString(), total: sources.length, sources });
  } catch (error) {
    handleError(res, error, 'list history sources');
  }
});

// Measurements recorded for a source
router.get('/measurements', async (req, res) => {
  try {
    if (!req.query.source) {
      return res.status(400).json({ error: 'Bad Request', message: 'source is required' });
    }
    const measurements = await history.measurements(getEngine(), { ...req.query, sourceId: req.query.source });
    res.json({ timestamp: new Date().toISOString(), sourceId: req.query.source, total: measurements.length, measurements });
  } catch (error) {
    handleError(res, error, 'list measurements');
  }
});

// Downsampled time series: ?source=&measurements=a,b&startTime=&endTime=&maxPoints=&bucket=
router.get('/series', async (req, res) => {
  try {
    if (!req.query.source) {
      return res.status(400).json({ error: 'Bad Request', message: 'source is required' });
    }
    const result = await history.series(getEngine(), { ...req.query, sourceId: req.query.source });
    res.json({ timestamp: new Date().toISOString(), ...result });
  } catch (error) {
    handleError(res, error, 'query time series');
  }
});

// Raw records: ?source=&startTime=&endTime=&limit=&offset=&order=asc|desc
router.get('/records', async (req, res) => {
  try {
    const result = await history.records(getEngine(), { ...req.query, sourceId: req.query.source });
    res.json({ timestamp: new Date().toISOString(), ...result });
  } catch (error) {
    handleError(res, error, 'query records');
  }
});

module.exports = router;
