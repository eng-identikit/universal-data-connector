const express = require('express');
const { createMcpHandler } = require('@modelcontextprotocol/server');
const { toNodeHandler } = require('@modelcontextprotocol/node');
const logger = require('../../utils/logger');
const { asyncHandler } = require('./helpers');
const { createUdcMcpServer, describeTools } = require('../mcp/mcpServer');

/**
 * MCP (Model Context Protocol) endpoint for LLM clients: Streamable HTTP on
 * /api/v1/mcp, stateless (any instance of a cluster can answer any request).
 *
 * Authentication is the broker's own: an API key (X-API-Key, or
 * "Authorization: Bearer udc_...") or a user token; the tenant comes from the
 * key or from X-Tenant. Tools are read-only and see what the credentials see.
 * BROKER_MCP=false turns the endpoint off.
 */
module.exports = function mcpRoutes({ services, tenantRole }) {
  const router = express.Router();
  const enabled = process.env.BROKER_MCP !== 'false';

  // The factory receives the authInfo forwarded from req.auth, which carries the tenant context
  const handler = createMcpHandler(({ authInfo }) => createUdcMcpServer(services, authInfo.ctx), {
    onerror: (error) => logger.error(`MCP: ${error.message}`)
  });
  const nodeHandler = toNodeHandler(handler, { onerror: (error) => logger.error(`MCP adapter: ${error.message}`) });

  router.get('/mcp-info', asyncHandler(async (req, res) => {
    res.json({ enabled, path: '/api/v1/mcp', transport: 'streamable-http', tools: enabled ? describeTools(services) : [] });
  }));

  router.all('/mcp', (req, res, next) => (enabled ? next() : res.status(404).json({ error: 'NotFound', message: 'MCP endpoint disabled (BROKER_MCP=false)' })),
    tenantRole('viewer'),
    (req, res) => {
      req.auth = { ...req.auth, ctx: req.ctx };
      // Streamed (SSE) answers must not be held back by a proxy
      res.setHeader('X-Accel-Buffering', 'no');
      nodeHandler(req, res, req.body);
    });

  return router;
};
