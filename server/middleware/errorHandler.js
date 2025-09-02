module.exports = (error, req, res, next) => {
  console.error(`[${new Date().toISOString()}] ❌ Server Error:`, error);
  
  if (res.headersSent) {
    return next(error);
  }

  if (error.type === 'entity.too.large') {
    return res.status(413).json({
      success: false,
      error: 'Request too large. Maximum 50MB allowed.',
      code: 'REQUEST_TOO_LARGE'
    });
  }

  if (error.type === 'entity.parse.failed') {
    return res.status(400).json({
      success: false,
      error: 'Invalid JSON format in request body.',
      code: 'JSON_PARSE_ERROR'
    });
  }

  if (error.code === 'TIMEOUT') {
    return res.status(408).json({
      success: false,
      error: 'Request timeout. Try with smaller batches.',
      code: 'TIMEOUT'
    });
  }

  res.status(500).json({
    success: false,
    error: 'Internal server error',
    message: error.message,
    code: 'INTERNAL_ERROR'
  });
};
