export default async function handler(req, res) {
  const apiKey = process.env.FINNHUB_API_KEY;

  if (!apiKey) {
    return res.status(500).json({ error: 'FINNHUB_API_KEY fehlt in Vercel Environment Variables.' });
  }

  const { endpoint, ...params } = req.query;

  if (!endpoint) {
    return res.status(400).json({ error: 'endpoint fehlt' });
  }

  const allowedEndpoints = new Set([
    'quote',
    'stock/candle',
    'calendar/earnings'
  ]);

  if (!allowedEndpoints.has(endpoint)) {
    return res.status(400).json({ error: 'endpoint nicht erlaubt' });
  }

  const url = new URL(`https://finnhub.io/api/v1/${endpoint}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, value);
    }
  });
  url.searchParams.set('token', apiKey);

  try {
    const response = await fetch(url.toString());
    const text = await response.text();

    res.setHeader('Content-Type', 'application/json');
    return res.status(response.status).send(text);
  } catch (error) {
    return res.status(500).json({
      error: 'Finnhub Request fehlgeschlagen',
      details: String(error)
    });
  }
}
