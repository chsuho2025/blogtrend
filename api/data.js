const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

module.exports = async (req, res) => {
  try {
    const raw = await redis.get('trend_data');
    if (!raw) {
      return res.status(404).json({ error: '데이터 없음. /api/refresh 먼저 실행 필요' });
    }
    const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
    res.status(200).json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
