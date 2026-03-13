const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

module.exports = async (req, res) => {
  try {
    const [raw, histRaw] = await Promise.all([
      redis.get('trend_data'),
      redis.get('trend_history'),
    ]);
    if (!raw) {
      return res.status(404).json({ error: '데이터 없음. /api/refresh 먼저 실행 필요' });
    }
    const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
    const history = histRaw
      ? (typeof histRaw === 'string' ? JSON.parse(histRaw) : histRaw)
      : [];
    res.status(200).json({ ...data, history });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
