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

    // detail 파라미터 없으면 경량 응답 (초기 화면 빠르게)
    const detail = req.query?.detail === '1';
    if (!detail) {
      return res.status(200).json({
        updatedAt: data.updatedAt,
        rankUpdatedAt: data.rankUpdatedAt || data.updatedAt,
        collectUpdatedAt: data.collectUpdatedAt || data.updatedAt,
        keywords: data.keywords,
        rising: data.rising,
        earlyTrends: data.earlyTrends || [],
      });
    }

    // detail=1이면 히스토리 포함 전체 응답
    res.status(200).json({ ...data, history });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
