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

    // 구형 데이터 호환성: 없는 필드 기본값 채우기
    const normalized = {
      updatedAt: data.updatedAt || new Date().toISOString(),
      rankUpdatedAt: data.rankUpdatedAt || data.updatedAt || new Date().toISOString(),
      collectUpdatedAt: data.collectUpdatedAt || data.updatedAt || new Date().toISOString(),
      keywords: (data.keywords || []).map(k => ({
        rank: k.rank || 1,
        prevRank: k.prevRank || null,
        keyword: k.keyword || '',
        score: k.score || 0,
        changeRate: k.changeRate || 0,
        risingRate: k.risingRate || 0,
        blogGrowth: k.blogGrowth || 0,
        postCount: k.postCount || 0,
        trend: k.trend || '유행중',
        isNew: k.isNew || false,
        isEarlyTrend: k.isEarlyTrend || false,
        earlyScore: k.earlyScore || 0,
        comment: k.comment || '',
        values: k.values || [],
      })),
      rising: data.rising || [],
      earlyTrends: data.earlyTrends || [],
    };

    // detail=1이면 히스토리 포함
    const url = req.url || '';
    const isDetail = url.includes('detail=1');
    if (isDetail) {
      try {
        const histRaw = await redis.get('trend_history');
        const history = histRaw
          ? (typeof histRaw === 'string' ? JSON.parse(histRaw) : histRaw)
          : [];
        return res.status(200).json({ ...normalized, history });
      } catch(e) {
        return res.status(200).json({ ...normalized, history: [] });
      }
    }

    res.status(200).json(normalized);

  } catch (err) {
    console.error('[data] 오류:', err);
    res.status(500).json({ error: err.message });
  }
};
