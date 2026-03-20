const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// ─────────────────────────────────────────
// 유틸
// ─────────────────────────────────────────
const avg = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;

function median(arr) {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function normKw(kw) {
  if (typeof kw !== 'string') return '';
  return kw.replace(/(레시피|추천|후기|방법|효능|사용법|만들기|하는법)/g, '').replace(/\s+/g, '').trim();
}

function daysDiff(dateStr) {
  const d = new Date(dateStr);
  const now = new Date();
  return Math.floor((now - d) / (1000 * 60 * 60 * 24));
}

function percentile(sortedArr, p) {
  const idx = Math.floor((p / 100) * sortedArr.length);
  return sortedArr[Math.min(idx, sortedArr.length - 1)];
}

// ─────────────────────────────────────────
// Step 1: 블로그 포스팅 수 조회 + 성장률 계산
// ─────────────────────────────────────────
async function getBlogGrowth(keywords) {
  const now = new Date().toISOString();
  const results = [];

  await Promise.all(keywords.map(async (kw) => {
    try {
      const res = await fetch(
        `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(kw)}&display=1`,
        {
          headers: {
            'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
            'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
          },
        }
      );
      const data = await res.json();
      const currentCount = (data.total && data.total > 0) ? data.total : null;

      // Redis에서 이전 포스팅 수 히스토리 조회
      let growthHistory = [];
      try {
        const stored = await redis.get(`blog_growth:${kw}`);
        if (stored) growthHistory = typeof stored === 'string' ? JSON.parse(stored) : stored;
      } catch(e) {}

      // 48시간 이내 기록만 유지
      const cutoff = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();
      growthHistory = growthHistory.filter(h => h.timestamp > cutoff);

      // null이 아닐 때만 히스토리에 추가
      if (currentCount !== null) {
        growthHistory.push({ timestamp: now, count: currentCount });
        await redis.set(`blog_growth:${kw}`, JSON.stringify(growthHistory));
      }

      // 블로그 성장률 계산 (최근 1시간 vs 이전 1시간)
      const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
      const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();

      const recentRecords = growthHistory.filter(h => h.timestamp >= oneHourAgo && h.count > 0);
      const prevRecords = growthHistory.filter(h => h.timestamp >= twoHoursAgo && h.timestamp < oneHourAgo && h.count > 0);

      const recentMax = recentRecords.length ? Math.max(...recentRecords.map(h => h.count)) : (currentCount || 0);
      const prevMax = prevRecords.length ? Math.max(...prevRecords.map(h => h.count)) : 0;

      const blogGrowth = (prevMax > 0 && recentMax > 0) ? ((recentMax - prevMax) / prevMax) * 100 : 0;
      const hasEnoughData = growthHistory.length >= 2;

      results.push({
        keyword: kw,
        postCount: currentCount,
        blogGrowth: Math.round(blogGrowth),
        hasEnoughData,
        growthHistoryCount: growthHistory.length,
      });
    } catch(e) {
      results.push({ keyword: kw, postCount: 0, blogGrowth: 0, hasEnoughData: false, growthHistoryCount: 0 });
    }
  }));

  return results;
}

// ─────────────────────────────────────────
// Step 2: EMA 스무딩
// ─────────────────────────────────────────
const EMA_ALPHA = 0.5; // 0.3 → 0.5: 하락 반영 속도 향상

async function applyEMA(keyword, currentScore) {
  const key = `ema:${keyword}`;
  try {
    const stored = await redis.get(key);
    const prevEMA = stored ? parseFloat(stored) : currentScore;
    const newEMA = EMA_ALPHA * currentScore + (1 - EMA_ALPHA) * prevEMA;
    await redis.set(key, newEMA.toString());
    return newEMA;
  } catch(e) {
    return currentScore;
  }
}

// ─────────────────────────────────────────
// Step 3: Early Trend 감지
// ─────────────────────────────────────────
function detectEarlyTrend(trends) {
  const indices = trends
    .map(t => avg(t.values.slice(-7)))
    .filter(v => v > 0)
    .sort((a, b) => a - b);

  const p70 = percentile(indices, 70);

  return trends.map(t => {
    const currentIndex = avg(t.values.slice(-7));
    // DataLab 신호: 검색량 급상승 + 아직 중간 이하
    const datalabSignal = t.risingRate >= 50 && currentIndex <= p70 && currentIndex > 0;
    // 블로그 신호: 포스팅 수 급등 (refresh에서 계산된 값)
    const blogSignal = (t.blogSurgeRate || 0) >= 20 && (t.postCount || 0) >= 500;
    // 둘 중 하나라도 있으면 조기감지
    const isEarlyTrend = (datalabSignal || blogSignal) && t.weeklyRate >= -10;

    const novelty = p70 > 0 ? Math.max(0, 1 - (currentIndex / p70)) : 0;
    const earlyScore = isEarlyTrend
      ? (Math.min(t.risingRate / 300, 1)) * 0.5
        + (Math.min(Math.max(t.weeklyRate, 0) / 100, 1)) * 0.2
        + novelty * 0.2
        + (blogSignal ? 0.1 : 0)
      : 0;

    return { ...t, isEarlyTrend, earlyScore: Math.round(earlyScore * 100) };
  });
}

// ─────────────────────────────────────────
// Step 4: 트렌드 단계 분류
// ─────────────────────────────────────────
function classifyTrend(weeklyRate, risingRate, blogGrowth) {
  // 1차: risingRate(최근 3일) 우선
  if (risingRate >= 20) return '유행중';
  if (risingRate <= -20) return '유행지남';

  // 2차: weeklyRate
  if (weeklyRate >= 10) return '유행중';
  if (weeklyRate <= -10) return '유행지남';

  // 3차: blogGrowth 보조
  if (blogGrowth >= 10) return '유행중';

  return risingRate >= 0 ? '유행중' : '유행지남';
}

// ─────────────────────────────────────────
// 메인
// ─────────────────────────────────────────
module.exports = async (req, res) => {
  try {
    // 기존 trend_data에서 DataLab 결과 가져오기
    const stored = await redis.get('trend_data');
    if (!stored) {
      return res.status(200).json({ message: 'trend_data 없음. /api/refresh 먼저 실행 필요' });
    }

    const prevData = typeof stored === 'string' ? JSON.parse(stored) : stored;
    const prevKeywords = prevData.keywords || [];
    if (!prevKeywords.length) {
      return res.status(200).json({ message: '키워드 없음' });
    }

    const keywordList = prevKeywords.map(k => k.keyword);
    console.log(`[rank] 키워드 ${keywordList.length}개 랭킹 재계산 시작`);

    // Step 1: 블로그 포스팅 수 + 성장률 조회
    const blogData = await getBlogGrowth(keywordList);
    const blogMap = Object.fromEntries(blogData.map(b => [b.keyword, b]));
    console.log('[rank] 블로그 성장률 top3:',
      blogData.sort((a,b) => b.blogGrowth - a.blogGrowth).slice(0,3).map(b => `${b.keyword}(${b.blogGrowth}%)`)
    );

    // 기존 DataLab values 복원
    const trendMap = Object.fromEntries(prevKeywords.map(k => [k.keyword, k]));

    // Step 2: 점수 재계산 (EMA 포함)
    const rawTrends = prevKeywords.map(k => ({
      keyword: k.keyword,
      weeklyRate: k.changeRate,
      risingRate: k.risingRate || 0,
      values: k.values || [],
      postCount: blogMap[k.keyword]?.postCount || k.postCount || 0,
      blogGrowth: blogMap[k.keyword]?.blogGrowth || 0,
      blogSurgeRate: k.blogSurgeRate || 0, // refresh에서 계산된 급등률
      blogSurge: k.blogSurge || false,
      category: k.category || '',
    }));

    const maxWeekly = Math.max(...rawTrends.map(t => t.weeklyRate), 1);
    const maxRising = Math.max(...rawTrends.map(t => t.risingRate), 1);
    const maxBlog = Math.max(...rawTrends.map(t => Math.max(t.blogGrowth, 0)), 1);

    // Step 3: Early Trend 감지
    const trendsWithEarly = detectEarlyTrend(rawTrends);

    // Step 4: EMA 적용 + 점수 계산
    const scored = await Promise.all(trendsWithEarly.map(async (t) => {
      const prevK = trendMap[t.keyword];
      const daysInPool = prevK?.isNew ? 3 : 10; // 간소화 (실제 pool 없으므로)
      const newBonus = prevK?.isNew ? 0.05 : 0;

      // 정규화
      const normWeekly = Math.min(Math.max(t.weeklyRate, 0) / maxWeekly, 1);
      const normRising = Math.min(Math.max(t.risingRate, 0) / maxRising, 1);
      const normBlog = Math.min(Math.max(t.blogGrowth, 0) / maxBlog, 1);

      // 점수: weeklyRate 35% + risingRate 25% + blogGrowth 30% + newBonus 10%
      // blogGrowth 비중 상향: 실시간 하락 감지 강화
      const rawScore = normWeekly * 0.35 + normRising * 0.25 + normBlog * 0.30 + newBonus;

      // EMA 스무딩 적용 (메인 랭킹용)
      const emaScore = await applyEMA(t.keyword, rawScore);

      return {
        ...t,
        rawScore,
        emaScore,
        trend: classifyTrend(t.weeklyRate, t.risingRate, t.blogGrowth),
      };
    }));

    // EMA 점수 기준 정렬
    const finalRanked = scored
      .filter(t => avg(t.values.slice(-7)) >= 1.0) // 검색량 최소 기준
      .sort((a, b) => b.emaScore - a.emaScore)
      .slice(0, 20)
      .map((k, i) => ({ ...k, rank: i + 1 }));

    // 이전 순위 비교 (순위 변동 계산)
    const prevRankMap = Object.fromEntries(prevKeywords.map(k => [k.keyword, k.rank]));

    // rising 목록 (risingRate 기준)
    const risingRanked = [...finalRanked]
      .filter(k => k.risingRate > 0)
      .sort((a, b) => b.risingRate - a.risingRate)
      .slice(0, 10);

    // Early Trend 목록
    const earlyTrends = finalRanked
      .filter(k => k.isEarlyTrend)
      .sort((a, b) => b.earlyScore - a.earlyScore)
      .slice(0, 5);

    console.log('[rank] top3:', finalRanked.slice(0, 3).map(k => k.keyword));
    console.log('[rank] earlyTrends:', earlyTrends.map(k => `${k.keyword}(earlyScore:${k.earlyScore})`));
    console.log('[rank] 트렌드 분포:', {
      유행중: finalRanked.filter(k => k.trend === '유행중').length,
      유행지남: finalRanked.filter(k => k.trend === '유행지남').length,
    });

    const rankUpdatedAt = new Date().toISOString();

    const result = {
      updatedAt: prevData.updatedAt,
      rankUpdatedAt,
      collectUpdatedAt: prevData.updatedAt,
      keywords: finalRanked.map((k, i) => ({
        rank: i + 1,
        prevRank: prevRankMap[k.keyword] || null,
        keyword: k.keyword,
        score: Math.round(k.emaScore * 100),
        changeRate: Math.round(k.weeklyRate),
        risingRate: Math.round(k.risingRate),
        blogGrowth: k.blogGrowth,
        hasEnoughData: k.hasEnoughData || false,
        postCount: k.postCount,
        blogSurgeRate: k.blogSurgeRate || 0,
        blogSurge: k.blogSurge || false,
        category: k.category || prevKeywords.find(p => p.keyword === k.keyword)?.category || '',
        trend: k.trend,
        isNew: prevKeywords.find(p => p.keyword === k.keyword)?.isNew || false,
        isEarlyTrend: k.isEarlyTrend,
        earlyScore: k.earlyScore,
        comment: prevKeywords.find(p => p.keyword === k.keyword)?.comment || '',
        values: k.values.slice(-28),
      })),
      rising: risingRanked.map((k, i) => ({
        rank: i + 1,
        keyword: k.keyword,
        risingRate: Math.round(k.risingRate),
        blogGrowth: k.blogGrowth,
        blogSurge: k.blogSurge || false,
        trend: k.trend,
        isEarlyTrend: k.isEarlyTrend,
      })),
      earlyTrends: earlyTrends.map(k => ({
        keyword: k.keyword,
        risingRate: Math.round(k.risingRate),
        blogGrowth: k.blogGrowth,
        blogSurge: k.blogSurge || false,
        earlyScore: k.earlyScore,
        changeRate: Math.round(k.weeklyRate),
      })),
    };

    await redis.set('trend_data', JSON.stringify(result));

    // 히스토리 누적 (최근 72회 = 3일치 매시간)
    let history = [];
    try {
      const raw = await redis.get('trend_history');
      if (raw) history = typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch(e) {}

    history.push({
      timestamp: rankUpdatedAt,
      type: 'rank',
      keywords: finalRanked.slice(0, 10).map(k => ({
        keyword: k.keyword,
        rank: k.rank,
        changeRate: Math.round(k.weeklyRate),
        risingRate: Math.round(k.risingRate),
        blogGrowth: k.blogGrowth,
      })),
    });
    if (history.length > 72) history = history.slice(-72);
    await redis.set('trend_history', JSON.stringify(history));

    res.status(200).json({
      success: true,
      rankUpdatedAt,
      keywordCount: finalRanked.length,
      earlyTrendCount: earlyTrends.length,
    });

  } catch (err) {
    console.error('[rank] 오류:', err);
    res.status(500).json({ error: err.message });
  }
};
