const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// 1단계: Google Trends RSS로 실시간 키워드 수집
async function getTrendingKeywords() {
  try {
    const res = await fetch('https://trends.google.com/trending/rss?geo=KR', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)',
        'Accept': 'application/rss+xml, application/xml, text/xml',
      },
    });
    const text = await res.text();

    // <title> 태그에서 키워드 추출 (첫 번째는 피드 제목이라 제외)
    const matches = [...text.matchAll(/<title>([^<]+)<\/title>/g)];
    const keywords = matches
      .map(m => m[1].trim())
      .filter(t => t !== 'Daily Search Trends' && t.length >= 2)
      .slice(0, 20);

    console.log('[getTrendingKeywords] google trends:', keywords);
    return keywords;
  } catch (e) {
    console.log('[getTrendingKeywords] failed:', e.message);
    return [];
  }
}

// 2단계: 키워드 풀 누적 관리 (KV)
async function updateKeywordPool(newKeywords) {
  let pool = [];
  try {
    const stored = await redis.get('keyword_pool');
    if (stored) pool = JSON.parse(stored);
  } catch (e) {
    console.log('[updateKeywordPool] pool empty, starting fresh');
  }

  // 새 키워드 병합 (중복 제거)
  const merged = [...new Set([...newKeywords, ...pool])];

  // 최대 80개 유지 (오래된 것 자동 제거)
  const trimmed = merged.slice(0, 80);

  await redis.set('keyword_pool', JSON.stringify(trimmed));
  console.log('[updateKeywordPool] pool size:', trimmed.length);
  return trimmed;
}

// 3단계: DataLab 검색량 트렌드 조회
async function getSearchTrends(keywords, mode = 'weekly') {
  const results = [];
  for (let i = 0; i < keywords.length; i += 5) {
    const chunk = keywords.slice(i, i + 5);
    const keywordGroups = chunk.map(kw => ({ groupName: kw, keywords: [kw] }));
    try {
      const res = await fetch('https://openapi.naver.com/v1/datalab/search', {
        method: 'POST',
        headers: {
          'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
          'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          startDate: getDateString(-28),
          endDate: getDateString(0),
          timeUnit: 'date',
          keywordGroups,
        }),
      });
      const data = await res.json();
      if (data.results) {
        for (const result of data.results) {
          const values = result.data.map(d => d.ratio);
          let changeRate;
          if (mode === 'rising') {
            const recent3 = values.slice(-3);
            const prev3 = values.slice(-6, -3);
            changeRate = avg(prev3) > 0 ? ((avg(recent3) - avg(prev3)) / avg(prev3)) * 100 : 0;
          } else {
            const recent7 = values.slice(-7);
            const prev7 = values.slice(-14, -7);
            changeRate = avg(prev7) > 0 ? ((avg(recent7) - avg(prev7)) / avg(prev7)) * 100 : 0;
          }
          results.push({ keyword: result.title, changeRate, values });
        }
      }
    } catch (e) {
      console.log('[getSearchTrends] error chunk', i, e.message);
    }
  }
  return results;
}

// 4단계: 포스팅 수 조회
async function getBlogPostCount(keywords) {
  const results = [];
  for (const kw of keywords) {
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
      if (data.total && data.total > 0) {
        results.push({ keyword: kw, total: data.total });
      } else {
        const res2 = await fetch(
          `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(kw)}&display=10`,
          {
            headers: {
              'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
              'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
            },
          }
        );
        const data2 = await res2.json();
        const total = (data2.total && data2.total > 0)
          ? data2.total
          : (data2.items ? data2.items.length * 1000 : 1000);
        results.push({ keyword: kw, total });
      }
    } catch {
      results.push({ keyword: kw, total: 1000 });
    }
  }
  return results;
}

// 5단계: HyperCLOVA X 코멘트 생성
async function generateComments(topKeywords) {
  const kwList = topKeywords.map((k, i) => `${i}:${k.keyword}`).join(', ');
  try {
    const res = await fetch(
      'https://clovastudio.stream.ntruss.com/testapp/v3/chat-completions/HCX-DASH-002',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${process.env.CLOVA_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          messages: [
            {
              role: 'system',
              content: '아래 번호:키워드 목록에서 각 키워드가 지금 네이버 블로그에서 뜨는 이유를 15자 이내로 설명해. 반드시 JSON 형식으로만 반환: {"0":"이유","1":"이유",...}. 다른 설명 없이 JSON만.',
            },
            { role: 'user', content: kwList },
          ],
          maxTokens: 400,
          temperature: 0.5,
          repetitionPenalty: 1.1,
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '{}';
    return JSON.parse(text.replace(/```json|```/g, '').trim());
  } catch {
    return {};
  }
}

// 유행 3단계 분류
function classifyTrend(changeRate, postCount, medianPostCount) {
  if (changeRate > 15 && postCount < medianPostCount * 1.5) return '유행예감';
  if (changeRate <= 0) return '유행지남';
  return '유행중';
}

function getDateString(daysOffset) {
  const d = new Date();
  d.setDate(d.getDate() + daysOffset);
  return d.toISOString().split('T')[0];
}

function avg(arr) {
  return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
}

function median(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function normalize(values) {
  const max = Math.max(...values);
  return values.map(v => (max > 0 ? v / max : 0));
}

module.exports = async (req, res) => {
  try {
    // 1. Google Trends RSS로 실시간 키워드 수집
    const trendingKeywords = await getTrendingKeywords();
    if (!trendingKeywords.length) throw new Error('Google Trends 키워드 수집 실패');

    // 2. 키워드 풀 누적 업데이트
    const keywordPool = await updateKeywordPool(trendingKeywords);

    // 3. DataLab 검색량 조회 - 7일(메인) + 3일(급상승) 동시
    // DataLab은 한 번에 최대 5개씩, 풀 전체 조회
    const [weeklyTrends, risingTrends] = await Promise.all([
      getSearchTrends(keywordPool, 'weekly'),
      getSearchTrends(keywordPool, 'rising'),
    ]);
    if (!weeklyTrends.length) throw new Error('트렌드 조회 실패');

    // 4. 포스팅 수 조회 (상위 20개)
    const top20keywords = [...weeklyTrends]
      .sort((a, b) => b.changeRate - a.changeRate)
      .slice(0, 20)
      .map(t => t.keyword);
    const postCounts = await getBlogPostCount(top20keywords);

    // 5. 중앙값 계산
    const postValues = postCounts.map(p => p.total);
    const medianPost = median(postValues);

    // 6. 메인 랭킹 스코어 계산 (7일 기준)
    const changeRates = weeklyTrends.map(t => t.changeRate);
    const normalizedRates = normalize(changeRates);
    const postCountMap = Object.fromEntries(postCounts.map(p => [p.keyword, p.total]));
    const maxPost = Math.max(...postValues, 1);

    const ranked = weeklyTrends.map((t, i) => {
      const postCount = postCountMap[t.keyword] || 0;
      const normalizedPost = postCount / maxPost;
      const score = normalizedRates[i] * 0.5 + (t.changeRate > 0 ? 0.3 : 0) + normalizedPost * 0.2;
      const trend = classifyTrend(t.changeRate, postCount, medianPost);
      return { keyword: t.keyword, score, changeRate: t.changeRate, postCount, trend };
    }).sort((a, b) => b.score - a.score).slice(0, 20);

    // 7. 급상승 랭킹 (3일 기준)
    const risingRateMap = Object.fromEntries(risingTrends.map(t => [t.keyword, t.changeRate]));
    const risingRanked = [...ranked]
      .map(k => ({ ...k, risingRate: risingRateMap[k.keyword] || 0 }))
      .filter(k => k.risingRate > 0)
      .sort((a, b) => b.risingRate - a.risingRate)
      .slice(0, 10);

    console.log('[ranked] top3:', ranked.slice(0, 3).map(k => k.keyword));
    console.log('[rising] top3:', risingRanked.slice(0, 3).map(k => `${k.keyword}(${Math.round(k.risingRate)}%)`));
    console.log('[ranked] trend distribution:', {
      유행예감: ranked.filter(k => k.trend === '유행예감').length,
      유행중: ranked.filter(k => k.trend === '유행중').length,
      유행지남: ranked.filter(k => k.trend === '유행지남').length,
    });

    // 8. 코멘트 생성
    const commentsRaw = await generateComments(ranked.slice(0, 10));
    const comments = ranked.slice(0, 10).map((_, i) => commentsRaw[String(i)] || '');

    // 9. KV 저장
    const result = {
      updatedAt: new Date().toISOString(),
      keywords: ranked.map((k, i) => ({
        rank: i + 1,
        keyword: k.keyword,
        score: Math.round(k.score * 100),
        changeRate: Math.round(k.changeRate),
        postCount: k.postCount,
        trend: k.trend,
        comment: comments[i] || '',
      })),
      rising: risingRanked.map((k, i) => ({
        rank: i + 1,
        keyword: k.keyword,
        risingRate: Math.round(k.risingRate),
        postCount: k.postCount,
        trend: k.trend,
      })),
    };

    await redis.set('trend_data', JSON.stringify(result));
    res.status(200).json({ success: true, updatedAt: result.updatedAt, poolSize: keywordPool.length });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
