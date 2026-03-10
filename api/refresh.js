const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

const CATEGORIES = [
  '여행', '음식', '패션', '뷰티', '육아',
  'IT', '건강', '경제', '문화'
];

async function getBlogTitles() {
  const titles = [];
  for (const category of CATEGORIES) {
    const res = await fetch(
      `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(category)}&display=20&sort=date`,
      {
        headers: {
          'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
          'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
        },
      }
    );
    const data = await res.json();
    if (data.items) {
      titles.push(...data.items.map(item => ({
        title: item.title.replace(/<[^>]*>/g, ''),
        category,
      })));
    }
  }
  return titles;
}

async function extractKeywords(titles) {
  // 카테고리별로 그룹핑해서 균등 추출 유도
  const grouped = {};
  for (const t of titles) {
    if (!grouped[t.category]) grouped[t.category] = [];
    grouped[t.category].push(t.title);
  }
  const categoryText = Object.entries(grouped)
    .map(([cat, ts]) => `[${cat}]\n${ts.slice(0, 10).join('\n')}`)
    .join('\n\n');

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
            content: '아래는 카테고리별 네이버 블로그 제목 목록이야. 각 카테고리에서 최소 3~4개씩 균등하게 핵심 키워드를 추출해서 총 40개를 JSON 배열로만 반환해. 반드시 문자열 배열 형식으로만: ["키워드1","키워드2","키워드3",...]. 객체 형식 사용 금지. 다른 설명이나 마크다운 없이 JSON 배열만.',
          },
          { role: 'user', content: categoryText },
        ],
        maxTokens: 400,
        temperature: 0.3,
        repetitionPenalty: 1.1,
      }),
    }
  );
  const data = await res.json();
  const text = data.result?.message?.content || '[]';
  console.log('[CLOVA 1차] raw:', text.slice(0, 200));
  try {
    const parsed = JSON.parse(text.replace(/```json|```/g, '').trim());
    // CLOVA가 [{키워드: "..."}] 형태로 반환할 경우 문자열 배열로 변환
    return parsed.map(item => {
      if (typeof item === 'string') return item;
      if (typeof item === 'object') return item['키워드'] || item['keyword'] || Object.values(item)[0] || '';
      return '';
    }).filter(Boolean);
  } catch {
    return [];
  }
}

async function getSearchTrends(keywords) {
  const results = [];
  for (let i = 0; i < keywords.length; i += 5) {
    const chunk = keywords.slice(i, i + 5);
    const keywordGroups = chunk.map(kw => ({
      groupName: kw,
      keywords: [kw],
    }));
    const res = await fetch(
      'https://openapi.naver.com/v1/datalab/search',
      {
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
      }
    );
    const data = await res.json();
    console.log('[DataLab] chunk', i, 'response:', JSON.stringify(data).slice(0, 300));
    if (data.results) {
      for (const result of data.results) {
        const values = result.data.map(d => d.ratio);
        const recent7 = values.slice(-7);
        const prev7 = values.slice(-14, -7);
        const recentAvg = avg(recent7);
        const prevAvg = avg(prev7);
        const changeRate = prevAvg > 0 ? ((recentAvg - prevAvg) / prevAvg) * 100 : 0;
        results.push({ keyword: result.title, changeRate, recentAvg });
      }
    }
  }
  return results;
}

async function getBlogPostCount(keywords) {
  // FIX: display=10으로 올리고, total 필드 없으면 items.length로 fallback
  const results = [];
  for (const kw of keywords) {
    try {
      const res = await fetch(
        `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(kw)}&display=10`,
        {
          headers: {
            'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
            'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
          },
        }
      );
      const data = await res.json();
      // total이 유효한 숫자면 사용, 아니면 items.length * 1000 으로 추정
      const total = (data.total && data.total > 0)
        ? data.total
        : (data.items ? data.items.length * 1000 : 0);
      results.push({ keyword: kw, total });
    } catch {
      results.push({ keyword: kw, total: 0 });
    }
  }
  return results;
}

async function generateComments(topKeywords) {
  // FIX: index 기반으로 매칭해서 키워드 이름 불일치 문제 해결
  const kwList = topKeywords.map((k, i) => `${i}:${k.keyword}`).join(', ');
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
  try {
    return JSON.parse(text.replace(/```json|```/g, '').trim());
  } catch {
    return {};
  }
}

function classifyTrend(changeRate, postCount, medianPostCount) {
  // 유행예감: 검색 증가 + 포스팅 적음 (선점 기회)
  if (changeRate > 30 && postCount < medianPostCount) return '유행예감';
  // 유행지남: 검색 감소
  if (changeRate <= 0) return '유행지남';
  // 유행중: 그 외 (검색 증가 + 포스팅 많음, 또는 소폭 증가)
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
    // 1. 블로그 제목 수집
    const titles = await getBlogTitles();

    // 2. 카테고리 균등 키워드 추출
    const keywords = await extractKeywords(titles);
    if (!keywords.length) throw new Error('키워드 추출 실패');

    // 3. 검색량 트렌드 조회
    const trends = await getSearchTrends(keywords);
    if (!trends.length) throw new Error('트렌드 조회 실패');

    // 4. 포스팅 수 조회 (상위 20개)
    const top20keywords = [...trends]
      .sort((a, b) => b.changeRate - a.changeRate)
      .slice(0, 20)
      .map(t => t.keyword);
    const postCounts = await getBlogPostCount(top20keywords);

    // 5. 중앙값 계산 (유행예감 기준)
    const postValues = postCounts.map(p => p.total);
    const medianPost = median(postValues);

    // 6. 랭킹 스코어 계산
    const changeRates = trends.map(t => t.changeRate);
    const normalizedRates = normalize(changeRates);
    const postCountMap = Object.fromEntries(postCounts.map(p => [p.keyword, p.total]));
    const maxPost = Math.max(...postValues, 1);

    const ranked = trends.map((t, i) => {
      const postCount = postCountMap[t.keyword] || 0;
      const normalizedPost = postCount / maxPost;
      const score = normalizedRates[i] * 0.5 + (t.changeRate > 0 ? 0.3 : 0) + normalizedPost * 0.2;
      const trend = classifyTrend(t.changeRate, postCount, medianPost);
      return { keyword: t.keyword, score, changeRate: t.changeRate, postCount, trend };
    }).sort((a, b) => b.score - a.score).slice(0, 20);

    // 7. 코멘트 생성 (index 기반)
    const commentsRaw = await generateComments(ranked.slice(0, 10));
    const comments = ranked.slice(0, 10).map((_, i) => commentsRaw[String(i)] || '');

    // 8. KV 저장
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
    };

    await redis.set('trend_data', JSON.stringify(result));
    res.status(200).json({ success: true, updatedAt: result.updatedAt });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
