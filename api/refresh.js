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
  const titleText = titles.map(t => t.title).join('\n');
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
            content: '아래 블로그 제목 목록에서 핵심 키워드를 추출하고 유사한 것끼리 묶어서 상위 40개 키워드를 JSON 배열로만 반환해. 다른 설명 없이 ["키워드1","키워드2",...] 형식으로만.',
          },
          { role: 'user', content: titleText },
        ],
        maxTokens: 300,
        temperature: 0.3,
        repetitionPenalty: 1.1,
      }),
    }
  );
  const data = await res.json();
  const text = data.result?.message?.content || '[]';
  try {
    return JSON.parse(text.replace(/```json|```/g, '').trim());
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
  const results = [];
  for (const kw of keywords) {
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
    results.push({ keyword: kw, total: data.total || 0 });
  }
  return results;
}

async function generateComments(topKeywords) {
  const kwList = topKeywords.map(k => k.keyword).join(', ');
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
            content: '아래 키워드들이 지금 네이버 블로그에서 트렌딩 중이야. 각 키워드마다 "지금 뜨는 이유"를 한 줄(20자 이내)로 설명해줘. JSON 형식으로만 반환: {"키워드":"이유",...}',
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

function getDateString(daysOffset) {
  const d = new Date();
  d.setDate(d.getDate() + daysOffset);
  return d.toISOString().split('T')[0];
}

function avg(arr) {
  return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
}

function normalize(values) {
  const max = Math.max(...values);
  return values.map(v => (max > 0 ? v / max : 0));
}

module.exports = async (req, res) => {
  try {
    const titles = await getBlogTitles();
    const keywords = await extractKeywords(titles);
    if (!keywords.length) throw new Error('키워드 추출 실패');

    const trends = await getSearchTrends(keywords);

    const top20keywords = trends
      .sort((a, b) => b.changeRate - a.changeRate)
      .slice(0, 20)
      .map(t => t.keyword);
    const postCounts = await getBlogPostCount(top20keywords);

    const changeRates = trends.map(t => t.changeRate);
    const normalizedRates = normalize(changeRates);
    const postCountMap = Object.fromEntries(postCounts.map(p => [p.keyword, p.total]));
    const maxPost = Math.max(...postCounts.map(p => p.total));

    const ranked = trends.map((t, i) => {
      const postCount = postCountMap[t.keyword] || 0;
      const normalizedPost = maxPost > 0 ? postCount / maxPost : 0;
      const score = normalizedRates[i] * 0.5 + (t.changeRate > 0 ? 0.3 : 0) + normalizedPost * 0.2;
      return { keyword: t.keyword, score, changeRate: t.changeRate, postCount };
    }).sort((a, b) => b.score - a.score).slice(0, 20);

    const comments = await generateComments(ranked.slice(0, 10));

    const result = {
      updatedAt: new Date().toISOString(),
      keywords: ranked.map((k, i) => ({
        rank: i + 1,
        keyword: k.keyword,
        score: Math.round(k.score * 100),
        changeRate: Math.round(k.changeRate),
        postCount: k.postCount,
        comment: comments[k.keyword] || '',
      })),
    };

    await redis.set('trend_data', JSON.stringify(result));

    res.status(200).json({ success: true, updatedAt: result.updatedAt });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
