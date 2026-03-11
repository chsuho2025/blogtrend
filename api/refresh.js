const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// ─────────────────────────────────────────
// 씨드 키워드 (카테고리별)
// 네이버 블로그에서 실제로 검색되는 라이프스타일 표현
// ─────────────────────────────────────────
const SEED_KEYWORDS = [
  // 음식/디저트
  '신상 디저트', '카페 신메뉴', '편의점 신상', '홈베이킹 레시피',
  '브런치 메뉴', '요즘 빵집', '디저트 맛집', '신상 음료',

  // 패션/뷰티
  '올리브영 신상', '다이소 뷰티', '요즘 립', '신상 향수',
  '무신사 추천', '코디 추천', '봄 신상', '뷰티 템',

  // 라이프스타일
  '자취 꿀템', '다이소 신상', '인테리어 소품', '홈카페 꾸미기',
  '살림 꿀팁', '청소 꿀팁', '주방 꿀템', '생활 꿀팁',

  // 건강/운동
  '홈트 루틴', '다이어트 식단', '필라테스 후기', '건강 간식',

  // 여행/나들이
  '주말 나들이', '국내 여행 추천', '서울 핫플', '카페 투어',
];

// ─────────────────────────────────────────
// 1단계: 씨드 키워드로 블로그 최신 제목 수집
// ─────────────────────────────────────────
async function collectBlogTitles() {
  const titles = [];

  for (const seed of SEED_KEYWORDS) {
    try {
      const url = `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(seed)}&display=50&sort=date`;
      const res = await fetch(url, {
        headers: {
          'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
          'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
        },
      });
      const data = await res.json();
      if (data.items) {
        for (const item of data.items) {
          // HTML 태그 제거
          const clean = item.title.replace(/<[^>]+>/g, '').trim();
          if (clean.length >= 2) titles.push(clean);
        }
      }
    } catch (e) {
      console.log(`[collectBlogTitles] 실패: ${seed}`, e.message);
    }
  }

  console.log(`[collectBlogTitles] 총 ${titles.length}개 제목 수집`);
  return titles;
}

// ─────────────────────────────────────────
// 2단계: HyperCLOVA X로 제목에서 트렌드 명사 추출
// ─────────────────────────────────────────
async function extractTrendKeywords(titles) {
  // 500개 샘플링 (토큰 절약)
  const sample = titles.sort(() => 0.5 - Math.random()).slice(0, 500);
  const titleText = sample.join('\n');

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
              content: `아래는 최근 네이버 블로그 제목 목록이야.
이 제목들에서 반복적으로 등장하거나 눈에 띄는 구체적인 트렌드 키워드를 30~50개 추출해줘.

추출 기준:
- 구체적인 음식명/아이템명 (예: 두바이초콜릿, 크림치즈볼, 흑임자라떼)
- SNS/커뮤니티에서 유행하는 표현 (예: 무지출챌린지, 갓생, 미니멀라이프)
- 특정 제품/브랜드 트렌드 (예: 다이소신상, 올리브영핫템)
- 라이프스타일 트렌드 (예: 홈카페, 자취템)

제외 기준:
- 뉴스/사건/인명/정치 관련 단어
- "추천", "후기", "리뷰" 같은 일반 단어
- 너무 광범위한 단어 (예: 맛집, 카페, 여행)

반드시 JSON 배열로만 반환: ["키워드1","키워드2",...]
다른 설명 없이 JSON만.`,
            },
            {
              role: 'user',
              content: titleText,
            },
          ],
          maxTokens: 600,
          temperature: 0.3,
          repetitionPenalty: 1.1,
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '[]';
    const cleaned = text.replace(/```json|```/g, '').trim();
    const keywords = JSON.parse(cleaned);
    console.log(`[extractTrendKeywords] ${keywords.length}개 추출:`, keywords.slice(0, 10));
    return keywords;
  } catch (e) {
    console.log('[extractTrendKeywords] 실패:', e.message);
    return [];
  }
}

// ─────────────────────────────────────────
// 3단계: 키워드 풀 누적 관리
// ─────────────────────────────────────────
async function updateKeywordPool(newKeywords) {
  let pool = [];
  try {
    const stored = await redis.get('keyword_pool');
    if (stored) pool = JSON.parse(stored);
  } catch {
    console.log('[updateKeywordPool] pool 없음, 새로 시작');
  }

  const merged = [...new Set([...newKeywords, ...pool])];
  const trimmed = merged.slice(0, 100);

  await redis.set('keyword_pool', JSON.stringify(trimmed));
  console.log(`[updateKeywordPool] pool 크기: ${trimmed.length}`);
  return trimmed;
}

// ─────────────────────────────────────────
// 4단계: DataLab 검색량 트렌드 조회
// ─────────────────────────────────────────
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
      console.log('[getSearchTrends] chunk 오류', i, e.message);
    }
  }
  return results;
}

// ─────────────────────────────────────────
// 5단계: 포스팅 수 조회
// ─────────────────────────────────────────
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

// ─────────────────────────────────────────
// 6단계: HyperCLOVA X 코멘트 생성
// ─────────────────────────────────────────
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

// ─────────────────────────────────────────
// 유틸
// ─────────────────────────────────────────
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

// ─────────────────────────────────────────
// 메인
// ─────────────────────────────────────────
module.exports = async (req, res) => {
  try {
    // 1. 씨드 키워드로 블로그 최신 제목 수집
    const titles = await collectBlogTitles();
    if (!titles.length) throw new Error('블로그 제목 수집 실패');

    // 2. HyperCLOVA X로 트렌드 키워드 추출
    const extracted = await extractTrendKeywords(titles);

    // 3. 키워드 풀 누적 업데이트
    const keywordPool = await updateKeywordPool(extracted);
    if (!keywordPool.length) throw new Error('키워드 풀 없음');

    // 4. DataLab 검색량 조회 (7일 + 3일 동시)
    const [weeklyTrends, risingTrends] = await Promise.all([
      getSearchTrends(keywordPool, 'weekly'),
      getSearchTrends(keywordPool, 'rising'),
    ]);
    if (!weeklyTrends.length) throw new Error('트렌드 조회 실패');

    // 5. 포스팅 수 조회 (상위 20개)
    const top20keywords = [...weeklyTrends]
      .sort((a, b) => b.changeRate - a.changeRate)
      .slice(0, 20)
      .map(t => t.keyword);
    const postCounts = await getBlogPostCount(top20keywords);

    // 6. 랭킹 스코어 계산
    const postValues = postCounts.map(p => p.total);
    const medianPost = median(postValues);
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

    // 7. 급상승 랭킹
    const risingRateMap = Object.fromEntries(risingTrends.map(t => [t.keyword, t.changeRate]));
    const risingRanked = [...ranked]
      .map(k => ({ ...k, risingRate: risingRateMap[k.keyword] || 0 }))
      .filter(k => k.risingRate > 0)
      .sort((a, b) => b.risingRate - a.risingRate)
      .slice(0, 10);

    console.log('[ranked] top3:', ranked.slice(0, 3).map(k => k.keyword));
    console.log('[rising] top3:', risingRanked.slice(0, 3).map(k => `${k.keyword}(${Math.round(k.risingRate)}%)`));
    console.log('[trend 분포]', {
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
    res.status(200).json({
      success: true,
      updatedAt: result.updatedAt,
      poolSize: keywordPool.length,
      titlesCollected: titles.length,
      keywordsExtracted: extracted.length,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
