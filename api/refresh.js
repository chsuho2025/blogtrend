const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// ─────────────────────────────────────────
// 씨드 키워드 (카테고리별)
// 네이버 투데이 탭 6개 카테고리 기준
// ─────────────────────────────────────────
const SEED_CATEGORIES = {
  패션뷰티: [
    '올리브영 신상', '다이소 뷰티', '요즘 립', '신상 향수',
    '무신사 추천', '코디 추천', '봄 신상', '뷰티 템',
  ],
  여행맛집: [
    '신상 디저트', '카페 신메뉴', '편의점 신상', '요즘 빵집',
    '디저트 맛집', '신상 음료', '주말 나들이', '서울 핫플', '카페 투어',
  ],
  리빙푸드: [
    '홈베이킹 레시피', '자취 꿀템', '다이소 신상', '인테리어 소품',
    '홈카페 꾸미기', '살림 꿀팁', '청소 꿀팁', '주방 꿀템',
  ],
  카테크: [
    '요즘 가전', '신상 전자기기', '다이소 신상템', '스마트 가전',
  ],
  지식: [
    '홈트 루틴', '다이어트 식단', '필라테스 후기', '건강 간식',
    '자기계발 책', '생활 꿀팁',
  ],
  경제: [
    '재테크 방법', '절약 꿀팁', '무지출 챌린지', '짠테크',
  ],
};

// 카테고리별 블로거 역할 설명
const BLOGGER_ROLES = {
  패션뷰티: '너는 패션·뷰티 전문 블로거야. 최신 뷰티 아이템, 코디 트렌드, 신상 제품에 민감해.',
  여행맛집: '너는 맛집·카페·여행 전문 블로거야. 요즘 뜨는 디저트, 신상 음료, 핫플레이스에 민감해.',
  리빙푸드: '너는 라이프스타일·인테리어·요리 전문 블로거야. 자취 꿀템, 홈카페, 살림 트렌드에 민감해.',
  카테크: '너는 IT·가전·테크 전문 블로거야. 신상 전자기기, 가성비 템, 앱 트렌드에 민감해.',
  지식: '너는 건강·자기계발 전문 블로거야. 요즘 운동 루틴, 다이어트 트렌드, 생활 꿀팁에 민감해.',
  경제: '너는 재테크·절약 전문 블로거야. 무지출 챌린지, 짠테크, 절약 트렌드에 민감해.',
};

// ─────────────────────────────────────────
// 1단계: 카테고리별 블로그 최신 제목 수집
// ─────────────────────────────────────────
async function collectBlogTitlesByCategory() {
  const categoryTitles = {}; // { 패션뷰티: [...], 여행맛집: [...], ... }
  let totalCount = 0;

  for (const [category, seeds] of Object.entries(SEED_CATEGORIES)) {
    categoryTitles[category] = [];
    for (const seed of seeds) {
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
            const clean = item.title.replace(/<[^>]+>/g, '').trim();
            if (clean.length >= 2) categoryTitles[category].push(clean);
          }
        }
      } catch (e) {
        console.log(`[collectBlogTitles] 실패: ${seed}`, e.message);
      }
    }
    console.log(`[collectBlogTitles] ${category}: ${categoryTitles[category].length}개`);
    totalCount += categoryTitles[category].length;
  }

  console.log(`[collectBlogTitles] 총 ${totalCount}개 제목 수집`);
  return categoryTitles;
}

// ─────────────────────────────────────────
// 2단계: 카테고리별 블로거 역할로 키워드 추출
// ─────────────────────────────────────────
async function extractKeywordsFromCategory(category, titles) {
  // 카테고리별 최대 200개 샘플링
  const sample = titles.sort(() => 0.5 - Math.random()).slice(0, 200);
  const titleText = sample.join('\n');
  const role = BLOGGER_ROLES[category];

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
              content: `${role}
아래는 최근 네이버 블로그 제목 목록이야.
네가 다음 포스팅 주제를 고른다면 어떤 키워드를 선택할 것 같아?
제목들에서 지금 당장 포스팅하고 싶은 구체적인 키워드 5~8개만 골라줘.

선택 기준:
- 구체적인 아이템명/음식명 (예: 상하이버터떡, 흑임자라떼, 크림치즈볼)
- 요즘 SNS에서 유행하는 표현 (예: 무지출챌린지, 갓생루틴)
- 방금 막 뜨기 시작한 느낌의 신조어나 신상

제외:
- 뉴스/사건/인명
- 너무 광범위한 단어 (맛집, 카페, 여행, 뷰티 등)
- 씨드 키워드 그대로 (예: "올리브영 신상" 같은 검색어 자체)

반드시 JSON 배열로만 반환: ["키워드1","키워드2",...]
다른 설명 없이 JSON만.`,
            },
            {
              role: 'user',
              content: titleText,
            },
          ],
          maxTokens: 200,
          temperature: 0.4,
          repetitionPenalty: 1.1,
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '[]';
    const cleaned = text.replace(/```json|```/g, '').trim();
    const keywords = JSON.parse(cleaned);
    console.log(`[extractKeywords] ${category}: ${keywords.length}개 →`, keywords);
    return keywords;
  } catch (e) {
    console.log(`[extractKeywords] ${category} 실패:`, e.message);
    return [];
  }
}

async function extractTrendKeywords(categoryTitles) {
  const allKeywords = [];
  for (const [category, titles] of Object.entries(categoryTitles)) {
    if (!titles.length) continue;
    const keywords = await extractKeywordsFromCategory(category, titles);
    allKeywords.push(...keywords);
  }
  // 중복 제거
  const unique = [...new Set(allKeywords)];
  console.log(`[extractTrendKeywords] 전체 ${unique.length}개 추출`);
  return unique;
}

// ─────────────────────────────────────────
// 3단계: 키워드 풀 누적 관리
// ─────────────────────────────────────────
async function updateKeywordPool(newKeywords) {
  let pool = [];
  try {
    const stored = await redis.get('keyword_pool');
    if (stored && stored !== null) {
      pool = typeof stored === 'string' ? JSON.parse(stored) : stored;
    }
    console.log('[updateKeywordPool] 기존 pool 크기:', pool.length);
  } catch (e) {
    console.log('[updateKeywordPool] pool 로드 실패, 새로 시작:', e.message);
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
          const recent7 = values.slice(-7);
          const prev7 = values.slice(-14, -7);
          const weeklyRate = avg(prev7) > 0 ? ((avg(recent7) - avg(prev7)) / avg(prev7)) * 100 : 0;
          const recent3 = values.slice(-3);
          const prev3 = values.slice(-6, -3);
          const risingRate = avg(prev3) > 0 ? ((avg(recent3) - avg(prev3)) / avg(prev3)) * 100 : 0;

          if (mode === 'both') {
            results.push({ keyword: result.title, weeklyRate, risingRate, values });
          } else if (mode === 'rising') {
            results.push({ keyword: result.title, changeRate: risingRate, values });
          } else {
            results.push({ keyword: result.title, changeRate: weeklyRate, values });
          }
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
  // 유행예감: 검색 급증 + 아직 포스팅 적음 (막 뜨기 시작)
  if (changeRate >= 30 && postCount < medianPostCount) return '유행예감';
  // 유행중: 검색 증가 중
  if (changeRate > 0) return '유행중';
  // 유행지남: 변화 없거나 하락
  return '유행지남';
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
    // 1. 카테고리별 블로그 최신 제목 수집
    const categoryTitles = await collectBlogTitlesByCategory();
    const totalTitles = Object.values(categoryTitles).flat().length;
    if (!totalTitles) throw new Error('블로그 제목 수집 실패');

    // 2. 카테고리별 블로거 역할로 키워드 추출
    const extracted = await extractTrendKeywords(categoryTitles);

    // 3. 키워드 풀 누적 업데이트
    const keywordPool = await updateKeywordPool(extracted);
    if (!keywordPool.length) throw new Error('키워드 풀 없음');

    // 4. DataLab 검색량 조회 (최신 40개만, 한 번 호출)
    const queryKeywords = keywordPool.slice(0, 40);
    const rawTrends = await getSearchTrends(queryKeywords, 'both');
    const weeklyTrends = rawTrends.map(t => ({ keyword: t.keyword, changeRate: t.weeklyRate, values: t.values }));
    const risingTrends = rawTrends.map(t => ({ keyword: t.keyword, changeRate: t.risingRate, values: t.values }));
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
    const sortedPostValues = [...postValues].sort((a, b) => a - b);
    // 상위 20% 이상치 제외한 최대값으로 정규화
    const p80 = sortedPostValues[Math.floor(sortedPostValues.length * 0.8)] || 1;
    const maxPost = p80;

    const ranked = weeklyTrends.map((t, i) => {
      const postCount = postCountMap[t.keyword] || 0;
      const normalizedPost = postCount / maxPost;
      const score = normalizedRates[i] * 0.75 + normalizedPost * 0.1 + (t.changeRate > 50 ? 0.15 : t.changeRate > 10 ? 0.08 : 0);
      const trend = classifyTrend(t.changeRate, postCount, medianPost);
      return { keyword: t.keyword, score, changeRate: t.changeRate, postCount, trend };
    })
    .filter(t => t.postCount < 500000) // 포스팅 50만 초과 = 너무 광범위한 키워드 제외
    .sort((a, b) => b.score - a.score);

    // 중복 키워드 제거
    // 전략: 키워드 쌍을 비교해서 한쪽이 다른쪽을 포함하면 점수 높은 것만 남김
    function normalize(kw) {
      return kw.replace(/(레시피|추천|후기|방법|효능|사용법|퍼퓸|프리미엄|정품)/g, '').replace(/\s+/g, '').trim();
    }
    const deduped = [];
    for (const item of ranked) {
      const normItem = normalize(item.keyword);
      const isDup = deduped.some(d => {
        const normD = normalize(d.keyword);
        // 공백 제거 후 한쪽이 다른쪽에 포함되는 경우
        const overlap = normItem.includes(normD) || normD.includes(normItem);
        return overlap;
      });
      if (!isDup) deduped.push(item);
    }
    const finalRanked = deduped.slice(0, 20).map((k, i) => ({ ...k, rank: i + 1 }));

    // 7. 급상승 랭킹
    const risingRateMap = Object.fromEntries(risingTrends.map(t => [t.keyword, t.changeRate]));
    const risingRanked = [...finalRanked]
      .map(k => ({ ...k, risingRate: risingRateMap[k.keyword] || 0 }))
      .filter(k => k.risingRate > 0)
      .sort((a, b) => b.risingRate - a.risingRate)
      .slice(0, 10);

    console.log('[ranked] top3:', finalRanked.slice(0, 3).map(k => k.keyword));
    console.log('[rising] top3:', risingRanked.slice(0, 3).map(k => `${k.keyword}(${Math.round(k.risingRate)}%)`));
    console.log('[trend 분포]', {
      유행예감: finalRanked.filter(k => k.trend === '유행예감').length,
      유행중: finalRanked.filter(k => k.trend === '유행중').length,
      유행지남: finalRanked.filter(k => k.trend === '유행지남').length,
    });

    // 8. 코멘트 생성
    const commentsRaw = await generateComments(finalRanked.slice(0, 10));
    const comments = finalRanked.slice(0, 10).map((_, i) => commentsRaw[String(i)] || '');

    // 9. KV 저장
    const result = {
      updatedAt: new Date().toISOString(),
      keywords: finalRanked.map((k, i) => {
        const trendData = weeklyTrends.find(t => t.keyword === k.keyword);
        return {
          rank: i + 1,
          keyword: k.keyword,
          score: Math.round(k.score * 100),
          changeRate: Math.round(k.changeRate),
          postCount: k.postCount,
          trend: k.trend,
          comment: comments[i] || '',
          values: trendData ? trendData.values.slice(-7) : [],
        };
      }),
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
      titlesCollected: totalTitles,
      keywordsExtracted: extracted.length,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
