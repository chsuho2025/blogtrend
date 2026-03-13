const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// ─────────────────────────────────────────
// 그물 키워드 38개
// ─────────────────────────────────────────
const NET_KEYWORDS = [
  // 트렌드성
  '신상', '요즘', '핫한', '뜨는', '화제', '인기', '난리', '대세',
  // 행동성
  '후기', '추천', '꿀팁', '레시피', '챌린지', '리뷰', '사용기', '솔직후기',
  // 발견성
  '득템', '하울', '언박싱', '추천템',
  // 계절/시기성
  '봄신상', '한정판', '화이트데이', '봄',
  // 라이프스타일성
  '갓생', '무지출', '루틴', '홈카페', '홈트', '자취',
  // 경제성
  '짠테크', '앱테크',
];

// ─────────────────────────────────────────
// 7명 블로거 역할
// ─────────────────────────────────────────
const BLOGGERS = {
  패션뷰티: '너는 패션·뷰티 전문 블로거야. 최신 뷰티 아이템, 코디 트렌드, 신상 제품에 민감해. 뷰티, 메이크업, 스킨케어, 패션, 코디, 향수 관련 트렌드를 잘 알아.',
  여행맛집: '너는 맛집·카페·여행 전문 블로거야. 요즘 뜨는 디저트, 신상 음료, 편의점 신상, 핫플레이스에 민감해. 먹거리, 카페, 여행지 관련 트렌드를 잘 알아.',
  리빙푸드: '너는 라이프스타일·인테리어·요리 전문 블로거야. 자취 꿀템, 홈카페, 살림 트렌드, 요리 레시피에 민감해. 생활용품, 인테리어, 요리 관련 트렌드를 잘 알아.',
  카테크: '너는 IT·가전·테크 전문 블로거야. 신상 전자기기, 가성비 가전, 앱 트렌드에 민감해. 스마트폰, 노트북, 가전제품 관련 트렌드를 잘 알아.',
  지식: '너는 건강·자기계발 전문 블로거야. 요즘 운동 루틴, 다이어트 트렌드, 영양제, 생활 꿀팁에 민감해. 건강, 운동, 식단, 자기계발 관련 트렌드를 잘 알아.',
  경제: '너는 재테크·절약 전문 블로거야. 무지출 챌린지, 짠테크, 앱테크, 청년 정책에 민감해. 절약, 투자, 재테크 관련 트렌드를 잘 알아.',
  트렌드: '너는 카테고리 경계 없이 트렌드 전반에 민감한 블로거야. 지금 SNS에서 가장 화제가 되고 있는 것, 세대를 막론하고 뜨는 것, 유행어나 챌린지 같은 것에 민감해.',
};

// ─────────────────────────────────────────
// Step 1: 그물 키워드로 블로그 제목 수집
// ─────────────────────────────────────────
async function collectBlogTitles() {
  const allTitles = [];
  for (const keyword of NET_KEYWORDS) {
    try {
      const url = `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(keyword)}&display=50&sort=date`;
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
          if (clean.length >= 2) allTitles.push(clean);
        }
      }
    } catch (e) {
      console.log(`[collectBlogTitles] 실패: ${keyword}`, e.message);
    }
  }
  // 광고/지역 블로그 필터링
  const NOISE_PATTERNS = [
    /\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}/, // 전화번호
    /(구|동|읍|면|리)\s*(맛집|미용실|헬스|학원|병원|치과|부동산|공인중개|임대|분양|공장|창고|세탁|네일|피부|한의원|안과|정형외과|노무사|변호사|회계사)/, // 지역+업종
    /(원룸|투룸|오피스텔|상가|사무실)\s*(임대|분양|매매)/, // 부동산
    /\d+평\s*(임대|분양|매매)/, // 부동산 평수
  ];

  const filtered = allTitles.filter(title => {
    return !NOISE_PATTERNS.some(pattern => pattern.test(title));
  });

  // 중복 제거
  const unique = [...new Set(filtered)];
  console.log(`[collectBlogTitles] 총 ${unique.length}개 수집 (원본: ${allTitles.length}개, 필터후: ${filtered.length}개)`);
  return unique;
}

// ─────────────────────────────────────────
// Step 2: HyperCLOVA X — 제목에서 트렌드 키워드 50개 추출
// 400개씩 4덩어리로 나눠서 각각 뽑고 합침
// ─────────────────────────────────────────
async function extractTrendKeywords(titles) {
  const CHUNK_SIZE = 400;
  const allKeywords = [];

  for (let i = 0; i < Math.min(titles.length, 1600); i += CHUNK_SIZE) {
    const chunk = titles.slice(i, i + CHUNK_SIZE);
    const titleText = chunk.join('\n');
    try {
      const res = await fetch(
        'https://clovastudio.stream.ntruss.com/v3/chat-completions/HCX-007',
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${process.env.CLOVA_API_KEY}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
          },
          body: JSON.stringify({
            messages: [
              {
                role: 'system',
                content: `너는 네이버 블로그 트렌드 분석가야.
아래는 최근 네이버 블로그 제목 목록이야.
이 제목들에서 지금 실제로 유행하고 있는 구체적인 트렌드 키워드 15개를 뽑아줘.

선택 기준:
- 구체적인 아이템명, 음식명, 제품명 (예: 상하이버터떡, 연세우유 황치즈크림빵)
- 요즘 SNS에서 유행하는 챌린지, 트렌드어 (예: 갓생루틴, 무지출챌린지)
- 전국적으로 유행하는 것 (특정 지역 한정 아닌 것)
- 제목에 2번 이상 등장하는 키워드 우선

반드시 제외:
- 간식, 샴푸, 노래, 도시락, 운동, 쇼핑, 음식, 청소, 요리, 패션 같은 카테고리 단어
- 맛집, 카페, 추천, 후기, 리뷰, 정리, 오늘, 진짜, 완전, 정말, 좋아요 같은 일반 단어
- 날짜, 연도 (3월, 2026 등)
- 지역명+업종 조합 (예: 학익동 맛집, 김포 공장, 광주 미용실)
- 지역명 단독 (서울, 부산, 인천 등)
- 특정 상호명, 업체명 (예: 비아스튜디오, 골드앤무드)
- 사람 이름, 뉴스성 키워드, 사건사고
- 브랜드명만 있고 구체적 제품명 없는 것 (예: 다이소, 이마트 단독은 제외, 다이소 추천템은 가능)

반드시 JSON 배열로만: ["키워드1","키워드2",...]
다른 설명 없이 JSON만.`,
              },
              {
                role: 'user',
                content: titleText,
              },
            ],
            maxCompletionTokens: 1000,
            temperature: 0.3,
            repetitionPenalty: 1.1,
            thinking: { effort: "none" },
          }),
        }
      );
      const data = await res.json();
      console.log(`[extractTrendKeywords] chunk${Math.floor(i/CHUNK_SIZE)+1} 응답:`, JSON.stringify(data).slice(0, 400));
      const text = data.result?.message?.content || data.choices?.[0]?.message?.content || '[]';
      const cleaned = text.replace(/```json|```/g, '').trim();
      const keywords = JSON.parse(cleaned);
      console.log(`[extractTrendKeywords] chunk${Math.floor(i/CHUNK_SIZE)+1}: ${keywords.length}개 →`, keywords);
      allKeywords.push(...keywords);
    } catch (e) {
      console.log(`[extractTrendKeywords] chunk${Math.floor(i/CHUNK_SIZE)+1} 실패:`, e.message);

    }
  }

  // 중복 제거
  const unique = [...new Set(allKeywords)];
  console.log(`[extractTrendKeywords] 전체 ${unique.length}개 추출`);
  return unique;
}

// ─────────────────────────────────────────
// Step 4: 7명 블로거 — 카테고리별 트렌드 픽
// ─────────────────────────────────────────
async function bloggerPick(keywords) {
  const kwList = keywords.join(', ');
  const allPicked = [];

  for (const [blogger, role] of Object.entries(BLOGGERS)) {
    try {
      const res = await fetch(
        'https://clovastudio.stream.ntruss.com/v3/chat-completions/HCX-007',
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${process.env.CLOVA_API_KEY}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
          },
          body: JSON.stringify({
            messages: [
              {
                role: 'system',
                content: `${role}
아래 키워드 목록에서 네가 "지금 당장 포스팅하고 싶다"고 느끼는 트렌드 키워드를 5~8개 골라줘.

선택 기준:
- 지금 네 독자들이 관심 가질 만한 것
- 너무 광범위하지 않고 구체적인 것
- 지금 막 뜨기 시작한 느낌

절대 금지:
- 목록에 없는 키워드를 새로 만들거나 변형하지 마
- 목록에 있는 키워드를 그대로만 골라줘
- 지역명+업종 조합은 선택하지 마

반드시 JSON 배열로만: ["키워드1","키워드2",...]
다른 설명 없이 JSON만.`,
              },
              {
                role: 'user',
                content: kwList,
              },
            ],
            maxCompletionTokens: 500,
            temperature: 0.5,
            repetitionPenalty: 1.1,
            thinking: { effort: "none" },
          }),
        }
      );
      const data = await res.json();
      const text = data.result?.message?.content || '[]';
      const picked = JSON.parse(text.replace(/```json|```/g, '').trim());
      console.log(`[bloggerPick] ${blogger}: ${picked.length}개 →`, picked);
      allPicked.push(...picked);
    } catch (e) {
      console.log(`[bloggerPick] ${blogger} 실패:`, e.message);
    }
  }

  // 중복 제거 (여러 블로거가 같은 키워드 픽하면 그만큼 신뢰도 높음)
  const unique = [...new Set(allPicked)];
  console.log(`[bloggerPick] 전체 픽 ${allPicked.length}개 → 중복제거 후 ${unique.length}개`);
  return unique;
}

// ─────────────────────────────────────────
// Step 5: 픽된 키워드 빈도 재검증
// ─────────────────────────────────────────
function verifyFrequency(pickedKeywords, titles) {
  // 공백 제거 버전으로 비교 (상하이버터떡 vs 상하이 버터떡 동일 취급)
  const normalizeStr = s => s.replace(/\s+/g, '').toLowerCase();
  const normalizedTitles = titles.map(normalizeStr);

  const verified = [];
  const seenNorm = new Set();
  for (const kw of pickedKeywords) {
    const normKw = normalizeStr(kw);
    if (seenNorm.has(normKw)) continue; // 띄어쓰기만 다른 중복 제거
    seenNorm.add(normKw);
    const count = normalizedTitles.filter(t => t.includes(normKw)).length;
    verified.push({ keyword: kw, titleCount: count });
  }
  // 제목에 1번이라도 나온 것만 유지
  const filtered = verified.filter(k => k.titleCount >= 1);
  filtered.sort((a, b) => b.titleCount - a.titleCount);
  console.log(`[verifyFrequency] ${pickedKeywords.length}개 → 검증 후 ${filtered.length}개`);
  console.log('[verifyFrequency] 생존:', filtered.map(k => `${k.keyword}(${k.titleCount})`));
  return filtered.map(k => k.keyword);
}


// ─────────────────────────────────────────
// Step 6 (구 3단계): 키워드 풀 누적 관리
// 0.2에서는 오래된 키워드 교체 로직 추가
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

  // 새 키워드 우선, 공백제거 기준 중복 제거 후 뒤에 붙임
  const norm = s => s.replace(/\s+/g, '').toLowerCase();
  const newNorms = new Set(newKeywords.map(norm));
  const oldPool = pool.filter(k => !newNorms.has(norm(k)));
  const merged = [...newKeywords, ...oldPool].slice(0, 100);

  await redis.set('keyword_pool', JSON.stringify(merged));
  console.log(`[updateKeywordPool] pool 크기: ${merged.length}`);
  return merged;
}

// ─────────────────────────────────────────
// Step 7: DataLab 검색량 조회
// ─────────────────────────────────────────
async function getSearchTrends(keywords) {
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
          results.push({ keyword: result.title, weeklyRate, risingRate, values });
        }
      }
    } catch (e) {
      console.log('[getSearchTrends] chunk 오류', i, e.message);
    }
  }
  return results;
}

// ─────────────────────────────────────────
// Step 8: 포스팅 수 조회
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
      const total = (data.total && data.total > 0) ? data.total : 1000;
      results.push({ keyword: kw, total });
    } catch {
      results.push({ keyword: kw, total: 1000 });
    }
  }
  return results;
}

// ─────────────────────────────────────────
// Step 9: 코멘트 생성
// ─────────────────────────────────────────
async function generateComments(topKeywords) {
  const kwList = topKeywords.map((k, i) => `${i}:${k.keyword}`).join(', ');
  try {
    const res = await fetch(
      'https://clovastudio.stream.ntruss.com/v3/chat-completions/HCX-007',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${process.env.CLOVA_API_KEY}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify({
          messages: [
            {
              role: 'system',
              content: '아래 번호:키워드 목록에서 각 키워드가 지금 네이버 블로그에서 뜨는 이유를 15자 이내로 설명해. 반드시 JSON 형식으로만 반환: {"0":"이유","1":"이유",...}. 다른 설명 없이 JSON만.',
            },
            { role: 'user', content: kwList },
          ],
          maxCompletionTokens: 800,
          temperature: 0.5,
          repetitionPenalty: 1.1,
          thinking: { effort: "none" },
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
  if (changeRate >= 30 && postCount < medianPostCount) return '유행예감';
  if (changeRate > 0) return '유행중';
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

function normKw(kw) {
  if (typeof kw !== 'string') return '';
  return kw.replace(/(레시피|추천|후기|방법|효능|사용법|퍼퓸|프리미엄|정품|만들기|하는법)/g, '').replace(/\s+/g, '').trim();
}

// ─────────────────────────────────────────
// 메인
// ─────────────────────────────────────────
module.exports = async (req, res) => {
  try {
    // Step 1: 그물 키워드로 블로그 제목 수집
    const allTitles = await collectBlogTitles();
    if (!allTitles.length) throw new Error('블로그 제목 수집 실패');

    // Step 2: HyperCLOVA X로 트렌드 키워드 50개 추출
    const refined = await extractTrendKeywords(allTitles);
    if (!refined.length) throw new Error('키워드 추출 실패');

    // Step 3: 7명 블로거 픽
    const picked = await bloggerPick(refined);

    // Step 5: 빈도 재검증
    const verified = verifyFrequency(picked, allTitles);

    // Step 6: 키워드 풀 누적
    const keywordPool = await updateKeywordPool(verified);
    if (!keywordPool.length) throw new Error('키워드 풀 없음');

    // Step 7: DataLab 검색량 조회 (최신 40개)
    const queryKeywords = keywordPool.slice(0, 40);
    const rawTrends = await getSearchTrends(queryKeywords);
    if (!rawTrends.length) throw new Error('트렌드 조회 실패');

    // Step 8: 포스팅 수 조회 (상위 20개)
    const top20 = [...rawTrends].sort((a, b) => b.weeklyRate - a.weeklyRate).slice(0, 20).map(t => t.keyword);
    const postCounts = await getBlogPostCount(top20);

    // 랭킹 계산
    const postValues = postCounts.map(p => p.total);
    const medianPost = median(postValues);
    const postCountMap = Object.fromEntries(postCounts.map(p => [p.keyword, p.total]));
    const maxPost = Math.max(...postValues, 1);
    const weeklyRates = rawTrends.map(t => t.weeklyRate);
    const maxRate = Math.max(...weeklyRates, 1);

    const ranked = rawTrends.map(t => {
      const postCount = postCountMap[t.keyword] || 0;
      const normalizedRate = t.weeklyRate / maxRate;
      const normalizedPost = postCount / maxPost;
      const score = normalizedRate * 0.75 + normalizedPost * 0.1 + (t.weeklyRate > 50 ? 0.15 : t.weeklyRate > 10 ? 0.08 : 0);
      const trend = classifyTrend(t.weeklyRate, postCount, medianPost);
      return { keyword: t.keyword, score, changeRate: t.weeklyRate, risingRate: t.risingRate, postCount, trend, values: t.values };
    })
    .filter(t => t.postCount < 500000)
    .sort((a, b) => b.score - a.score);

    // 중복 제거
    const sortedForDedup = [...ranked].sort((a, b) => {
      const aBase = normKw(a.keyword);
      const bBase = normKw(b.keyword);
      if (aBase.length !== bBase.length) return aBase.length - bBase.length;
      return b.score - a.score;
    });
    const deduped = [];
    for (const item of sortedForDedup) {
      const normItem = normKw(item.keyword);
      if (normItem.length < 2) { deduped.push(item); continue; }
      const isDup = deduped.some(d => {
        const normD = normKw(d.keyword);
        if (normD.length < 2) return false;
        return normItem.includes(normD) || normD.includes(normItem);
      });
      if (!isDup) deduped.push(item);
    }
    const finalRanked = deduped.sort((a, b) => b.score - a.score).slice(0, 20).map((k, i) => ({ ...k, rank: i + 1 }));

    // 급상승
    const risingRanked = [...finalRanked]
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

    // Step 9: 코멘트 생성
    const commentsRaw = await generateComments(finalRanked.slice(0, 10));
    const comments = finalRanked.slice(0, 10).map((_, i) => commentsRaw[String(i)] || '');

    // KV 저장
    const result = {
      updatedAt: new Date().toISOString(),
      keywords: finalRanked.map((k, i) => ({
        rank: i + 1,
        keyword: k.keyword,
        score: Math.round(k.score * 100),
        changeRate: Math.round(k.changeRate),
        postCount: k.postCount,
        trend: k.trend,
        comment: comments[i] || '',
        values: k.values.slice(-28),
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
      titlesCollected: allTitles.length,
      verified: verified.length,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
