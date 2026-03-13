const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// ─────────────────────────────────────────
// 그물 키워드
// ─────────────────────────────────────────
const NET_KEYWORDS = [
  '신상', '요즘', '핫한', '뜨는', '화제', '인기', '난리', '대세',
  '후기', '추천', '꿀팁', '레시피', '챌린지', '리뷰', '사용기', '솔직후기',
  '득템', '하울', '언박싱', '추천템',
  '봄신상', '한정판', '화이트데이', '봄',
  '갓생', '무지출', '루틴', '홈카페', '홈트', '자취',
  '짠테크', '앱테크',
];

// 코드 기반 블랙리스트 필터
const BLACKLIST = new Set([
  '간식','샴푸','노래','도시락','운동','쇼핑','음식','청소','요리','패션',
  '화장품','옷','신발','가방','여행','맛집','카페','추천','후기','리뷰',
  '정리','오늘','진짜','완전','정말','좋아요','최고','진심','솔직','꿀팁',
  '방법','하는법','이유','사용법','효능','효과','정보','공유','구매','쇼핑',
  '서울','부산','인천','대구','광주','대전','울산','세종',
]);

// ─────────────────────────────────────────
// Step 1: 그물 키워드로 블로그 제목 수집
// ─────────────────────────────────────────
async function collectBlogTitles() {
  const allTitles = [];
  const seenTitles = new Set();
  const NOISE_PATTERNS = [
    /\d{2,4}-\d{3,4}-\d{4}/,
    /010[-.]?\d{4}[-.]?\d{4}/,
    /(?:서울|부산|인천|대구|광주|대전|울산|수원|성남|고양|용인|창원|청주|전주|천안|안산|안양|남양주|화성|평택|의정부|시흥|파주|김포|광명|광주시|하남|양주|구리|오산|군포|의왕|포천|동두천|가평|여주|이천|안성|양평)[가-힣\s]{1,10}(?:맛집|카페|헬스|병원|학원|부동산|공인중개|인테리어|치과|피부과|한의원|미용실|네일|네일샵|분양|아파트|오피스텔)/,
  ];

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
          const title = item.title.replace(/<[^>]+>/g, '').trim();
          if (!seenTitles.has(title)) {
            seenTitles.add(title);
            allTitles.push(title);
          }
        }
      }
    } catch (e) {
      console.log(`[collectBlogTitles] ${keyword} 오류:`, e.message);
    }
  }

  const originalCount = allTitles.length;
  const filtered = allTitles.filter(t => !NOISE_PATTERNS.some(p => p.test(t)));
  console.log(`[collectBlogTitles] 총 ${filtered.length}개 수집 (원본: ${originalCount}개, 필터후: ${filtered.length}개)`);
  return filtered;
}

// ─────────────────────────────────────────
// Step 2: HyperCLOVA X 키워드 추출
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
- 간식, 샴푸, 노래, 도시락, 운동, 쇼핑, 패션, 음식, 청소, 요리 같은 카테고리 단어
- 맛집, 카페, 추천, 후기, 리뷰, 정리, 오늘, 진짜, 완전, 정말 같은 일반 단어
- 날짜, 연도 (3월, 2026 등)
- 지역명+업종 조합 (예: 학익동 맛집, 광주 미용실)
- 지역명 단독 (서울, 부산 등)
- 특정 상호명, 업체명
- 사람 이름, 뉴스성 키워드, 사건사고
- 20자 초과 키워드 (제목 그대로 복붙 금지)

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
            thinking: { effort: 'none' },
          }),
        }
      );
      const data = await res.json();
      const text = data.result?.message?.content || '[]';
      const cleaned = text.replace(/```json|```/g, '').trim();
      const keywords = JSON.parse(cleaned);
      console.log(`[extractTrendKeywords] chunk${Math.floor(i / CHUNK_SIZE) + 1}: ${keywords.length}개 →`, keywords);
      allKeywords.push(...keywords);
    } catch (e) {
      console.log(`[extractTrendKeywords] chunk${Math.floor(i / CHUNK_SIZE) + 1} 실패:`, e.message);
    }
  }

  // 코드 기반 필터: 길이, 블랙리스트, 특수문자
  const norm = s => s.replace(/\s+/g, '').toLowerCase();
  const seenNorm = new Set();
  const filtered = allKeywords.filter(kw => {
    if (typeof kw !== 'string') return false;
    if (kw.length > 20) return false;                          // 너무 긴 것 (제목 복붙)
    if (kw.length < 2) return false;                           // 너무 짧은 것
    if (/[\[\]【】()（）]/.test(kw)) return false;             // 특수문자 포함
    if (BLACKLIST.has(kw.replace(/\s+/g, ''))) return false;  // 블랙리스트
    const n = norm(kw);
    if (seenNorm.has(n)) return false;                         // 띄어쓰기 중복
    seenNorm.add(n);
    return true;
  });

  console.log(`[extractTrendKeywords] 전체 ${allKeywords.length}개 추출 → 필터후 ${filtered.length}개`);
  return filtered;
}

// ─────────────────────────────────────────
// Step 3: verifyFrequency — 실제 제목 존재 검증
// ─────────────────────────────────────────
function verifyFrequency(keywords, titles) {
  const norm = s => s.replace(/\s+/g, '').toLowerCase();
  const normalizedTitles = titles.map(norm);

  const verified = [];
  const seenNorm = new Set();

  for (const kw of keywords) {
    const n = norm(kw);
    if (seenNorm.has(n)) continue;
    seenNorm.add(n);
    const count = normalizedTitles.filter(t => t.includes(n)).length;
    if (count >= 1) verified.push({ keyword: kw, titleCount: count });
  }

  verified.sort((a, b) => b.titleCount - a.titleCount);
  console.log(`[verifyFrequency] ${keywords.length}개 → 검증 후 ${verified.length}개`);
  console.log('[verifyFrequency] 생존:', verified.map(k => `${k.keyword}(${k.titleCount})`));
  return verified.map(k => k.keyword);
}

// ─────────────────────────────────────────
// Step 4: 키워드 풀 누적 (진입일 기록)
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
    console.log('[updateKeywordPool] pool 로드 실패:', e.message);
  }

  const today = getDateString(0);
  const norm = s => s.replace(/\s+/g, '').toLowerCase();

  // 기존 풀은 { keyword, addedAt } 형태로 저장
  // 하위 호환: string이면 변환
  const poolNormalized = pool.map(item =>
    typeof item === 'string' ? { keyword: item, addedAt: '2026-01-01' } : item
  );

  const existingNorms = new Set(poolNormalized.map(item => norm(item.keyword)));

  // 신규 키워드만 앞에 추가
  const newEntries = newKeywords
    .filter(kw => !existingNorms.has(norm(kw)))
    .map(kw => ({ keyword: kw, addedAt: today }));

  const merged = [...newEntries, ...poolNormalized].slice(0, 100);

  await redis.set('keyword_pool', JSON.stringify(merged));
  console.log(`[updateKeywordPool] pool 크기: ${merged.length} (신규: ${newEntries.length}개)`);
  return merged;
}

// ─────────────────────────────────────────
// Step 5: DataLab 검색량 조회
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
// Step 6: 포스팅 수 조회
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
// Step 7: 키워드 정제 (사용자 노출용)
// ─────────────────────────────────────────
async function polishKeywords(keywords) {
  const kwList = keywords.map((k, i) => `${i}:${k}`).join('\n');
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
              content: `아래는 네이버 블로그 트렌드 키워드 목록이야.
각 키워드를 사용자가 보기 편하게 다듬어줘.

규칙:
- 한국어 맞춤법에 맞게 띄어쓰기 교정 (예: 갓생루틴 → 갓생 루틴, 무지출챌린지 → 무지출 챌린지)
- 브랜드+제품 조합은 자연스러운 띄어쓰기 (예: 다이슨에어스트레이트너 → 다이슨 에어스트레이트너)
- 앞뒤 불필요한 특수문자 제거
- 의미가 명확하도록 너무 붙어있는 단어는 띄워줘
- 단, 원래 뜻이나 고유명사는 절대 바꾸지 마
- 이미 자연스러운 것은 그대로 유지

반드시 JSON으로만: {"0":"정제된키워드","1":"정제된키워드",...}
번호는 입력과 동일하게. 다른 설명 없이 JSON만.`,
            },
            { role: 'user', content: kwList },
          ],
          maxCompletionTokens: 800,
          temperature: 0.1,
          repetitionPenalty: 1.0,
          thinking: { effort: 'none' },
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '{}';
    const polished = JSON.parse(text.replace(/```json|```/g, '').trim());
    // 결과 적용 (실패한 인덱스는 원본 유지)
    return keywords.map((kw, i) => polished[String(i)] || kw);
  } catch (e) {
    console.log('[polishKeywords] 실패, 원본 유지:', e.message);
    return keywords;
  }
}

// ─────────────────────────────────────────
// Step 8: 코멘트 생성
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
          thinking: { effort: 'none' },
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

function daysDiff(dateStr) {
  const d = new Date(dateStr);
  const now = new Date();
  return Math.floor((now - d) / (1000 * 60 * 60 * 24));
}

// ─────────────────────────────────────────
// 메인
// ─────────────────────────────────────────
module.exports = async (req, res) => {
  try {
    // Step 1: 그물 키워드로 블로그 제목 수집
    const allTitles = await collectBlogTitles();
    if (!allTitles.length) throw new Error('블로그 제목 수집 실패');

    // Step 2: HyperCLOVA X 키워드 추출 + 코드 필터
    const refined = await extractTrendKeywords(allTitles);
    if (!refined.length) throw new Error('키워드 추출 실패');

    // Step 3: 실제 제목 존재 검증
    const verified = verifyFrequency(refined, allTitles);

    // Step 4: 키워드 풀 누적 (진입일 기록)
    const keywordPool = await updateKeywordPool(verified);
    if (!keywordPool.length) throw new Error('키워드 풀 없음');

    // Step 5: DataLab 검색량 조회 (최신 40개)
    const queryKeywords = keywordPool.slice(0, 40).map(item =>
      typeof item === 'string' ? item : item.keyword
    );
    const rawTrends = await getSearchTrends(queryKeywords);
    if (!rawTrends.length) throw new Error('트렌드 조회 실패');

    // Step 6: 포스팅 수 조회 (상위 20개)
    const top20 = [...rawTrends].sort((a, b) => b.weeklyRate - a.weeklyRate).slice(0, 20).map(t => t.keyword);
    const postCounts = await getBlogPostCount(top20);

    // 랭킹 계산 — 신규 진입 보너스 포함
    const postValues = postCounts.map(p => p.total);
    const medianPost = median(postValues);
    const postCountMap = Object.fromEntries(postCounts.map(p => [p.keyword, p.total]));
    const maxPost = Math.max(...postValues, 1);
    const weeklyRates = rawTrends.map(t => t.weeklyRate);
    const maxRate = Math.max(...weeklyRates, 1);

    // 키워드별 진입일 맵
    const addedAtMap = Object.fromEntries(
      keywordPool.map(item =>
        typeof item === 'string'
          ? [item, '2026-01-01']
          : [item.keyword, item.addedAt || '2026-01-01']
      )
    );

    const ranked = rawTrends.map(t => {
      const postCount = postCountMap[t.keyword] || 0;
      const normalizedRate = t.weeklyRate / maxRate;
      const normalizedPost = postCount / maxPost;

      // 신규 진입 보너스: 7일 이내 진입 키워드에 +0.15
      const daysInPool = daysDiff(addedAtMap[t.keyword] || '2026-01-01');
      const newBonus = daysInPool <= 7 ? 0.15 : 0;

      const score = normalizedRate * 0.6 + normalizedPost * 0.1
        + (t.weeklyRate > 50 ? 0.15 : t.weeklyRate > 10 ? 0.08 : 0)
        + newBonus;

      const trend = classifyTrend(t.weeklyRate, postCount, medianPost);
      return {
        keyword: t.keyword,
        score,
        changeRate: t.weeklyRate,
        risingRate: t.risingRate,
        postCount,
        trend,
        values: t.values,
        isNew: daysInPool <= 7,
      };
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
    console.log('[신규 키워드]', finalRanked.filter(k => k.isNew).map(k => k.keyword));

    // Step 7: 키워드 정제 (사용자 노출용 띄어쓰기/표기 정규화)
    const rawKeywordNames = finalRanked.map(k => k.keyword);
    const polishedNames = await polishKeywords(rawKeywordNames);
    console.log('[polishKeywords] 정제 결과:', polishedNames.slice(0, 5));
    finalRanked.forEach((k, i) => { k.keyword = polishedNames[i]; });

    // Step 8: 코멘트 생성
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
        isNew: k.isNew,
        comment: comments[i] || '',
        values: k.values.slice(-28),
      })),
      rising: risingRanked.map((k, i) => ({
        rank: i + 1,
        keyword: k.keyword,
        risingRate: Math.round(k.risingRate),
        postCount: k.postCount,
        trend: k.trend,
        isNew: k.isNew,
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
