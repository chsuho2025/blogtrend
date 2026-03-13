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

// ─────────────────────────────────────────
// Step 1: 그물 키워드로 블로그 제목 수집
// ─────────────────────────────────────────
async function collectBlogTitles() {
  const seenTitles = new Set();
  const allTitles = [];
  let rawCount = 0;

  const NOISE_PATTERNS = [
    /\d{2,4}-\d{3,4}-\d{4}/,
    /010[-.]?\d{4}[-.]?\d{4}/,
    /(?:서울|부산|인천|대구|광주|대전|울산|수원|성남|고양|용인|창원|청주|전주|천안|안산|안양|남양주|화성|평택|의정부|시흥|파주|김포|광명|광주시|하남|양주|구리|오산|군포|의왕|포천|동두천|가평|여주|이천|안성|양평)[가-힣\s]{1,10}(?:맛집|카페|헬스|병원|학원|부동산|공인중개|인테리어|치과|피부과|한의원|미용실|네일|네일샵|분양|아파트|오피스텔)/,
  ];

  const sleep = ms => new Promise(r => setTimeout(r, ms));

  for (const keyword of NET_KEYWORDS) {
    await sleep(100);
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
        rawCount += data.items.length;
        for (const item of data.items) {
          const title = item.title
            .replace(/<[^>]+>/g, '')
            .replace(/&amp;/g, '&')
            .replace(/&quot;/g, '"')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&apos;/g, "'")
            .replace(/&#(\d+);/g, (_, c) => String.fromCharCode(c))
            .trim();
          if (!seenTitles.has(title)) {
            seenTitles.add(title);
            allTitles.push(title);
          }
        }
      } else {
        console.log(`[collectBlogTitles] ${keyword} 빈응답:`, JSON.stringify(data).slice(0, 200));
      }
    } catch (e) {
      console.log(`[collectBlogTitles] ${keyword} 오류:`, e.message);
    }
  }

  const filtered = allTitles.filter(t => !NOISE_PATTERNS.some(p => p.test(t)));
  console.log(`[collectBlogTitles] 총 ${filtered.length}개 수집 (API응답: ${rawCount}개, 중복제거: ${allTitles.length}개, 노이즈제거: ${filtered.length}개)`);
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
이 제목들에서 "지금 이 키워드로 블로그를 쓰면 검색이 될 것 같다"고 판단되는 트렌드 키워드 15개를 뽑아줘.

트렌드 키워드란:
- 요즘 특별히 주목받는 구체적인 제품명, 음식명, 챌린지명 (예: 황치즈칩, 갓생루틴, 무지출챌린지)
- 지금 이 시점에 유독 많이 검색되는 고유한 무언가
- 막연한 카테고리가 아니라, 그 안의 특정 아이템

트렌드 키워드가 아닌 것:
- 언제나 검색되는 카테고리 단어 (예: 운동, 요리, 패션, 여행, 뷰티, 맛집, 추천, 후기)
- 막연한 수식어 (예: 꿀팁, 신상, 할인, 솔직, 내돈내산)
- 지역명 단독 또는 지역+업종 조합
- 날짜, 연도 단독
- 사람 이름, 사건사고, 뉴스성 단어
- 30자 이상 긴 문장

반드시 JSON 배열로만: ["키워드1","키워드2",...]
다른 설명 없이 JSON만.`,
              },
              {
                role: 'user',
                content: chunk.join('\n'),
              },
            ],
            maxCompletionTokens: 2000,
            temperature: 0.3,
            repetitionPenalty: 1.1,
            thinking: { effort: 'none' },
          }),
        }
      );
      const data = await res.json();
      const text = data.result?.message?.content || '[]';
      const keywords = JSON.parse(text.replace(/```json|```/g, '').trim());
      console.log(`[extractTrendKeywords] chunk${Math.floor(i / CHUNK_SIZE) + 1}: ${keywords.length}개 →`, keywords);
      allKeywords.push(...keywords);
    } catch (e) {
      console.log(`[extractTrendKeywords] chunk${Math.floor(i / CHUNK_SIZE) + 1} 실패:`, e.message);
    }
  }

  // 코드 필터: 타입, 최소 길이, 특수문자, 띄어쓰기 중복
  const norm = s => s.replace(/\s+/g, '').toLowerCase();
  const seenNorm = new Set();
  const filtered = allKeywords.filter(kw => {
    if (typeof kw !== 'string') return false;
    if (kw.length < 2) return false;
    if (/[\[\]【】()（）<>《》]/.test(kw)) return false;
    const n = norm(kw);
    if (seenNorm.has(n)) return false;
    seenNorm.add(n);
    return true;
  });

  console.log(`[extractTrendKeywords] 전체 ${allKeywords.length}개 추출 → 필터후 ${filtered.length}개`);
  return filtered;
}

// ─────────────────────────────────────────
// Step 3: 키워드 풀 누적 (진입일 기록)
// ─────────────────────────────────────────
async function updateKeywordPool(newKeywords) {
  let rawPool = [];
  try {
    const stored = await redis.get('keyword_pool');
    if (stored) rawPool = typeof stored === 'string' ? JSON.parse(stored) : stored;
    console.log('[updateKeywordPool] 기존 pool 크기:', rawPool.length);
  } catch (e) {
    console.log('[updateKeywordPool] pool 로드 실패:', e.message);
  }

  const today = getDateString(0);
  const norm = s => s.replace(/\s+/g, '').toLowerCase();

  // 모두 { keyword, addedAt } 형태로 정규화
  const pool = rawPool.map(item =>
    typeof item === 'string' ? { keyword: item, addedAt: '2026-01-01' } : item
  );

  // 신규 키워드 추가
  const existingNorms = new Set(pool.map(item => norm(item.keyword)));
  const newEntries = newKeywords
    .filter(kw => !existingNorms.has(norm(kw)))
    .map(kw => ({ keyword: kw, addedAt: today }));

  const merged = [...newEntries, ...pool].slice(0, 100);
  await redis.set('keyword_pool', JSON.stringify(merged));
  console.log(`[updateKeywordPool] pool 크기: ${merged.length} (신규: ${newEntries.length}개)`);
  return merged;
}

// ─────────────────────────────────────────
// Step 4: DataLab 검색량 조회
// ─────────────────────────────────────────
async function getSearchTrends(keywords) {
  const chunks = [];
  for (let i = 0; i < keywords.length; i += 5) {
    chunks.push(keywords.slice(i, i + 5));
  }

  const chunkResults = await Promise.all(chunks.map(async (chunk, ci) => {
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
        return data.results.map(result => {
          const values = result.data.map(d => d.ratio);
          const recent7 = values.slice(-7);
          const prev7 = values.slice(-14, -7);
          const weeklyRate = avg(prev7) > 0 ? ((avg(recent7) - avg(prev7)) / avg(prev7)) * 100 : 0;
          const recent3 = values.slice(-3);
          const prev3 = values.slice(-6, -3);
          const risingRate = avg(prev3) > 0 ? ((avg(recent3) - avg(prev3)) / avg(prev3)) * 100 : 0;
          return { keyword: result.title, weeklyRate, risingRate, values };
        });
      }
    } catch (e) {
      console.log('[getSearchTrends] chunk 오류', ci, e.message);
    }
    return [];
  }));

  return chunkResults.flat();
}

// ─────────────────────────────────────────
// Step 5: 포스팅 수 조회
// ─────────────────────────────────────────
async function getBlogPostCount(keywords) {
  return Promise.all(keywords.map(async (kw) => {
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
      return { keyword: kw, total: data.total > 0 ? data.total : 1000 };
    } catch {
      return { keyword: kw, total: 1000 };
    }
  }));
}

// ─────────────────────────────────────────
// Step 6: 키워드 정제 (사용자 노출용 표기 정규화)
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
- 붙어있는 합성어는 자연스러운 띄어쓰기로 교정 (예: 갓생루틴 → 갓생 루틴)
- 고유명사, 브랜드명, 제품명은 절대 쪼개지 마 (예: 황치즈칩 → 황치즈칩, 버터떡 → 버터떡, 오뚜기 진밀면 → 오뚜기 진밀면)
- 이미 자연스러운 것은 절대 바꾸지 마
- 앞뒤 불필요한 특수문자만 제거

반드시 JSON으로만: {"0":"정제된키워드","1":"정제된키워드",...}
다른 설명 없이 JSON만.`,
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
    return keywords.map((kw, i) => polished[String(i)] || kw);
  } catch (e) {
    console.log('[polishKeywords] 실패, 원본 유지:', e.message);
    return keywords;
  }
}

// ─────────────────────────────────────────
// Step 7: 코멘트 생성
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
    const allTitles = await collectBlogTitles();
    if (!allTitles.length) throw new Error('블로그 제목 수집 실패');

    const refined = await extractTrendKeywords(allTitles);
    if (!refined.length) throw new Error('키워드 추출 실패');

    const keywordPool = await updateKeywordPool(refined);
    if (!keywordPool.length) throw new Error('키워드 풀 없음');

    // pool 앞 40개 포스팅 수 조회 → 50만 초과 제거 → 중복 제거 → DataLab 조회
    const poolKeywords = keywordPool.slice(0, 40).map(item => item.keyword);
    const poolPostCounts = await getBlogPostCount(poolKeywords);
    const poolPostMap = Object.fromEntries(poolPostCounts.map(p => [p.keyword, p.total]));

    // 50만 초과 제거
    const filteredPool = poolKeywords.filter(kw => (poolPostMap[kw] || 0) < 500000);
    console.log(`[preFilter] 50만 초과 제거: ${poolKeywords.length}개 → ${filteredPool.length}개`);

    // 중복 제거 (짧은 키워드 우선)
    const dedupedPool = [];
    const sortedPool = [...filteredPool].sort((a, b) => normKw(a).length - normKw(b).length);
    for (const kw of sortedPool) {
      const n = normKw(kw);
      if (n.length < 2) { dedupedPool.push(kw); continue; }
      const isDup = dedupedPool.some(d => {
        const nd = normKw(d);
        if (nd.length < 2) return false;
        return n.includes(nd) || nd.includes(n);
      });
      if (!isDup) dedupedPool.push(kw);
    }
    console.log(`[preFilter] 중복 제거: ${filteredPool.length}개 → ${dedupedPool.length}개`);

    const rawTrends = await getSearchTrends(dedupedPool);
    if (!rawTrends.length) throw new Error('트렌드 조회 실패');

    const postValues = dedupedPool.map(kw => poolPostMap[kw] || 0);
    const medianPost = median(postValues);
    const postCountMap = poolPostMap;
    const maxPost = Math.max(...postValues, 1);
    const maxRate = Math.max(...rawTrends.map(t => t.weeklyRate), 1);

    const addedAtMap = Object.fromEntries(
      keywordPool.map(item => [item.keyword, item.addedAt || '2026-01-01'])
    );

    const ranked = rawTrends.map(t => {
      const postCount = postCountMap[t.keyword] || 0;
      const daysInPool = daysDiff(addedAtMap[t.keyword] || '2026-01-01');
      const newBonus = daysInPool <= 7 ? 0.15 : 0;

      // 점수: 검색량 변화율 60% + 포스팅수 10% + 신규 진입 보너스 15%
      const score = (t.weeklyRate / maxRate) * 0.6
        + (postCount / maxPost) * 0.1
        + newBonus;

      return {
        keyword: t.keyword,
        score,
        changeRate: t.weeklyRate,
        risingRate: t.risingRate,
        postCount,
        trend: classifyTrend(t.weeklyRate, postCount, medianPost),
        values: t.values,
        isNew: daysInPool <= 7,
      };
    })
    .sort((a, b) => b.score - a.score);

    // 최종 랭킹 (DataLab 조회 후 추가 중복 없으므로 그대로 사용)
    const deduped = ranked;
    const finalRanked = deduped.sort((a, b) => b.score - a.score).slice(0, 20).map((k, i) => ({ ...k, rank: i + 1 }));

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

    // 키워드 정제
    const polishedNames = await polishKeywords(finalRanked.map(k => k.keyword));
    console.log('[polishKeywords] 정제 결과:', polishedNames.slice(0, 5));
    finalRanked.forEach((k, i) => { k.keyword = polishedNames[i]; });

    // 코멘트 생성
    const commentsRaw = await generateComments(finalRanked.slice(0, 10));
    const comments = finalRanked.slice(0, 10).map((_, i) => commentsRaw[String(i)] || '');

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
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
