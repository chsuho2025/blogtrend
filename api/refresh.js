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

  // display=100으로 한 번에 수집: 앞 50개(최근 3일) vs 뒤 50개(이전 3일)
  const recentTitles = [];  // 최근 3일
  const olderTitles = [];   // 이전 3일
  const seenRecent = new Set();
  const seenOlder = new Set();

  for (const keyword of NET_KEYWORDS) {
    await sleep(100);
    try {
      const url = `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(keyword)}&display=100&start=1&sort=date`;
      const res = await fetch(url, {
        headers: {
          'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
          'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
        },
      });
      const data = await res.json();
      if (data.items) {
        rawCount += data.items.length;
        data.items.forEach((item, idx) => {
          const title = item.title
            .replace(/<[^>]+>/g, '')
            .replace(/&amp;/g, '&').replace(/&quot;/g, '"')
            .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
            .replace(/&apos;/g, "'").replace(/&#(\d+);/g, (_, c) => String.fromCharCode(c))
            .trim();

          // 앞 50개 = 최근 3일, 뒤 50개 = 이전 3일
          if (idx < 50) {
            if (!seenRecent.has(title)) { seenRecent.add(title); recentTitles.push(title); }
          } else {
            if (!seenOlder.has(title)) { seenOlder.add(title); olderTitles.push(title); }
          }
          if (!seenTitles.has(title)) { seenTitles.add(title); allTitles.push(title); }
        });
      } else {
        console.log(`[collectBlogTitles] ${keyword} 빈응답`);
      }
    } catch (e) {
      console.log(`[collectBlogTitles] ${keyword} 오류:`, e.message);
    }
  }

  const filtered = allTitles.filter(t => !NOISE_PATTERNS.some(p => p.test(t)));
  const filteredRecent = recentTitles.filter(t => !NOISE_PATTERNS.some(p => p.test(t)));
  const filteredOlder = olderTitles.filter(t => !NOISE_PATTERNS.some(p => p.test(t)));

  // 명사구 단위 빈도 계산 (서술어/형용사 어미 제외)
  const tokenize = titles => {
    const freq = {};
    const VERB_ENDINGS = /[는은을를이가의에서로도와과만도씩며고면서하고하며하면한할합니다해요했습니다이다이에요]$/;
    const STOP_WORDS = new Set([
      '후기', '추천', '리뷰', '구매', '사용', '소개', '정보', '방법', '이유', '가격',
      '할인', '이벤트', '베스트', '정리', '꿀팁', '공유', '마케팅', '브랜딩',
      'BEST', 'TOP', 'feat',
    ]);

    for (const title of titles) {
      const korWords = title.match(/[가-힣]{2,}/g) || [];
      for (const w of korWords) {
        if (STOP_WORDS.has(w)) continue;
        if (VERB_ENDINGS.test(w)) continue;
        if (/^\d+$/.test(w)) continue;
        freq[w] = (freq[w] || 0) + 1;
      }
      const mixedWords = title.match(/[A-Za-z가-힣][A-Za-z0-9가-힣]{2,}/g) || [];
      for (const w of mixedWords) {
        if (STOP_WORDS.has(w)) continue;
        if (/^[a-z]/.test(w) && w.length < 4) continue;
        freq[w] = (freq[w] || 0) + 1;
      }
    }
    return freq;
  };

  const recentFreq = tokenize(filteredRecent);
  const olderFreq = tokenize(filteredOlder);

  // 최근 3일 vs 이전 3일 급등 단어 추출
  const risingWords = Object.entries(recentFreq)
    .filter(([word, cnt]) => {
      if (cnt < 2) return false;
      const older = olderFreq[word] || 0;
      if (older === 0) return cnt >= 3;
      return (cnt / older) >= 1.5;
    })
    .sort((a, b) => b[1] - a[1])
    .slice(0, 30)
    .map(([word]) => word);

  console.log(`[collectBlogTitles] 총 ${filtered.length}개 수집 (최근3일: ${filteredRecent.length}개, 이전3일: ${filteredOlder.length}개)`);
  console.log(`[collectBlogTitles] 급상승 단어 TOP10:`, risingWords.slice(0, 10));

  return { titles: filtered, risingWords };
}

// ─────────────────────────────────────────
// Step 2: HyperCLOVA X 키워드 추출
// ─────────────────────────────────────────
async function extractTrendKeywords(titles, risingWords = []) {
  const CHUNK_SIZE = 400;
  const allKeywords = [];

  // risingWords 상위 20개를 프롬프트에 힌트로 제공
  // risingWords 상위 20개를 프롬프트에 힌트로 제공
  const risingHint = risingWords.length > 0
    ? `\n\n참고: 최근 3일간 블로그에서 급상승한 단어들이야. 이 단어가 포함된 키워드를 우선적으로 뽑아줘:\n${risingWords.slice(0, 20).join(', ')}`
    : '';

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
                content: `너는 네이버 DataLab 트렌드 키워드 선별 전문가야.
아래 블로그 제목들에서 "지금 전국민이 네이버에서 검색하는 트렌드 키워드" 15개만 뽑아줘.
반드시 15개 이하로만 뽑아. 15개를 초과하면 절대 안 돼.

뽑아야 할 것:
- 전국민이 검색하는 제품명/음식명 (예: 황치즈칩, 버터떡, 닌텐도 스위치, 에브리봇)
- 브랜드+카테고리 (예: 자라 봄신상, 스타벅스 신메뉴, 메가커피 봄신메뉴)
- 지금 막 뜨는 트렌드어 (예: 갓생루틴, 무지출챌린지, 두바이 찰떡파이)
- 영화/드라마/게임 타이틀 (예: 아카데미 시상식, 케이팝 데몬 헌터스)

절대 뽑지 말 것:
- 연예인/인물 이름 (예: 이영애, 신봉선, 이휘재, 쯔양, 장영란, 한소희, 장원영)
- 방송 프로그램/회차 (예: 나솔사계, 현역가왕3, 나는솔로 22기, 나솔 30기)
- 법률/의료/부동산 광고 (예: 손해배상변호사, 그루밍성범죄, 파주성범죄로펌, 강남변호사)
- 지역 상호명/맛집 (예: 선릉 버터떡 맛집, 경복궁맛집 푸페또클럽, 을지로 돌판집)
- 모델번호/시리얼 (예: LG휘센 SQ09B9JWBS, NT930X5JK82S, LG 15인치 놀라운생각)
- 범용어 단독 (예: 다이어트, 홈트레이닝, 영등포, 가디건, 인테리어, 맛집, 후기)
- 날짜/채용/일정 (예: 2026년 3월 15일, 2026 CJ제일제당 채용)
- 블로그 제목 그대로 (예: 선릉 버터떡 맛집 후기 다녀왔어요)
- 단독 단어 (예: 화이트, 스마트, 분위기, 직장인, 드라마, 비교)

반드시 JSON 배열로만, 15개 이하: ["키워드1","키워드2",...]
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
      const limited = keywords.slice(0, 15); // chunk당 최대 15개 강제 제한
      console.log(`[extractTrendKeywords] chunk${Math.floor(i / CHUNK_SIZE) + 1}: ${limited.length}개 →`, limited);
      allKeywords.push(...limited);
    } catch (e) {
      console.log(`[extractTrendKeywords] chunk${Math.floor(i / CHUNK_SIZE) + 1} 실패:`, e.message);
    }
  }

  // 코드 필터: 타입, 최소 길이, 특수문자, 띄어쓰기 중복
  const norm = s => s.replace(/\s+/g, '').toLowerCase();
  const seenNorm = new Set();
  // 광고성/노이즈 키워드 패턴
  const NOISE_KW = [
    /변호사/, /법률/, /법인/, /소송/, /파산/, /이혼/, /형사/, /민사/, /고소/, /로펌/,
    /성범죄/, /추행/, /그루밍/, /성폭/, /성추행/, /강간/, /음란/, /도촬/, /중절/,
    /병원/, /의원/, /클리닉/, /한의원/, /치과/, /성형/, /피부과/, /시술/,
    /부동산/, /분양/, /임대/, /매매/, /공인중개/, /가전매입/, /렉카/,
    /줄거리/, /결말/, /등장인물/, /마케팅/, /브랜딩/,
    /추천하는/, /솔직한/, /나만의/, /이야기/, /가능한/, /특별한/,
    /태교여행/, /육아박스/, /이유식/, /임신초기/,
    /나솔/, /현역가왕/, /핫딜/, /공매도/, /파산/, /챌린지$/,
  ];
  const filtered = allKeywords.filter(kw => {
    if (typeof kw !== 'string') return false;
    if (kw.length < 2) return false;
    if (/[\[\]【】()（）<>《》]/.test(kw)) return false;
    if (NOISE_KW.some(p => p.test(kw))) return false;
    const n = norm(kw);
    if (seenNorm.has(n)) return false;
    seenNorm.add(n);
    return true;
  });

  console.log(`[extractTrendKeywords] 전체 ${allKeywords.length}개 추출 → 필터후 ${filtered.length}개`);
  return filtered;
}

// ─────────────────────────────────────────
// Step 3: 키워드 풀 누적 (이전 TOP20 고정 + 신규 추가)
// ─────────────────────────────────────────
async function updateKeywordPool(newKeywords) {
  let rawPool = [];
  let top20Fixed = [];

  try {
    const [poolStored, top20Stored] = await Promise.all([
      redis.get('keyword_pool'),
      redis.get('top20_pool'),
    ]);
    if (poolStored) rawPool = typeof poolStored === 'string' ? JSON.parse(poolStored) : poolStored;
    if (top20Stored) top20Fixed = typeof top20Stored === 'string' ? JSON.parse(top20Stored) : top20Stored;
    console.log('[updateKeywordPool] 기존 pool 크기:', rawPool.length, '/ 이전 TOP20:', top20Fixed.length);
  } catch (e) {
    console.log('[updateKeywordPool] pool 로드 실패:', e.message);
  }

  const today = getDateString(0);
  const norm = s => s.replace(/\s+/g, '').toLowerCase();

  // 모두 { keyword, addedAt } 형태로 정규화
  const pool = rawPool.map(item =>
    typeof item === 'string' ? { keyword: item, addedAt: '2026-01-01' } : item
  );

  // 이전 TOP20을 고정 앵커로 설정 (addedAt은 기존 날짜 유지, 새 키워드면 오늘)
  const top20Anchors = top20Fixed.map(kw => {
    const existing = pool.find(p => norm(p.keyword) === norm(kw));
    return {
      keyword: kw,
      addedAt: existing?.addedAt || today, // 기존 날짜 유지, 없으면 오늘
      isAnchor: true,
    };
  });
  const top20Norms = new Set(top20Fixed.map(norm));

  // 신규 키워드 중 TOP20 앵커와 중복 아닌 것만 추가 (최대 20개)
  const existingNorms = new Set([
    ...pool.map(item => norm(item.keyword)),
    ...top20Norms,
  ]);
  const newEntries = newKeywords
    .filter(kw => !existingNorms.has(norm(kw)))
    .slice(0, 20)
    .map(kw => ({ keyword: kw, addedAt: today }));

  // 14일 지난 키워드 제거 (앵커 제외)
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 14);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  const poolFiltered = pool.filter(item =>
    !top20Norms.has(norm(item.keyword)) && // 앵커는 위에서 따로 처리
    (item.addedAt || '2026-01-01') >= cutoffStr
  );
  console.log(`[updateKeywordPool] 14일 초과 제거: ${pool.length}개 → ${poolFiltered.length}개`);

  // 구성: [이전 TOP20 앵커] + [신규 20개] + [기존 pool 잔여]
  // pool 저장 전 연예인/인물/단어 필터링
  const POOL_NOISE = [
    /변호사/, /법률/, /소송/, /성범죄/, /추행/, /그루밍/, /중절/, /로펌/,
    /나솔/, /현역가왕/, /태교여행/, /핫딜/, /공매도/,
  ];
  const POOL_STOP_SINGLES = new Set([
    '화이트', '가지', '스마트', '분위기', '직장인', '드라마', '우리', '비교',
    '에어', '한정판', '실사용', '언박싱', '필라테스', '가습기', '두쫀쿠',
    '다이어트', '홈트레이닝', '맛집', '후기', '추천',
  ]);

  const cleanMerged = [...top20Anchors, ...newEntries, ...poolFiltered]
    .filter(item => {
      const kw = item.keyword;
      if (POOL_STOP_SINGLES.has(kw)) return false;
      if (POOL_NOISE.some(p => p.test(kw))) return false;
      return true;
    })
    .slice(0, 100);

  await redis.set('keyword_pool', JSON.stringify(cleanMerged));
  console.log(`[updateKeywordPool] pool 크기: ${cleanMerged.length} (앵커: ${top20Anchors.length}개, 신규: ${newEntries.length}개)`);
  return cleanMerged;
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
      // total이 0이거나 없으면 null (API 실패로 간주, 프론트에서 "—" 표시)
      const total = (data.total && data.total > 0) ? data.total : null;
      return { keyword: kw, total };
    } catch {
      return { keyword: kw, total: null };
    }
  }));
}

// ─────────────────────────────────────────
// Step 6: 키워드 정제 + 카테고리 분류
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
각 키워드를 다듬고 카테고리도 분류해줘.

규칙:
- 붙어있는 합성어는 자연스럽게 띄어쓰기 (예: 갓생루틴 → 갓생 루틴)
- 고유명사/브랜드명/제품명은 절대 쪼개지 마
- 이미 자연스러운 것은 바꾸지 마

카테고리는 반드시 아래 6가지 중 하나:
FOOD(음식/식품/음료), FASHION(패션/의류/잡화), BEAUTY(뷰티/화장품/스킨케어), TECH(테크/가전/IT), LIFE(생활/육아/인테리어), ENTER(엔터/문화/이벤트/스포츠)

반드시 JSON으로만: {"0":{"name":"정제된키워드","category":"FOOD"},...}
다른 설명 없이 JSON만.`,
            },
            { role: 'user', content: kwList },
          ],
          maxCompletionTokens: 1000,
          temperature: 0.1,
          repetitionPenalty: 1.0,
          thinking: { effort: 'none' },
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '{}';
    const polished = JSON.parse(text.replace(/```json|```/g, '').trim());
    const names = keywords.map((kw, i) => polished[String(i)]?.name || kw);
    const categories = keywords.map((kw, i) => polished[String(i)]?.category || '');
    return { names, categories };
  } catch (e) {
    console.log('[polishKeywords] 실패, 원본 유지:', e.message);
    return { names: keywords, categories: keywords.map(() => '') };
  }
}

// ─────────────────────────────────────────
// Step 7: 코멘트 생성
// ─────────────────────────────────────────
async function fetchBlogContent(keyword) {
  // 키워드로 블로그 검색해서 제목+본문요약 3개 가져오기
  try {
    const res = await fetch(
      `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(keyword)}&display=3&sort=sim`,
      {
        headers: {
          'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
          'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
        },
      }
    );
    const data = await res.json();
    if (!data.items) return [];
    return data.items.map(item => {
      const title = item.title.replace(/<[^>]+>/g, '').trim();
      const desc = item.description.replace(/<[^>]+>/g, '').trim();
      return `${title}: ${desc}`;
    });
  } catch {
    return [];
  }
}

async function generateComments(topKeywords, allTitles = []) {
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  // 각 키워드별 블로그 본문 내용 병렬 수집
  const contentResults = await Promise.all(
    topKeywords.map(async (k, idx) => {
      await sleep(idx * 80); // 80ms 간격으로 분산
      const posts = await fetchBlogContent(k.keyword);
      return posts;
    })
  );

  // 키워드 + 관련글 제목 + 본문요약 조합
  const kwWithContext = topKeywords.map((k, i) => {
    const posts = contentResults[i];
    const context = posts.length > 0
      ? '\n  참고:\n' + posts.map(p => `  - ${p}`).join('\n')
      : '';
    return `${i}:${k.keyword}${context}`;
  }).join('\n\n');

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
              content: `아래는 네이버 블로그 트렌드 키워드와 실제 블로그 게시글 내용이야.
각 키워드가 무엇인지 게시글 내용을 참고해서 20자 이내 한 줄로 설명해.

규칙:
- "~이다", "~했다" 같은 서술형으로 끝내지 말고 명사형으로 끝낼 것
- 게시글 내용을 읽고 키워드가 무엇인지 정확하게 파악할 것
- 영화/드라마라면 "OO 감독의 OO 장르 영화" 형식
- 제품이라면 "OO 브랜드의 OO 기능 제품" 형식
- 음식이라면 "OO 특징의 OO 음식" 형식
- 이벤트/시상식이라면 "OO에서 개최된 OO" 형식
- 절대 "~화제", "~인기", "~관심" 같은 반응형 표현 쓰지 말 것
- 키워드 자체를 설명에 그대로 반복하지 말 것

좋은 예시:
- 아카데미 시상식 → "미국 영화예술과학아카데미 주관 영화 시상식"
- 황치즈칩 → "오리온의 진한 황치즈 맛 과자 신제품"
- 케이팝 데몬 헌터스 → "넷플릭스 공개 K팝 스타 주인공 판타지 영화"

반드시 JSON 형식으로만: {"0":"설명","1":"설명",...}
다른 설명 없이 JSON만.`,
            },
            { role: 'user', content: kwWithContext },
          ],
          maxCompletionTokens: 1000,
          temperature: 0.3,
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
function classifyTrend(weeklyRate, risingRate, postCount, medianPostCount) {
  // 1차: risingRate(최근 3일) 우선 판단
  if (risingRate >= 20) {
    return postCount < medianPostCount ? '유행예감' : '유행중';
  }
  if (risingRate <= -20) return '유행지남';

  // 2차: 보합 구간(-20~20) → weeklyRate로 판단
  if (weeklyRate >= 10) return '유행중';
  if (weeklyRate <= -10) return '유행지남';

  // 3차: 둘 다 보합 → risingRate 방향으로 미세 판단
  return risingRate >= 0 ? '유행중' : '유행지남';
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
    const { titles: allTitles, risingWords } = await collectBlogTitles();
    if (!allTitles.length) throw new Error('블로그 제목 수집 실패');

    const refined = await extractTrendKeywords(allTitles, risingWords);
    if (!refined.length) throw new Error('키워드 추출 실패');

    const keywordPool = await updateKeywordPool(refined);
    if (!keywordPool.length) throw new Error('키워드 풀 없음');

    // pool 앞 40개 포스팅 수 조회 → 50만 초과 제거 → 중복 제거 → DataLab 조회
    const poolKeywords = keywordPool.slice(0, 40).map(item => item.keyword);
    const poolPostCounts = await getBlogPostCount(poolKeywords);
    const poolPostMap = Object.fromEntries(poolPostCounts.map(p => [p.keyword, p.total]));

    // 50만 초과 제거 (null이면 통과)
    const filteredPool = poolKeywords.filter(kw => {
      const cnt = poolPostMap[kw];
      return cnt === null || cnt === undefined || cnt < 500000;
    });
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

    // null 제외하고 medianPost 계산
    const postValues = dedupedPool.map(kw => poolPostMap[kw]).filter(v => v != null);
    const medianPost = postValues.length ? median(postValues) : 0;
    const postCountMap = poolPostMap;
    const maxPost = Math.max(...postValues, 1);
    const maxRate = Math.max(...rawTrends.map(t => t.weeklyRate), 1);

    const addedAtMap = Object.fromEntries(
      keywordPool.map(item => [item.keyword, item.addedAt || '2026-01-01'])
    );

    // ── 포스팅 급등 알고리즘 (blogSurge) ──
    // 어제 포스팅 수와 비교해서 급등 키워드 감지
    const postHistoryMap = {};
    await Promise.all(dedupedPool.map(async (kw) => {
      const postCount = poolPostMap[kw];
      if (!postCount) return;
      try {
        const histKey = `post_history:${kw}`;
        let hist = [];
        const stored = await redis.get(histKey);
        if (stored) hist = typeof stored === 'string' ? JSON.parse(stored) : stored;

        // 30일 이상 된 기록 제거
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - 30);
        const cutoffStr = cutoff.toISOString().slice(0, 10);
        hist = hist.filter(h => h.date >= cutoffStr);

        // 어제 포스팅 수
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        const yesterdayStr = yesterday.toISOString().slice(0, 10);
        const yesterdayEntry = hist.find(h => h.date === yesterdayStr);
        const yesterdayCount = yesterdayEntry?.count || 0;

        // blogSurgeRate 계산
        let blogSurgeRate = 0;
        if (yesterdayCount >= 500 && postCount > yesterdayCount) {
          blogSurgeRate = ((postCount - yesterdayCount) / yesterdayCount) * 100;
        }

        // 오늘 기록 추가
        hist = hist.filter(h => h.date !== today);
        hist.push({ date: today, count: postCount });
        await redis.set(histKey, JSON.stringify(hist));

        if (blogSurgeRate >= 20) {
          postHistoryMap[kw] = { blogSurgeRate: Math.round(blogSurgeRate), yesterdayCount };
          console.log(`[blogSurge] 급등 감지: ${kw} (+${Math.round(blogSurgeRate)}%, ${yesterdayCount}→${postCount})`);
        }
      } catch(e) {}
    }));

    console.log('[blogSurge] 급등 키워드:', Object.keys(postHistoryMap));

    const ranked = rawTrends.filter(t => {
      // 최근 7일 평균 검색량이 너무 낮으면 제외 (분모가 작아서 변화율이 뻥튀기되는 문제 방지)
      const recent7avg = avg(t.values.slice(-7));
      if (recent7avg < 1.0) {
        console.log(`[ranked] 검색량 낮아 제외: ${t.keyword} (7일평균 ${recent7avg.toFixed(2)})`);
        return false;
      }
      return true;
    }).map(t => {
      const postCount = postCountMap[t.keyword] ?? null;
      const addedDate = addedAtMap[t.keyword] || today;
      const daysInPool = daysDiff(addedDate);
      const newBonus = daysInPool <= 3 ? 0.15 : 0;
      const surge = postHistoryMap[t.keyword];
      const blogSurgeRate = surge?.blogSurgeRate || 0;
      const blogSurgeBonus = blogSurgeRate >= 20 ? 0.15 : blogSurgeRate >= 10 ? 0.08 : 0;

      const maxRising = Math.max(...rawTrends.map(r => r.risingRate), 1);
      const risingScore = t.risingRate > 0 ? (t.risingRate / maxRising) * 0.3 : 0;
      const score = (t.weeklyRate / maxRate) * 0.50
        + risingScore
        + blogSurgeBonus
        + newBonus;

      return {
        keyword: t.keyword,
        score,
        changeRate: t.weeklyRate,
        risingRate: t.risingRate,
        postCount,
        blogSurgeRate,
        blogSurge: blogSurgeRate >= 20,
        trend: classifyTrend(t.weeklyRate, t.risingRate, postCount, medianPost),
        values: t.values,
        isNew: daysInPool <= 3,
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

    // 키워드 정제 + 카테고리 분류
    const { names: polishedNames, categories } = await polishKeywords(finalRanked.map(k => k.keyword));
    console.log('[polishKeywords] 정제 결과:', polishedNames.slice(0, 5));
    finalRanked.forEach((k, i) => {
      k.keyword = polishedNames[i];
      k.category = categories[i] || '';
    });

    // 코멘트 생성 (관련 블로그 제목 포함)
    const commentsRaw = await generateComments(finalRanked.slice(0, 10), allTitles);
    const comments = finalRanked.slice(0, 10).map((_, i) => commentsRaw[String(i)] || '');

    const result = {
      updatedAt: new Date().toISOString(),
      keywords: finalRanked.map((k, i) => ({
        rank: i + 1,
        keyword: k.keyword,
        score: Math.round(k.score * 100),
        changeRate: Math.round(k.changeRate),
        risingRate: Math.round(k.risingRate),
        postCount: k.postCount,
        blogSurgeRate: k.blogSurgeRate || 0,
        blogSurge: k.blogSurge || false,
        category: k.category || '',
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
        blogSurge: k.blogSurge || false,
        trend: k.trend,
        isNew: k.isNew,
      })),
    };

    await redis.set('trend_data', JSON.stringify(result));

    // 이전 TOP20 키워드 목록 저장 (다음 리프레시에서 pool 앵커로 사용)
    // 연예인/인물/노이즈 필터링 후 저장
    const TOP20_NOISE = [
      /변호사/, /법률/, /소송/, /성범죄/, /추행/, /그루밍/, /중절/, /로펌/,
      /나솔/, /현역가왕/, /태교여행/, /핫딜/, /공매도/,
    ];
    const TOP20_STOP = new Set([
      '화이트', '가지', '스마트', '분위기', '직장인', '드라마', '우리', '비교',
      '에어', '한정판', '실사용', '언박싱', '필라테스', '가습기',
      '다이어트', '홈트레이닝', '맛집', '후기', '추천', '버터떡',
    ]);
    const top20Keywords = finalRanked
      .filter(k => {
        // 하락세 키워드 앵커 제외 (risingRate < -20 AND weeklyRate < -10)
        if (k.risingRate < -20 && k.changeRate < -10) {
          console.log('[top20_pool] 하락세 제외:', k.keyword, `(rising:${Math.round(k.risingRate)}%, weekly:${Math.round(k.changeRate)}%)`);
          return false;
        }
        return true;
      })
      .map(k => k.keyword)
      .filter(kw => {
        if (TOP20_STOP.has(kw)) return false;
        if (TOP20_NOISE.some(p => p.test(kw))) return false;
        if (kw.replace(/\s/g, '').length <= 2) return false;
        return true;
      });
    await redis.set('top20_pool', JSON.stringify(top20Keywords));
    console.log('[top20_pool] 저장:', top20Keywords.slice(0, 5));

    // 히스토리 저장 - 0시 1회 cron이므로 항상 저장, 최대 30일치
    const nowKST = new Date(new Date().getTime() + 9 * 60 * 60 * 1000);
    const dateStrKST = nowKST.toISOString().slice(0, 10);
    try {
      let history = [];
      const raw = await redis.get('trend_history');
      if (raw) history = typeof raw === 'string' ? JSON.parse(raw) : raw;
      history = history.filter(h => h.date !== dateStrKST);
      history.push({
        date: dateStrKST,
        timestamp: result.updatedAt,
        keywords: finalRanked.slice(0, 10).map(k => ({
          keyword: k.keyword,
          changeRate: Math.round(k.changeRate),
          risingRate: Math.round(k.risingRate),
          rank: k.rank,
          blogSurge: k.blogSurge || false,
        })),
      });
      history = history.filter(h => h.date); // date 없는 구형 항목 제거
      history.sort((a, b) => b.date.localeCompare(a.date));
      history = history.slice(0, 30); // 5일 → 30일
      await redis.set('trend_history', JSON.stringify(history));
      console.log('[trend_history] 저장:', dateStrKST, '/ 누적:', history.length + '일치');
    } catch(e) {
      console.log('[trend_history] 저장 실패:', e.message);
    }
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
