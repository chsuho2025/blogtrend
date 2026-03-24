const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// ─────────────────────────────────────────
// Step 0-1: 그물 키워드 자동 갱신 (매일)
// ─────────────────────────────────────────
const NET_KEYWORDS_BASE = [
  '신상', '요즘', '핫한', '뜨는', '화제', '인기', '난리', '대세',
  '후기', '추천', '꿀팁', '레시피', '챌린지', '리뷰', '사용기', '솔직후기',
  '득템', '하울', '언박싱', '추천템',
];

async function loadNetKeywords() {
  try {
    const stored = await redis.get('net_keywords_dynamic');
    if (stored) {
      const dynamic = typeof stored === 'string' ? JSON.parse(stored) : stored;
      if (Array.isArray(dynamic) && dynamic.length >= 5) {
        console.log('[netKeywords] 동적 키워드 로드:', dynamic.slice(0, 5));
        return [...NET_KEYWORDS_BASE, ...dynamic];
      }
    }
  } catch(e) {}
  return NET_KEYWORDS_BASE;
}

async function updateNetKeywords(risingWords) {
  if (!risingWords || risingWords.length < 5) return;
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
              content: `너는 네이버 블로그 트렌드 수집 전문가야.
아래는 오늘 네이버 블로그에서 급상승한 단어들이야.
이 단어들을 참고해서 내일 네이버 블로그에서 트렌드 콘텐츠를 많이 포함할 것 같은 검색 키워드 10개를 추천해줘.

조건:
- 블로그 검색 시 다양한 트렌드 콘텐츠가 나올 수 있는 일반적인 키워드
- 너무 구체적인 제품명 말고 카테고리 수준 (예: "봄신상", "신메뉴", "핫플")
- 오늘 급상승 단어와 연관된 카테고리 반영
- 중복 없이 10개

반드시 JSON 배열로만: ["키워드1","키워드2",...]
다른 설명 없이 JSON만.`,
            },
            {
              role: 'user',
              content: `오늘 급상승 단어: ${risingWords.slice(0, 20).join(', ')}`,
            },
          ],
          maxCompletionTokens: 300,
          temperature: 0.4,
          thinking: { effort: 'none' },
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '[]';
    const keywords = JSON.parse(text.replace(/```json|```/g, '').trim());
    if (Array.isArray(keywords) && keywords.length >= 5) {
      await redis.set('net_keywords_dynamic', JSON.stringify(keywords));
      console.log('[netKeywords] 동적 갱신 완료:', keywords);
    }
  } catch(e) {
    console.log('[netKeywords] 갱신 실패:', e.message);
  }
}

// ─────────────────────────────────────────
// Step 1: 그물 키워드로 블로그 제목 수집
// ─────────────────────────────────────────
async function collectBlogTitles(netKeywords) {
  const seenTitles = new Set();
  const allTitles = [];
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

  for (const keyword of netKeywords) {
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

  // 제목 사전 필터: 너무 짧거나 범용어 수준 제목 제거
  const TITLE_STOP = new Set(['봄', '여름', '가을', '겨울', '신상', '후기', '추천', '리뷰', '레시피', '꿀팁']);
  const preFilter = t => {
    if (!NOISE_PATTERNS.every(p => !p.test(t))) return false; // 노이즈 패턴
    if (t.replace(/\s/g, '').length < 8) return false;        // 공백 제거 후 8자 미만
    if (TITLE_STOP.has(t.trim())) return false;               // 단일 범용어 제목
    return true;
  };

  const filtered = allTitles.filter(preFilter);
  const filteredRecent = recentTitles.filter(preFilter);
  const filteredOlder = olderTitles.filter(preFilter);

  // 셔플: NET_KEYWORDS 순서 편향 제거 → chunk 균질화
  for (let i = filtered.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [filtered[i], filtered[j]] = [filtered[j], filtered[i]];
  }

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

  // 급상승 단어를 system 프롬프트에 컨텍스트로 통합 (user 메시지 오염 방지)
  const risingContext = risingWords.length > 0
    ? `\n\n[급상승 신호] 최근 3일간 블로그에서 특히 많이 등장한 단어들이야. 이 단어들이 포함된 구체적인 제품명/이벤트명/트렌드어를 우선 포착해:\n${risingWords.slice(0, 15).join(', ')}`
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

★ 뽑아야 할 것 (반드시 아래 형태여야 함):
- 브랜드+제품 조합 (예: 맥도날드 짱구 해피밀, 나이키 체리블라썸 운동화, 스타벅스 딸기 라떼)
- 음식/식품명 (예: 황치즈칩, 버터떡, 두바이 찰떡파이, 흑백요리사 레시피)
- 지금 화제인 이벤트/시상식/공연 (예: 아카데미 시상식, BTS 광화문 콘서트)
- 지금 막 뜨는 트렌드어 (예: 갓생 루틴, 무지출 챌린지)
- 영화/드라마/게임 타이틀 (예: 케이팝 데몬 헌터스, 붉은사막, 프로젝트 헤일메리)

★ 절대 뽑으면 안 되는 것:
- 단일 브랜드명만 (나쁨: "나이키", "아디다스", "이마트" → 반드시 뒤에 제품/카테고리 붙어야 함)
- 단일 범용어 (나쁨: "레시피", "피부", "가디건", "강아지", "웨이팅", "해결")
- 5자 이하 단독어 (나쁨: "봄", "루틴", "하울", "갓생", "피부")
- 연예인/인물 이름 단독 (나쁨: "풍자", "김현숙", "이준호", "카리나" → 이름+이벤트 조합만 허용)
- 방송 프로그램/회차 (나쁨: 나솔사계, 현역가왕3, 미우새, 편스토랑)
- 다이어트/체중 인물 서사 (나쁨: "풍자 28kg 감량", "김현숙 다이어트")
- 법률/의료/부동산 광고
- 지역 상호명/맛집명 (나쁨: "선릉 버터떡 맛집", "성수 오밀파스타")
- 모델번호/시리얼번호
- 날짜/채용/일정 정보
- 앱 퀴즈/이벤트 정답 (예: 신한 쏠퀴즈, 카카오뱅크 AI퀴즈, 캐시워크 3월23일 정답)
- 쇼핑몰 적립금/쿠폰 이벤트 (예: CJ온스타일 적립금, 카카오페이 쿠폰받기, 롯데하이마트 창립기념)
- 정부 보조금/지원금 정보 (예: 청년 일자리 도약 장려금, 청년 월세 지원, 국민취업지원제도)
- 재테크/주식/코인 정보 (예: 공모주 청약, 코스피 폭락, 비트코인 시세)
- 블로그 제목 그대로 복사${risingContext}

반드시 JSON 배열로만, 15개 이하: ["키워드1","키워드2",...]
다른 설명 없이 JSON만.`,
              },
              {
                role: 'user',
                content: chunk.join('\n'),
              },
            ],
            maxCompletionTokens: 3000,
            temperature: 0.3,
            repetitionPenalty: 1.1,
            thinking: { effort: 'none' },
          }),
        }
      );
      const data = await res.json();
      const raw = data.result?.message?.content || '[]';
      // JSON 클렌징: 백틱 제거 → 배열 추출 → 제어문자 제거
      let text = raw.replace(/```json|```/g, '').trim();
      const arrMatch = text.match(/\[[\s\S]*\]/);
      text = arrMatch ? arrMatch[0] : '[]';
      text = text.replace(/[\x00-\x1F\x7F]/g, ' ');
      let keywords = [];
      try {
        keywords = JSON.parse(text);
      } catch(parseErr) {
        // 파싱 실패 시 따옴표 안 문자열 직접 추출
        const extracted = [...text.matchAll(/"([^"\\]*(?:\\.[^"\\]*)*)"/g)].map(m => m[1]);
        keywords = extracted.length > 0 ? extracted : [];
        if (extracted.length > 0) {
          console.log(`[extractTrendKeywords] chunk${Math.floor(i / CHUNK_SIZE) + 1} 파싱 복구: ${extracted.length}개`);
        }
      }
      const limited = Array.isArray(keywords) ? keywords.slice(0, 15) : [];
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
    // 바이럴 마케팅/이벤트성 키워드
    /퀴즈/, /정답/, /OX퀴즈/, /적립금/, /쿠폰받기/, /무료나눔/, /선착순/,
    /지원금/, /장려금/, /보조금/, /바우처/, /페이래플/, /앱테크/, /리워드/,
    /공모주/, /청약/, /배당금/, /주가/, /코스피/, /코스닥/, /비트코인/,
    /코인/, /암호화폐/, /가상화폐/, /거래소/, /디지털자산/, /NFT/,
  ];
  // 최소 코드 필터: 공백 제거 후 3자 이하 단일어만 차단
  // (나머지 범용어는 autoBlacklist + AI 게이팅에 위임)
  const SINGLE_STOP = new Set([
    '봄', '여름', '가을', '겨울', '신상', '후기', '추천', '리뷰', '꿀팁',
    '피부', '일상', '하루', '오늘', '서울', '만원', '쇼핑',
  ]);

  const filtered = allKeywords.filter(kw => {
    if (typeof kw !== 'string') return false;
    if (kw.length < 2) return false;
    if (/[\[\]【】()（）<>《》]/.test(kw)) return false;
    if (NOISE_KW.some(p => p.test(kw))) return false;
    // 단일어 강화 필터: 공백 없는 4자 미만 또는 차단 목록
    const noSpace = kw.replace(/\s+/g, '');
    if (noSpace.length < 4 && !kw.includes(' ')) return false;
    if (SINGLE_STOP.has(kw.trim())) return false;
    const n = norm(kw);
    if (seenNorm.has(n)) return false;
    seenNorm.add(n);
    return true;
  });

  console.log(`[extractTrendKeywords] 전체 ${allKeywords.length}개 추출 → 필터후 ${filtered.length}개`);
  return filtered;
}


// ─────────────────────────────────────────
// Step 자가학습: DataLab 0.00 자동 블랙리스트
// ─────────────────────────────────────────
async function updateAutoBlacklist(rawTrends, top20Keywords = []) {
  try {
    const top20Set = new Set(top20Keywords.map(k => k.toLowerCase().replace(/\s+/g, '')));
    const zeroKws = rawTrends
      .filter(t => {
        if (!t.values || t.values.length === 0) return false;
        const slice = t.values.slice(-7).filter(v => v != null);
        if (slice.length === 0) return false;
        const avg7 = slice.reduce((a, b) => a + b, 0) / slice.length;
        if (avg7 >= 0.1) return false; // 검색지수 있으면 제외
        // 현재 상위 랭킹 키워드는 블랙리스트 제외 (DataLab 지연 가능성)
        const norm = t.keyword.toLowerCase().replace(/\s+/g, '');
        if (top20Set.has(norm)) return false;
        return true;
      })
      .map(t => t.keyword);

    if (zeroKws.length === 0) return;

    let blacklist = [];
    try {
      const stored = await redis.get('auto_blacklist');
      if (stored) blacklist = typeof stored === 'string' ? JSON.parse(stored) : stored;
    } catch(e) {}

    const today = new Date().toISOString().slice(0, 10);
    const expiryCutoff = new Date();
    expiryCutoff.setDate(expiryCutoff.getDate() - 30);
    const expiryStr = expiryCutoff.toISOString().slice(0, 10);

    // 30일 지난 항목 제거
    blacklist = blacklist.filter(b => b.date >= expiryStr);

    // 새 0.00 키워드 추가 (중복 제외)
    const existingKws = new Set(blacklist.map(b => b.keyword));
    const newEntries = zeroKws
      .filter(kw => !existingKws.has(kw))
      .map(kw => ({ keyword: kw, date: today }));

    blacklist.push(...newEntries);
    await redis.set('auto_blacklist', JSON.stringify(blacklist));

    if (newEntries.length > 0) {
      console.log('[autoBlacklist] 추가:', newEntries.map(b => b.keyword));
    }
    console.log('[autoBlacklist] 현재 블랙리스트:', blacklist.length, '개');
  } catch(e) {
    console.log('[autoBlacklist] 업데이트 실패:', e.message);
  }
}

async function loadAutoBlacklist() {
  try {
    const stored = await redis.get('auto_blacklist');
    if (!stored) return new Set();
    const blacklist = typeof stored === 'string' ? JSON.parse(stored) : stored;
    return new Set(blacklist.map(b => b.keyword));
  } catch(e) {
    return new Set();
  }
}

// ─────────────────────────────────────────
// Step 3-1: AI 기반 의미 중복 제거
// ─────────────────────────────────────────
async function deduplicateByMeaning(newKeywords, existingKeywords) {
  if (newKeywords.length === 0) return [];
  try {
    const newList = newKeywords.map((k, i) => `NEW_${i}: ${k}`).join('\n');
    const existList = existingKeywords.slice(0, 30).join(', '); // 기존 키워드 상위 30개만 참고

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
              content: `너는 키워드 품질 심사 및 중복 제거 전문가야.

아래 "신규 키워드" 목록을 두 가지 기준으로 필터링해줘.

[1단계: 품질 제거]
아래에 해당하면 제거:
- 단일 브랜드명만 (예: "나이키", "아디다스", "이마트", "스타벅스")
- 단일 범용어 (예: "레시피", "피부", "가디건", "강아지", "루틴", "해결")
- 연예인/방송인 이름 단독 (예: "풍자", "카리나", "이준호")
- 다이어트/체중 인물 서사 (예: "풍자 28kg 감량", "김현숙 다이어트")
- 방송 프로그램/회차 (예: "나솔사계", "미우새", "편스토랑")
- 앱 퀴즈/이벤트 정답 (예: "신한 쏠퀴즈", "카카오뱅크 AI퀴즈", "캐시워크 정답")
- 쇼핑몰 적립금/쿠폰 이벤트 (예: "CJ온스타일 적립금", "카카오페이 쿠폰받기")
- 정부 보조금/지원금 (예: "청년 일자리 도약 장려금", "청년 월세 지원")
- 재테크/주식/코인 정보 (예: "공모주 청약", "코스피 폭락", "비트코인 시세")
- 네이버에서 실제로 검색할 것 같지 않은 키워드

[2단계: 의미 중복 제거]
- 같은 인물/그룹의 다른 표현 (BTS = 방탄소년단)
- 같은 이벤트의 다른 표현 (BTS 광화문 콘서트 = 방탄소년단 광화문 공연 = BTS 컴백 콘서트)
- 같은 제품/브랜드의 다른 표현 (삼성전자 배당금 = 삼성전자 특별배당금)
- 기존 키워드와 같은 이슈를 다루는 신규 키워드
- 중복 그룹에서 가장 구체적이고 검색량이 많을 것 같은 1개만 남길 것

★ 절대 규칙: 중복 그룹이 있어도 반드시 그 그룹에서 대표 키워드 1개는 살려야 해.
예) "BTS 광화문 콘서트", "방탄소년단 광화문 공연", "BTS 컴백 콘서트" → 이 중 1개는 반드시 남김
절대로 중복 그룹 전체를 다 제거하면 안 돼.

반드시 JSON 배열로만: ["남길키워드1", "남길키워드2", ...]
제거 없이 다 남기는 것도 가능. 다른 설명 없이 JSON만.`,
            },
            {
              role: 'user',
              content: `기존 키워드: ${existList}\n\n신규 키워드:\n${newList}`,
            },
          ],
          maxCompletionTokens: 800,
          temperature: 0.1,
          repetitionPenalty: 1.0,
          thinking: { effort: 'none' },
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '[]';
    const result = JSON.parse(text.replace(/```json|```/g, '').trim());
    if (!Array.isArray(result)) return newKeywords;
    console.log(`[deduplicateByMeaning] ${newKeywords.length}개 → ${result.length}개 (${newKeywords.length - result.length}개 중복 제거)`);
    return result.filter(k => typeof k === 'string');
  } catch (e) {
    console.log('[deduplicateByMeaning] 실패, 원본 유지:', e.message);
    return newKeywords;
  }
}

// ─────────────────────────────────────────
// Step 3-2: pool 전체 의미 중복 정리
// ─────────────────────────────────────────
async function cleanPoolDuplicates(pool) {
  // pool 전체 키워드 중 의미 중복 제거 (앵커 포함)
  const keywords = pool.map(p => p.keyword);
  if (keywords.length === 0) return pool;
  try {
    // 앞 20개만 처리 (토큰 초과 방지 + 앵커 위주로 정리)
    const targetKeywords = keywords.slice(0, 20);
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
              content: `아래 키워드 목록에서 같은 이슈/인물/이벤트를 가리키는 중복 키워드를 찾아줘.

중복 그룹을 찾아서 각 그룹에서 가장 구체적이고 검색량이 많을 것 같은 대표 키워드 1개만 남기고,
나머지는 제거한 목록을 반환해줘.

예시:
- "BTS 광화문 콘서트", "방탄소년단 광화문 공연", "BTS 컴백 콘서트" → "BTS 광화문 콘서트" 1개만
- "삼성전자 배당금", "삼성전자 특별배당금" → "삼성전자 특별배당금" 1개만
- "아카데미 시상식", "아카데미상" → "아카데미 시상식" 1개만

★ 절대 규칙: 중복 그룹이 있어도 반드시 그 그룹에서 대표 키워드 1개는 살려야 해.
절대로 중복 그룹 전체를 다 제거하면 안 돼.

반드시 JSON 배열로만: ["키워드1", "키워드2", ...]
다른 설명 없이 JSON만.`,
            },
            {
              role: 'user',
              content: targetKeywords.join('\n'),
            },
          ],
          maxCompletionTokens: 3000,
          temperature: 0.1,
          repetitionPenalty: 1.0,
          thinking: { effort: 'none' },
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '[]';
    const cleaned = JSON.parse(text.replace(/```json|```/g, '').trim());
    if (!Array.isArray(cleaned) || cleaned.length === 0) return pool;

    const cleanedSet = new Set(cleaned.map(k => k.trim()));
    // 앞 20개는 정리된 결과로, 나머지는 그대로 유지
    const front = pool.slice(0, 20).filter(p => cleanedSet.has(p.keyword));
    const rest = pool.slice(20);
    const result = [...front, ...rest];
    console.log(`[cleanPoolDuplicates] ${pool.length}개 → ${result.length}개 (${pool.length - result.length}개 중복 정리)`);
    return result;
  } catch (e) {
    console.log('[cleanPoolDuplicates] 실패, 원본 유지:', e.message);
    return pool;
  }
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
  // 자동 블랙리스트 로드 (DataLab 0.00 자동 학습)
  const autoBlacklist = await loadAutoBlacklist();
  const candidates = newKeywords.filter(kw =>
    !existingNorms.has(norm(kw)) && !autoBlacklist.has(kw)
  );
  if (autoBlacklist.size > 0) {
    const blocked = newKeywords.filter(kw => autoBlacklist.has(kw));
    if (blocked.length > 0) console.log('[autoBlacklist] 차단:', blocked);
  }

  // AI 기반 의미 중복 제거 — 앵커 목록도 함께 전달해서 앵커와 신규 간 중복도 제거
  // (앵커끼리 중복은 top20_pool 저장 단계에서 이미 처리됨)
  const deduplicatedCandidates = await deduplicateByMeaning(candidates, [
    ...top20Fixed,
    ...pool.map(p => p.keyword),
  ]);

  const newEntries = deduplicatedCandidates
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
  // 법적 민감 패턴만 코드 레벨 유지 (나머지는 AI 게이팅 위임)
  const LEGAL_NOISE = [
    /성범죄/, /추행/, /그루밍/, /성폭/, /성추행/, /강간/, /음란/, /도촬/, /중절/,
    /변호사/, /법률/, /법인/, /소송/, /로펌/,
  ];

  const preCleaned = [...top20Anchors, ...newEntries, ...poolFiltered]
    .filter(item => {
      const kw = item.keyword;
      if (LEGAL_NOISE.some(p => p.test(kw))) return false;
      if (kw.replace(/\s/g, '').length <= 1) return false;
      return true;
    })
    .slice(0, 100);

  // pool 전체 의미 중복 정리 (앵커 포함)
  const cleanMerged = await cleanPoolDuplicates(preCleaned);

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
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const results = [];

  // 40개 동시 호출 → rate limit 위험. 5개씩 순차 처리로 변경
  for (let i = 0; i < keywords.length; i += 5) {
    const chunk = keywords.slice(i, i + 5);
    const chunkResults = await Promise.all(chunk.map(async (kw) => {
      try {
        const res = await fetch(
          `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(kw)}&display=1&sort=sim`,
          {
            headers: {
              'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
              'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
            },
          }
        );
        const data = await res.json();
        // total이 0이거나 없으면 null
        const total = (data.total && data.total > 0) ? data.total : null;
        return { keyword: kw, total };
      } catch {
        return { keyword: kw, total: null };
      }
    }));
    results.push(...chunkResults);
    if (i + 5 < keywords.length) await sleep(120); // chunk 간 120ms 간격
  }
  return results;
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
async function generateComments(topKeywords, allTitles = []) {
  // fetchBlogContent(API 추가 호출) 대신 이미 수집한 allTitles에서 매칭
  // → API 호출 0회 추가, 20개 전부 커버 가능
  const normStr = s => s.replace(/\s+/g, '').toLowerCase();

  const kwWithContext = topKeywords.map((k, i) => {
    const kwWords = k.keyword.split(/\s+/).filter(w => w.length >= 2);
    const related = allTitles
      .filter(t => kwWords.some(w => normStr(t).includes(normStr(w))))
      .slice(0, 5);
    const context = related.length > 0
      ? '\n  참고:\n' + related.map(t => `  - ${t}`).join('\n')
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
    const today = getDateString(0);
    const netKeywords = await loadNetKeywords();
    const { titles: allTitles, risingWords } = await collectBlogTitles(netKeywords);
    if (!allTitles.length) throw new Error('블로그 제목 수집 실패');

    const refined = await extractTrendKeywords(allTitles, risingWords);
    if (!refined.length) throw new Error('키워드 추출 실패');

    const keywordPool = await updateKeywordPool(refined);
    if (!keywordPool.length) throw new Error('키워드 풀 없음');

    // pool 앞 40개 포스팅 수 조회 → 50만 초과 제거 → 중복 제거 → DataLab 조회
    const poolKeywords = keywordPool.slice(0, 40).map(item => item.keyword);
    const poolPostCounts = await getBlogPostCount(poolKeywords);
    const poolPostMap = Object.fromEntries(poolPostCounts.map(p => [p.keyword, p.total]));
    // DataLab이 키워드를 약간 변형해서 반환할 때를 위한 normKw 기반 fallback 맵
    const poolPostNormMap = Object.fromEntries(
      poolPostCounts.map(p => [normKw(p.keyword), p.total])
    );

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

    // 자가학습: DataLab 0.00 키워드 자동 블랙리스트 업데이트 (상위 랭킹 키워드는 제외)
    // ranked는 아직 미정의 → rawTrends weeklyRate 기준 상위 20개를 보호 대상으로 설정
    const top20Kws = [...rawTrends]
      .sort((a, b) => b.weeklyRate - a.weeklyRate)
      .slice(0, 20)
      .map(k => k.keyword);
    await updateAutoBlacklist(rawTrends, top20Kws);

    // null 제외하고 medianPost 계산
    const postValues = dedupedPool.map(kw => poolPostMap[kw]).filter(v => v != null);
    const medianPost = postValues.length ? median(postValues) : 0;
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

        // 오늘 기록 추가 — count(누적), daily(전날 대비 신규) 함께 저장
        const daily = yesterdayCount > 0 ? Math.max(0, postCount - yesterdayCount) : null;
        hist = hist.filter(h => h.date !== today);
        hist.push({ date: today, count: postCount, daily });
        await redis.set(histKey, JSON.stringify(hist));

        if (blogSurgeRate >= 20) {
          postHistoryMap[kw] = { blogSurgeRate: Math.round(blogSurgeRate), yesterdayCount };
          console.log(`[blogSurge] 급등 감지: ${kw} (+${Math.round(blogSurgeRate)}%, ${yesterdayCount}→${postCount})`);
        }
      } catch(e) {}
    }));

    console.log('[blogSurge] 급등 키워드:', Object.keys(postHistoryMap));

    const maxRising = Math.max(...rawTrends.map(r => r.risingRate), 1);

    const ranked = rawTrends.map(t => {
      const postCount = poolPostMap[t.keyword] ?? poolPostNormMap[normKw(t.keyword)] ?? null;
      const addedDate = addedAtMap[t.keyword] || today;
      const daysInPool = daysDiff(addedDate);
      const newBonus = daysInPool <= 3 ? 0.15 : 0;
      const surge = postHistoryMap[t.keyword];
      const blogSurgeRate = surge?.blogSurgeRate || 0;
      const blogSurgeBonus = blogSurgeRate >= 20 ? 0.15 : blogSurgeRate >= 10 ? 0.08 : 0;

      // 포스팅 수 규모 점수 — log 스케일 정규화 (상한 50만)
      // 100건 ≈ 0.37, 5만건 ≈ 0.86, 50만건 = 1.0
      const postVolumeScore = postCount && postCount > 0
        ? Math.min(Math.log10(postCount) / Math.log10(500000), 1)
        : 0;

      // BTR Score: 변화율 중심 유지 (73%) + 포스팅 규모 보조 (12%)
      const risingScore = t.risingRate > 0 ? (t.risingRate / maxRising) * 0.28 : 0;
      const score = (t.weeklyRate / maxRate) * 0.45
        + risingScore
        + postVolumeScore * 0.12
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

    // 최종 랭킹 — AI 기반 의미 중복 제거 후 상위 20개
    // ranked 상위 30개를 AI에게 넘겨서 같은 이슈 중복 제거 (높은 점수 키워드 우선 유지)
    const top30Keywords = ranked.slice(0, 30).map(k => k.keyword);
    const dedupedKeywords = await deduplicateByMeaning(top30Keywords, []);
    const dedupedSet = new Set(dedupedKeywords);
    const deduped = ranked.filter(k => dedupedSet.has(k.keyword)).slice(0, 20);
    // deduplicateByMeaning 실패 시 fallback
    const finalRanked = (deduped.length >= 5 ? deduped : ranked.slice(0, 20))
      .map((k, i) => ({ ...k, rank: i + 1 }));
    console.log('[finalRanked] 중복제거 후:', finalRanked.length, '개', finalRanked.slice(0,3).map(k=>k.keyword));

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

    // post_history 읽기 — polishKeywords 전에 원본 keyword로 조회 (중요: polish 후 keyword 불일치 방지)
    // daily(전날 대비 신규 포스팅 수) 기반으로 그래프 구성 — null이면 데이터 없는 날
    const originalKeywords = finalRanked.map(k => k.keyword);
    const postHistoryCache = {};
    await Promise.all(originalKeywords.map(async (origKw, i) => {
      try {
        const raw = await redis.get(`post_history:${origKw}`);
        if (raw) {
          const hist = typeof raw === 'string' ? JSON.parse(raw) : raw;
          hist.sort((a, b) => a.date.localeCompare(b.date));
          // daily 값 사용 (전날 대비 신규 포스팅 수), 없으면 null
          const dailyValues = hist.map(h => h.daily != null ? h.daily : null);
          // 모두 null이면 오늘 count 1개만 fallback
          const hasData = dailyValues.some(v => v != null);
          postHistoryCache[i] = hasData
            ? dailyValues
            : (hist.length > 0 ? [hist[hist.length - 1].count] : []);
        }
      } catch(e) {}
    }));

    // 키워드 정제 + 카테고리 분류
    const { names: polishedNames, categories } = await polishKeywords(finalRanked.map(k => k.keyword));
    console.log('[polishKeywords] 정제 결과:', polishedNames.slice(0, 5));
    finalRanked.forEach((k, i) => {
      k.keyword = polishedNames[i];
      k.category = categories[i] || '';
    });

    // 코멘트 생성 — allTitles 매칭 방식으로 API 추가 호출 없이 20개 전부 생성
    const commentsRaw = await generateComments(finalRanked.slice(0, 20), allTitles);
    const comments = finalRanked.map((_, i) => commentsRaw[String(i)] || '');

    // 이전 랭킹 읽어서 prevRank 계산
    let prevRankMap = {};
    try {
      const prevRaw = await redis.get('trend_data');
      if (prevRaw) {
        const prevData = typeof prevRaw === 'string' ? JSON.parse(prevRaw) : prevRaw;
        prevRankMap = Object.fromEntries((prevData.keywords || []).map(k => [k.keyword, k.rank]));
      }
    } catch(e) {}

    const result = {
      updatedAt: new Date().toISOString(),
      keywords: finalRanked.map((k, i) => ({
        rank: i + 1,
        prevRank: prevRankMap[k.keyword] || null,
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
        scoreValues: [Math.round(k.score * 100)], // score_history 누적 후 rank.js에서 확장
        postValues: postHistoryCache[i] || (k.postCount ? [k.postCount] : []),
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
    // 법적 민감 패턴만 유지 (나머지는 AI 게이팅 위임)
    const LEGAL_NOISE_TOP20 = [
      /성범죄/, /추행/, /그루밍/, /성폭/, /성추행/, /강간/, /음란/, /도촬/, /중절/,
      /변호사/, /법률/, /법인/, /소송/, /로펌/,
    ];
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
        if (LEGAL_NOISE_TOP20.some(p => p.test(kw))) return false;
        if (kw.replace(/\s/g, '').length <= 2) return false;
        return true;
      });
    // top20_pool 저장 전 AI 의미 중복 제거 (앵커끼리 BTS/방탄소년단 등 통합)
    const top20Deduped = await deduplicateByMeaning(top20Keywords, []);
    await redis.set('top20_pool', JSON.stringify(top20Deduped));
    console.log('[top20_pool] 저장:', top20Deduped.slice(0, 5), `(중복제거: ${top20Keywords.length}→${top20Deduped.length}개)`);

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
          score: Math.round(k.score * 100),
          rank: k.rank,
          blogSurge: k.blogSurge || false,
        })),
      });
      history = history.filter(h => h.date);
      history.sort((a, b) => b.date.localeCompare(a.date));
      history = history.slice(0, 30);
      await redis.set('trend_history', JSON.stringify(history));
      console.log('[trend_history] 저장:', dateStrKST, '/ 누적:', history.length + '일치');
    } catch(e) {
      console.log('[trend_history] 저장 실패:', e.message);
    }

    // BTR Score 히스토리 저장 - 키워드별 score 시계열
    try {
      await Promise.all(finalRanked.slice(0, 20).map(async k => {
        const scoreKey = `score_history:${k.keyword}`;
        let scoreHist = [];
        const stored = await redis.get(scoreKey);
        if (stored) scoreHist = typeof stored === 'string' ? JSON.parse(stored) : stored;
        scoreHist = scoreHist.filter(h => h.date !== dateStrKST);
        scoreHist.push({ date: dateStrKST, score: Math.round(k.score * 100) });
        scoreHist.sort((a, b) => a.date.localeCompare(b.date));
        scoreHist = scoreHist.slice(-30); // 최대 30일치
        await redis.set(scoreKey, JSON.stringify(scoreHist));
      }));
      console.log('[score_history] 저장 완료:', finalRanked.slice(0, 3).map(k => `${k.keyword}(${Math.round(k.score * 100)})`));
    } catch(e) {
      console.log('[score_history] 저장 실패:', e.message);
    }
    // 자가학습: 내일 그물 키워드 자동 갱신
    await updateNetKeywords(risingWords);

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
