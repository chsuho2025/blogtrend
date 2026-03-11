const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

const CATEGORIES = [
  '여행', '음식', '패션', '뷰티', '육아',
  'IT', '건강', '경제', '문화'
];

// 1단계: 블로그 제목 수집
async function getBlogTitles() {
  const titles = [];
  for (const category of CATEGORIES) {
    try {
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
          title: item.title.replace(/<[^>]*>/g, '').replace(/&amp;|&lt;|&gt;|&quot;|&#\d+;/g, ' '),
          category,
        })));
      }
    } catch (e) {
      console.log('[getBlogTitles] error for', category, e.message);
    }
  }
  console.log('[getBlogTitles] total titles:', titles.length);
  return titles;
}

// 2단계: HyperCLOVA X로 명사 추출
async function extractNounsWithClova(titles) {
  const titleTexts = titles.map(t => t.title).join('\n');

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
              content: `아래 블로그 제목 목록에서 트렌드 분석에 유의미한 명사만 추출해.
규칙:
- 동사, 형용사, 조사, 어미 제외
- 2글자 이상 한국어 명사만
- 브랜드명, 인명, 지명 제외
- 너무 일반적인 단어 제외 (것, 수, 때, 일상, 생활 등)
- 2회 이상 등장한 단어만
- 빈도 높은 순으로 최대 40개
- 반드시 JSON 형식으로만 반환: {"keywords":["단어1","단어2",...]}
- 다른 설명 없이 JSON만.`,
            },
            { role: 'user', content: titleTexts },
          ],
          maxTokens: 600,
          temperature: 0.3,
          repetitionPenalty: 1.1,
        }),
      }
    );
    const data = await res.json();
    const text = data.result?.message?.content || '{}';
    const parsed = JSON.parse(text.replace(/```json|```/g, '').trim());
    const keywords = parsed.keywords || [];
    console.log('[extractNouns] clova extracted:', keywords.slice(0, 10));
    return keywords;
  } catch (e) {
    console.log('[extractNouns] clova failed, fallback to regex:', e.message);
    return extractNounsFallback(titles);
  }
}

// fallback: 정규식 기반 (CLOVA 실패 시)
const STOPWORDS = new Set([
  '추천', '후기', '리뷰', '정보', '방법', '이유', '종류', '소개', '정리',
  '하는법', '꿀팁', '총정리', '완벽', '진짜', '제대로', '먹는법', '사용법',
  '구매', '가격', '비교', '최고', '최신', '완전', '쉽게', '간단', '빠르게',
  '무료', '공짜', '할인', '이벤트', '특가', '세일', '베스트', '인기',
  '오늘', '요즘', '최근', '올해', '이번', '지금', '바로', '드디어',
  '하기', '하는', '되는', '있는', '없는', '위한', '대한', '관한',
  '이란', '란', '은', '는', '이', '가', '을', '를', '의', '에', '로',
  '부터', '까지', '에서', '으로', '이고', '이며', '이나', '이든',
  '것', '수', '때', '곳', '집', '날', '말', '글', '분', '편', '권',
  '개', '번', '번째', '가지', '종', '명', '원', '시간', '기간', '개월',
  '좋은', '나쁜', '큰', '작은', '많은', '적은', '높은', '낮은', '새로운',
  '일상', '생활', '사람', '우리', '모두', '전체', '기타',
  '시작', '완료', '성공', '실패', '준비', '계획', '목표', '결과', '효과',
  '원인', '문제', '해결', '변화', '차이', '장점', '단점',
  '필수', '중요', '기본', '전문', '공식', '무선', '유선',
  '광고', '협찬', '제공', '지원', '후원', '홍보', '마케팅', '브랜드',
  '사용처', '혜택', '적립', '포인트', '캐시백',
  '블로그', '포스팅', '게시글', '댓글', '공유', '저장', '클릭', '링크',
  '건강', '음식', '여행', '패션', '뷰티', '육아', '경제', '문화',
]);

const JOSA_ENDINGS = ['의', '을', '를', '이', '가', '은', '는', '로', '도', '만', '와', '과', '서', '에', '한', '된', '던'];

function extractNounsFallback(titles) {
  const allTitleTexts = titles.map(t => t.title);
  const totalCount = allTitleTexts.length;
  const freqMap = {};
  const docFreqMap = {};
  for (const title of allTitleTexts) {
    const words = title.match(/[가-힣]{2,8}/g) || [];
    const uniqueWords = [...new Set(words)];
    for (const word of words) freqMap[word] = (freqMap[word] || 0) + 1;
    for (const word of uniqueWords) docFreqMap[word] = (docFreqMap[word] || 0) + 1;
  }
  const threshold = totalCount * 0.5;
  return Object.entries(freqMap)
    .filter(([word, freq]) => {
      if (STOPWORDS.has(word)) return false;
      if (JOSA_ENDINGS.some(j => word.endsWith(j))) return false;
      if (docFreqMap[word] >= threshold) return false;
      if (freq < 3) return false;
      return true;
    })
    .sort((a, b) => b[1] - a[1])
    .slice(0, 40)
    .map(([word]) => word);
}

// 3단계: DataLab 검색량 트렌드 조회
async function getSearchTrends(keywords, mode = 'weekly') {
  // mode: 'weekly' = 7일vs7일, 'rising' = 3일vs3일
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
            // 3일 vs 3일
            const recent3 = values.slice(-3);
            const prev3 = values.slice(-6, -3);
            const recentAvg = avg(recent3);
            const prevAvg = avg(prev3);
            changeRate = prevAvg > 0 ? ((recentAvg - prevAvg) / prevAvg) * 100 : 0;
          } else {
            // 7일 vs 7일
            const recent7 = values.slice(-7);
            const prev7 = values.slice(-14, -7);
            const recentAvg = avg(recent7);
            const prevAvg = avg(prev7);
            changeRate = prevAvg > 0 ? ((recentAvg - prevAvg) / prevAvg) * 100 : 0;
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
    // 1. 블로그 제목 수집
    const titles = await getBlogTitles();
    if (!titles.length) throw new Error('블로그 제목 수집 실패');

    // 2. HyperCLOVA X로 명사 추출
    const keywords = await extractNounsWithClova(titles);
    if (!keywords.length) throw new Error('키워드 추출 실패');

    // 3. DataLab 검색량 조회 - 7일(메인) + 3일(급상승) 동시
    const [weeklyTrends, risingTrends] = await Promise.all([
      getSearchTrends(keywords, 'weekly'),
      getSearchTrends(keywords, 'rising'),
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

    // 7. 급상승 랭킹 (3일 기준, changeRate 순)
    const risingRateMap = Object.fromEntries(risingTrends.map(t => [t.keyword, t.changeRate]));
    const risingRanked = [...ranked]
      .map(k => ({ ...k, risingRate: risingRateMap[k.keyword] || 0 }))
      .filter(k => k.risingRate > 0)
      .sort((a, b) => b.risingRate - a.risingRate)
      .slice(0, 10);

    console.log('[ranked] trend distribution:', {
      유행예감: ranked.filter(k => k.trend === '유행예감').length,
      유행중: ranked.filter(k => k.trend === '유행중').length,
      유행지남: ranked.filter(k => k.trend === '유행지남').length,
    });
    console.log('[rising] top3:', risingRanked.slice(0, 3).map(k => `${k.keyword}(${Math.round(k.risingRate)}%)`));

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
    res.status(200).json({ success: true, updatedAt: result.updatedAt });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};
