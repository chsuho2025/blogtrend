module.exports = async (req, res) => {
  const { keyword } = req.query;
  if (!keyword) return res.status(400).json({ error: 'keyword 파라미터 필요' });

  try {
    // 최신순 20개 가져오기
    const response = await fetch(
      `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(keyword)}&display=20&sort=date`,
      {
        headers: {
          'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
          'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
        },
      }
    );
    const data = await response.json();
    if (!data.items) return res.status(200).json({ posts: [] });

    // 키워드 핵심 단어 추출 (공백 기준 분리, 2자 이상)
    const keywordTokens = keyword
      .split(/\s+/)
      .filter(t => t.length >= 2)
      .map(t => t.toLowerCase());

    const posts = data.items
      .map(item => ({
        title: item.title.replace(/<[^>]*>/g, ''),
        description: item.description.replace(/<[^>]*>/g, ''),
        link: item.link,
        bloggerName: item.bloggername,
        postdate: item.postdate,
      }))
      .filter(post => {
        // 제목 또는 설명에 키워드 토큰 중 하나라도 포함되면 통과
        const text = (post.title + ' ' + post.description).toLowerCase();
        return keywordTokens.some(token => text.includes(token));
      })
      .slice(0, 5); // 최대 5개

    res.status(200).json({ keyword, posts });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
