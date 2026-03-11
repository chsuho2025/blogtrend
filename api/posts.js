module.exports = async (req, res) => {
  const { keyword } = req.query;
  if (!keyword) return res.status(400).json({ error: 'keyword 파라미터 필요' });

  try {
    const response = await fetch(
      `https://openapi.naver.com/v1/search/blog?query=${encodeURIComponent(keyword)}&display=3&sort=sim`,
      {
        headers: {
          'X-Naver-Client-Id': process.env.NAVER_CLIENT_ID,
          'X-Naver-Client-Secret': process.env.NAVER_CLIENT_SECRET,
        },
      }
    );
    const data = await response.json();
    if (!data.items) return res.status(200).json({ posts: [] });

    const posts = data.items.map(item => ({
      title: item.title.replace(/<[^>]*>/g, ''),
      description: item.description.replace(/<[^>]*>/g, ''),
      link: item.link,
      bloggerName: item.bloggername,
      postdate: item.postdate,
    }));

    res.status(200).json({ keyword, posts });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
