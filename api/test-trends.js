module.exports = async (req, res) => {
  try {
    const response = await fetch('https://trends.google.com/trending/rss?geo=KR', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)',
        'Accept': 'application/rss+xml, application/xml, text/xml',
      },
    });

    console.log('[test-trends] status:', response.status);
    console.log('[test-trends] content-type:', response.headers.get('content-type'));

    if (!response.ok) {
      return res.status(200).json({ success: false, status: response.status, error: 'HTTP 오류' });
    }

    const text = await response.text();
    console.log('[test-trends] response length:', text.length);
    console.log('[test-trends] first 500 chars:', text.slice(0, 500));

    // 키워드 추출
    const titles = [...text.matchAll(/<title><!\[CDATA\[([^\]]+)\]\]><\/title>/g)].map(m => m[1]);
    const keywords = titles.filter(t => !t.includes('Google')); // 첫 번째 피드 제목 제거

    res.status(200).json({ success: true, keywords, raw: text.slice(0, 1000) });
  } catch (err) {
    res.status(200).json({ success: false, error: err.message });
  }
};
