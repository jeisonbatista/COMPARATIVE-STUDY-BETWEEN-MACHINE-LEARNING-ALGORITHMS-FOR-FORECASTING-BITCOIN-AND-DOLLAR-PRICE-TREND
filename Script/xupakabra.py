from urllib.request import Request, urlopen
from datetime import datetime, timedelta

import pandas as pd


HDR = {
  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
  'Accept-Encoding': 'none',
  'Accept-Language': 'en-US,en;q=0.8',
  'Connection': 'keep-alive'
}


def get_data(ativos="BMF%5EDOLK19", datas=None, delta_days=60, resolution=1):

  date_pattern = '%Y-%m-%d %H:%M:%S'
  columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
  tmps = []
  date_now = "{:%Y-%m-%d}".format(datetime.now())
  if datas is None:
    # date_now = "{:%Y-%m-%d}".format(datetime.now())
    p_end = int(datetime.now().timestamp())
    p_start = int((datetime.now() - timedelta(days=delta_days)).timestamp())
    if type(ativos) == str:
      ativos = [ativos]
      datas = [(p_start, p_end)]
      search = zip(ativos, datas)
    else:
      assert len(ativos) == len(datas)
      search = zip(ativos, [(datetime.strptime(d[0], '%Y-%m-%d').timestamp(),
                             datetime.strptime(d[1], '%Y-%m-%d').timestamp())
                            for d in datas])
  else:
    assert len(ativos) == len(datas)
    search = zip(ativos, [(datetime.strptime(d[0], '%Y-%m-%d').timestamp(),
                           datetime.strptime(d[1], '%Y-%m-%d').timestamp())
                          for d in datas])

  for _, (ativo, (start, end)) in enumerate(search):
    url_string = 'https://br.advfn.com/common/javascript/tradingview/advfn/history?'
    url_string += 'symbol={0}&resolution={1}&from={2}&to={3}'
    url_string = url_string.format(ativo, resolution, int(start), int(end))
    print("\n\nObtendo dados de \n\n{0}".format(url_string))

    req = Request(url_string, headers=HDR)
    webpage = urlopen(req).read()
    df = pd.read_json(webpage)
    df = df[['t', 'o', 'h', 'l', 'c', 'v']]
    df.columns = columns

    df['date'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime(date_pattern))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df.to_csv('BMF-{0}.csv-{1}-{2}'.format(ativo.replace('^', ''), date_now, resolution))
    tmps.append(df)
  return df, tmps

df_, tmp = get_data(
  ativos=['BMF%5EDOLU19'], #BMF%5EDOLK19 COIN%5EBTCUSD BMF%5EDOLU19
  datas=[('2019-08-01', (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'))],
  resolution=1)
