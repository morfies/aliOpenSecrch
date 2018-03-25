'use  strict';
const  assert  =  require('assert');
const  urllib  =  require('urllib');
const  utility  =  require('utility');
//  const  utf8  =  require('utf8');
const  rawUrlEncode  =  require('./rawurlencode');
const  co  =  require('co');
/**
  *  aliyun  opensearch  SDK
  *  @author  morfies
  */
//  fetch_fields  多个用;分隔
const  PATH  =  '/v3/openapi/apps/$app_name/search';
const  endpoint  =  '';
//  ?fetch_fields=$fetch_fields&query=config=format:fulljson&&query=default:'kj_buyer'&&sort=user_id

class  OpenSearch  {
    constructor(options)  {
        this.endpoint  =  options.endpoint  ||  endpoint;
        this.appName  =  options.appName;
        this.path  =  (options.path  ||  PATH).replace('$app_name',  this.appName);
        this.accessKeyId  =  options.accessKeyId;
        this.accessKeySecret  =  options.accessKeySecret;
        this.verb  =  'GET';
        this.contentType  =  'application/json';
        this.contentMd5  =  '';
        this.seperator  =  '\n';
        this.canonicalizedOpenSearchHeaders  =  '';
        this.canonicalizedResource  =  '';
        this.date  =  new  Date();
        this.isoDate  =  this.date.toISOString().replace(/.\d+Z/g,  'Z');
        this.nonce  =  Math.round(this.date.getTime())  +  ''  +  Math.round(Math.random()  *  100000);
        this.qt  =  {
            query:  {
                query:  '',
                config:  {
                    start:  0,  //  召回最多5k,  start  +  hit  >  5000  会报错
                    hit:  50,
                    format:  'json',
                    rerank_size:  200
                },
                filter:  {},
                sort:  {}
            },
            fetch_fields:  []
            //  scroll:  '1m',
            //  search_type:  'scan',
            //  scroll_id:  ''
        };


        assert(this.endpoint,  'endpoint  required');
        assert(this.appName,  'appName  required');
        assert(this.path,  'path  required');
        assert(this.accessKeyId,  'accessKeyId  required');
        assert(this.accessKeySecret,  'accessKeySecret  required');
    }

    setQuery(qs)  {
        assert(typeof  qs  ===  'string');
        this.qt.query.query  =  qs;
    }
    //  {start,  hit,  format,  rerank_size}
    setConfig(conf)  {
        if  (conf)  {
            this.qt.que ry.config = { ...this.qt.query.config, ...conf };
    }
  }
  setFilter(filter) {
    assert(filter);
    // todo
  }
  setSort(sort) {
    assert(sort);
    // todo
  }

  setFetchFields(fields) {
    assert(typeof fields === 'object');
    assert(fields.length);
    this.qt.fetch_fields = fields;
  }
  setScroll(scroll) {
    if (scroll) {
      this.qt.scroll = scroll;
    }
  }
  // 分页scroll取数
  setSearchTypeScan() {
    this.qt.search_type = 'scan';
  }
  setScrollId(id) {
    this.qt.scroll_id = id;
  }

  buildQuery() {
    const q = {
      query: `query=${this.qt.query.query}&&config=${JSON.stringify(this.qt.query.config).replace(/["\{\}]/g, '')}`,
      fetch_fields: this.qt.fetch_fields.join(';')
    };
    // console.log('--------q', q);
    if (this.qt.search_type) {
      q.search_type = this.qt.search_type;
    }
    if (this.qt.scroll) {
      q.scroll = this.qt.scroll;
    }
    if (this.qt.scroll_id) {
      q.scroll_id = this.qt.scroll_id;
    }
    // sort
    const r = [];
    const rr = [];
    for (const p in q) {
      r.push(p);
    }
    r.sort();
    for (const p of r) {
      rr.push(`${rawUrlEncode(p)}=${rawUrlEncode(q[p])}`);
    }
    return rr.join('&').replace(/\+/g, '%20');
  }

  /**
   * 生成Authorization请求头数据 "Authorization: OPENSEARCH " + AccessKeyId + ":" + Signature
   * @return {String}       [description]
   */
  _genAuthHeader() {
    this.canonicalizedOpenSearchHeaders = 'x-opensearch-nonce:' + this.nonce + this.seperator;
    this.canonicalizedResource = rawUrlEncode(this.path).replace(/%2F/g, '/') + '?' + this.buildQuery();
    const auth = 'OPENSEARCH ' + this.accessKeyId + ':' + this._signature();
    return auth;
  }

  _signature() {
    // console.log('headers::', this.canonicalizedOpenSearchHeaders);
    // console.log('resource::', this.canonicalizedResource);

    const str = this.verb + this.seperator
      + this.contentMd5 + this.seperator
      + this.contentType + this.seperator
      + this.isoDate + this.seperator
      + this.canonicalizedOpenSearchHeaders + this.canonicalizedResource;
    // console.log('-------------str\n', str);
    return utility.hmac('sha1', this.accessKeySecret, str);
  }

  // 普通的召回接口，返回最多5000条，原则上不当数据库使用，只召回最匹配的搜索结果
  * search() {
    const signature = this._genAuthHeader();
    const headers = {
      Authorization: signature,
      'Content-Md5': '',
      'Content-Type': this.contentType,
      Date: this.isoDate,
      'Accept-Language': 'zh-cn',
      'X-Opensearch-Nonce': this.nonce
    };
    const url = this.endpoint + this.canonicalizedResource;

    console.log('url::', url);
    return yield urllib.requestThunk(url, {
      method: 'GET',
      headers
    });
  }
  /**
   * 分批召回，可以实现类似数据库的分页取
   * 第一次请求只返回scroll_id, 第二次请求需要带上scroll_id和hit才能取到数据，
   * 然后接下来的请求都得带上前一次的scroll_id
   * config子句中 start 无效，通过hit值设置每次返回的结果数
   * @param {int} pageSize 返回多少数据
   * @param {int} scrollId 上一次请求返回的scroll_id，若无，则表明是第一次请求
   * @return {Object} result
   */
  * scroll(pageSize, scrollId) {
    this.setConfig({
      hit: +pageSize
    });
    this.setScroll('1m'); // scrollId expire time 1 minute
    if (!scrollId) { // first time
      this.setSearchTypeScan();
    } else {
      this.setScrollId(scrollId);
    }
    const result = yield this.search();

    if (result.status === 200) {
      const data = JSON.parse(result.data.toString('utf8'));
      // console.log('data...', data);
      if (data.status === 'OK') {
        const scrollId = data.result.scroll_id;
        const items = data.result.items;
        return {
          scrollId,
          items,
          total: data.result.total,
          num: data.result.num,
          viewtotal: data.result.viewtotal
        };
      }
    }
    return null;
  }
}

module.exports = OpenSearch;
