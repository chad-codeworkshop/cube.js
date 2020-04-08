const { Client } = require('@elastic/elasticsearch');
const SqlString = require('sqlstring');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');

const dialects = {
  awselasticsearch: 'awselasticsearch',
  odelasticsearch: 'odelasticsearch',
  elasticsearch: 'elasticsearch',
};

class ElasticSearchDriver extends BaseDriver {
  constructor(config) {
    super();

    console.log(config);

    const dialect = dialects[process.env.CUBEJS_DB_TYPE];

    // TODO: This config applies to AWS ES, Native ES and OpenDistro ES
    // All 3 have different dialects according to their respective documentation
    this.config = {
      url: process.env.CUBEJS_DB_URL,
      // openDistro:
      //   (process.env.CUBEJS_DB_ELASTIC_OPENDISTRO || 'false').toLowerCase() === 'true' ||
      //   process.env.CUBEJS_DB_TYPE === 'odelasticsearch',
      dialect,
      ...config
    };
    this.client = new Client({ node: this.config.url, cloud: this.config.cloud, auth: this.config.auth, ...config });
    this.sqlClient = this.config.openDistro ? new Client({ node: `${this.config.url}/_opendistro` }) : this.client;
  }

  async testConnection() {
    return this.client.cat.indices({
      format: 'json'
    });
  }

  async query(query, values) {
    try {
      const result = (await this.sqlClient.sql.query({ // TODO cursor
        body: {
          query: SqlString.format(query, values)
        }
      })).body;

      if (result.error) {
        throw Error(JSON.stringify(result.error, null, 2));
      }

      switch (this.config.dialect) {
        // Elastic Search - X PACK
        case dialects.elasticsearch:
          return this.traverseXPackResult(result);
        // AWS ElasticSearch
        // Open Distro
        case dialects.awselasticsearch:
        case dialects.odelasticsearch:
          return this.traverseODResult(result);
        default:
          throw Error("Not Implemented");
      }
    } catch (e) {
      if (e.body) {
        throw new Error(JSON.stringify(e.body, null, 2));
      }

      throw e;
    }
  }

  traverseXPackResult(result) {
    return result.rows.map(
      r => result.columns.reduce((prev, cur, idx) => ({ ...prev, [cur.name]: r[idx] }), {})
    );
  }

  traverseODResult(result) {
    const records = result.datarows.map(
      r => result.schema.reduce((prev, cur, idx) => ({ ...prev, [cur.alias]: r[idx] }), {})
    );

    return records;

    // return result && result.aggregations && this.traverseAggregations(result.aggregations);
  }

  traverseAggregations(aggregations) {
    const fields = Object.keys(aggregations).filter(k => k !== 'key' && k !== 'doc_count');
    if (fields.find(f => aggregations[f].hasOwnProperty('value'))) {
      return [fields.map(f => ({ [f]: aggregations[f].value })).reduce((a, b) => ({ ...a, ...b }))];
    }
    if (fields.length === 0) {
      return [{}];
    }
    if (fields.length !== 1) {
      throw new Error(`Unexpected multiple fields at ${fields.join(', ')}`);
    }
    const dimension = fields[0];
    if (!aggregations[dimension].buckets) {
      throw new Error(`Expecting buckets at dimension ${dimension}: ${aggregations[dimension]}`);
    }
    return aggregations[dimension].buckets.map(b => this.traverseAggregations(b).map(
      innerRow => ({ ...innerRow, [dimension]: b.key })
    )).reduce((a, b) => a.concat(b), []);
  }

  async tablesSchema() {
    const indices = await this.client.cat.indices({
      format: 'json'
    });

    const schema = (await Promise.all(indices.body.map(async i => {
      const props = (await this.client.indices.getMapping({ index: i.index })).body[i.index].mappings.properties || {};
      return {
        [i.index]: Object.keys(props).map(p => ({ name: p, type: props[p].type })).filter(c => !!c.type)
      };
    }))).reduce((a, b) => ({ ...a, ...b }));

    return {
      main: schema
    };
  }
}

module.exports = ElasticSearchDriver;
