from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk

class ConnectToEs(object):

    def __init__(self, index_name):
        host = ES_HOST
        # 连接数据库
        self.es = Elasticsearch(host, timeout=180, max_retries=2, retry_on_timeout=True)
        self.__index_name = index_name
        self.__index_type = "default"

    def __del__(self):
        pass

    def create_index(self, mapping):
        """
        创建索引
        :param dict 对应索引的列信息:
        """
        _index_mappings = {
            "mappings": {
                self.__index_type: mapping
            }
        }
        if self.es.indices.exists(index=self.__index_name) is not True:
            res = self.es.indices.create(index=self.__index_name, body=_index_mappings)
            print(res)
        else:
            print(self.__index_name, 'aready exists!!!')

    def index_data(self, doc):
        """
        索引数据（相当于插入数据）
        :param dict 需要索引的数据:
        """
        res = self.es.index(index=self.__index_name, doc_type=self.__index_type, body=doc)
        if res['created'] is not True:
            print('index_data failed!!!')

    def multi_index_data(self, docs):
        """
        批量索引数据（相当于批量插入数据）
        :param list 需要索引的数据列表:
        """
        ACTIONS = []
        for line in docs:
            action = {
                "_index": self.__index_name,
                "_type": self.__index_type,
                "_source": line
            }
            ACTIONS.append(action)

        # 单次处理不能超过500条，不然容易插入失败
        need_do = True
        times = 0
        while need_do:
            do_actions = ACTIONS[times * 500: (times + 1) * 500]
            times += 1
            if len(do_actions) > 0:
                success, _ = bulk(self.es, do_actions, index=self.__index_name, raise_on_error=True)
                print('Performed %d actions' % success)
            if len(do_actions) < 500:
                need_do = False

    def search(self, doc={}):
        """查询"""
        result = self.es.search(index=self.__index_name, body=doc)
        return result

    # 第一次滚动查询用
    def search_scroll(self, doc={}):
        """查询"""
        result = self.es.search(index=self.__index_name, body=doc, scroll='1m')
        return result

    # 后续滚动查询用
    def scroll(self, sid):
        result = self.es.scroll(scroll_id=sid, scroll='1m')
        return result

    def search_source(self, doc={}):
        """
        查询结果的source列表，并且将数据行的id也放到结果中的_id里
        只支持普通查询，不支持聚合搜索
        """
        search = self.es.search(index=self.__index_name, body=doc)
        if 'hits' in search and 'hits' in search['hits']:
            search = search['hits']['hits']
        result = []
        for row in search:
            if '_source' in row and '_id' in row:
                r = row['_source'].copy()
                r['_id'] = row['_id']
                result.append(r)
        return result

    def search_by_id(self, id):
        """用id查询，注意这个id不是字段里面的id，而是自身生成的id"""
        try:
            res = self.es.get(index=self.__index_name, doc_type=self.__index_type, id=id)
        except Exception as err:
            raise Exception(err)

        return res

    def delete_by_id(self, id):
        """通过id删除，注意这个id不是字段里面的id，而是自身生成的id"""
        res = self.es.delete(index=self.__index_name, doc_type=self.__index_type, id=id)
        return res

    def update_by_id(self, id, doc):
        """
        更新id对应的数据行
        doc为需要更新的内容组成的字典，只需要存在更新内容的字段即可
        """
        try:
            res = self.es.update(index=self.__index_name, doc_type=self.__index_type, id=id, body={"doc": doc})
        except Exception as err:
            raise Exception(err)

        return res

    def get_mapping(self, i):
        """获取索引的字段信息"""
        return self.es.indices.get_mapping(i)

    def multi_update_data(self, docs):
        success = []
        errors = []
        for ok, item in parallel_bulk(self.es,
                                      actions=docs,
                                      refresh=True,
                                      raise_on_error=False,
                                      raise_on_exception=False):
            key = list(item.keys())[0]
            if not ok:
                res = {
                    "result": item[key]["error"]["reason"],
                    "_id": item[key]["_id"]
                }
                errors.append(res)
            else:
                res = {
                    "result": item[key]["result"],
                    "_id": item[key]["_id"]
                }
                success.append(res)

        data = [
            {"success": success},
            {"errors": errors}
        ]
        self.es.indices.flush(index=self.__index_name)
        return data

    def multi_update_by_id(self, id_list, doc):
        data = [
            {"success": []},
            {"errors": []}
        ]
        for _id in id_list:
            es_res = self.es.update(index=self.__index_name, doc_type=self.__index_type, id=_id, body={"doc": doc})
            res = {
                "_id": _id,
                "result": es_res["result"]
            }
            if es_res["_shards"]["failed"] == 0:
                data[0]["success"].append(res)
            else:
                data[1]["errors"].append(res)

        return data

    def multi_delete_by_query(self, doc):
        res = self.es.delete_by_query(index=self.__index_name, body=doc)

        return res
