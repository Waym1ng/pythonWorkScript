'''
列表形式的数据转换成树结构(以parent_id的方式)，转换的结果，children字段嵌套的形式，适用于前端树形结构的渲染
'''
def list_to_tree(data):
    root = []
    node = []

    # 初始化数据，获取根节点和其他子节点list
    for d in data:
        d["choice"] = 0
        if d.get("parent_id") == 0:
            root.append(d)
        else:
            node.append(d)
    # print("root----",root)
    # print("node----",node)
    # 查找子节点
    for p in root:
        add_node(p, node)

    # 无子节点
    if len(root) == 0:
        return node

    return root


def add_node(p, node):
    # 子节点list
    p["children"] = []
    for n in node:
        if n.get("parent_id") == p.get("theme_id"):
            p["children"].append(n)

    # 递归子节点，查找子节点的节点
    for t in p["children"]:
        if not t.get("children"):
            t["children"] = []
        t["children"].append(add_node(t, node))

    # 退出递归的条件
    if len(p["children"]) == 0:
        p["choice"] = 1
        return
      
if __name__ == '__main__':
  data_list = [{'parent_id': 10023, 'theme_id': 10024, 'theme_name': '英语三级'},
               {'parent_id': 10022, 'theme_id': 10023, 'theme_name': '英语二级'},
               {'parent_id': 0, 'theme_id': 10025, 'theme_name': '语文一级'},
               {'parent_id': 10025, 'theme_id': 10026, 'theme_name': '语文二级'},
               {'parent_id': 0, 'theme_id': 10022, 'theme_name': '英语一级'}]
  data_tree = list_to_tree(data_list)
  print(data_tree)
  '''
  结果如下：
  [
  {
    "parent_id": 0,
    "theme_id": 10025,
    "theme_name": "语文一级",
    "choice": 0,
    "children": [
      {
        "parent_id": 10025,
        "theme_id": 10026,
        "theme_name": "语文二级",
        "choice": 1,
        "children": []
      }
    ]
  },
  {
    "parent_id": 0,
    "theme_id": 10022,
    "theme_name": "英语一级",
    "choice": 0,
    "children": [
      {
        "parent_id": 10022,
        "theme_id": 10023,
        "theme_name": "英语二级",
        "choice": 0,
        "children": [
          {
            "parent_id": 10023,
            "theme_id": 10024,
            "theme_name": "英语三级",
            "choice": 1,
            "children": []
          }
        ]
      }
    ]
  }
  ]
  '''
