import json


def generate_model_class(json_data, class_name):
    data_obj = json.loads(json_data)
    
    def get_python_type(value):
        if isinstance(value, int):
            return "int"
        elif isinstance(value, str):
            return "(type(None), str)"
        elif isinstance(value, bool):
            return "bool"
        elif isinstance(value, dict):
            return generate_structure(value)
        elif value is None:
            return "(type(None), type(None))"
        else:
            return "(type(None), type(None))"
    
    def generate_structure(data):
        structure_items = []
        for key, value in data.items():
            structure_items.append(f'"{key}": {get_python_type(value)}')
        return "{" + ", \n        ".join(structure_items) + "}"
    
    expected_structure = generate_structure(data_obj)
    
    class_template = f"""import json
class {class_name}:
    def __init__(self):
        self.expected_structure = {expected_structure}

    @staticmethod
    def parse_data(data):
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                raise ValueError("Failed to decode JSON data")
        else:
            return data

    def validate_data(self, data, expected_structure=None):
        if expected_structure is None:
            expected_structure = self.expected_structure

        data = self.parse_data(data)

        for key, expected_type in expected_structure.items():
            if key not in data:
                raise KeyError(f"Missing key: {{key}}")
            if isinstance(expected_type, dict):
                self.validate_data(data[key], expected_type)
            elif isinstance(expected_type, tuple) and isinstance(expected_type[1], list):
                if not isinstance(data[key], list):
                    raise TypeError(f"Unexpected type for key {{key}}: expected list, got {{type(data[key])}}")
                for item in data[key]:
                    if not isinstance(item, expected_type[1][1]):
                        raise TypeError(f"Unexpected type for item in list {{key}}: expected {{expected_type[1][1]}}, got {{type(item)}}")
            elif not isinstance(data[key], expected_type):
                raise TypeError(f"Unexpected type for key {{key}}: expected {{expected_type}}, got {{type(data[key])}}")

        return data
    """
    
    return class_template


json_data = '''
{
"database": "points",
"table": "pss_sstx2248_payreq",
"type": "insert",
"ts": 1690261216,
"xid": 1562014434,
"commit": true,
"data": {
    "id": 8081,
    "payid": "pss_sstx2248_64BF56E00001",
    "game": "pss",
    "zone": "sstx2248",
    "svr": "sstx2248",
    "tmReq": "2023-07-25 05:00:16",
    "tmRet": "0000-00-00 00:00:00",
    "buyerAcct": "16073402440897429504",
    "buyerIP": "223.101.141.24",
    "toAcct": "16073402440897429504",
    "opid": 16073402440897429504,
    "exInfo": "icnUe97e91394f2941aa9a5539e58906896e",
    "exCh": "icn",
    "exDid": "R301bdf5a8db54aacb2f6162ea6ea27b4",
    "exSpid": "spid",
    "product": "pay1005",
    "reqTotalFee": 9800,
    "reqPtType": 0,
    "reqPtNum": 980,
    "paytype": null,
    "bankid": null,
    "gateid": null,
    "msg": null,
    "payTotalFee": null,
    "ptType": null,
    "ptNum": null,
    "state": 0,
    "sign": 0
    }
}
'''

generated = generate_model_class(json_data, class_name="New_Model")

with open('out.py', 'w', encoding='utf-8') as f:
    f.write(generated)

# 提取部分JSON的方法
# 找到 expected_structure 的开始位置:使用 index 方法查找字符串 "self.expected_structure = " 的位置。
# 为了获得实际的 expected_structure 部分，我们需要从该字符串之后开始，所以加上这个字符串的长度 len("self.expected_structure = ")。
# start_index = generate_model_class.index("self.expected_structure = ") + len("self.expected_structure = ")
# 找到 expected_structure 的结束位置:
# 使用 index 方法查找下一个大的空行和 "@staticmethod" 的位置。
# 生成代码中，expected_structure 之后紧接着就是一个空行和 "@staticmethod" 修饰符，所以这是提取结束位置的标识
# end_index = generate_model_class.index("\n\n    @staticmethod")
# 提取 expected_structure: 使用 Python 的字符串切片功能从 start_index 到 end_index 提取出 expected_structure。
# generate_model_class[start_index:end_index]
