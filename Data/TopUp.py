import json


class TopUp:
    """
    数据处理模型类
    """
    
    def __init__(self):
        """
        初始化方法
        """
        # This is our expected data structure and type
        self.expected_structure = {
                "database": (type(None), str),
                "table": (type(None), str),
                "type": (type(None), str),
                "ts": (type(None), int),
                "xid": (type(None), int),
                "commit": (type(None), bool),
                "data": {
                        "id": int,
                        "payid": (type(None), str),
                        "game": (type(None), str),
                        "zone": (type(None), str),
                        "svr": (type(None), str),
                        "tmReq": (type(None), str),
                        "tmRet": (type(None), str),
                        "buyerAcct": (type(None), str),
                        "buyerIP": (type(None), str),
                        "toAcct": (type(None), str),
                        "opid": (type(None), int),
                        "exInfo": (type(None), str),
                        "exCh": (type(None), str),
                        "exDid": (type(None), str),
                        "exSpid": (type(None), str),
                        "product": (type(None), str),
                        "reqTotalFee": (type(None), int),
                        "reqPtType": (type(None), int),
                        "reqPtNum": (type(None), int),
                        "paytype": (type(None), str),
                        "bankid": (type(None), str),
                        "gateid": (type(None), str),
                        "msg": (type(None), str),
                        "payTotalFee": (type(None), int),
                        "ptType": (type(None), int),
                        "ptNum": (type(None), int),
                        "state": (type(None), int),
                        "sign": (type(None), int)
                }
        }
    
    @staticmethod
    def parse_data(data):
        """
        解析 JSON 数据
        :param data: 需要解析的数据
        :return: 解析后的数据
        """
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                raise ValueError("Failed to decode JSON data")
        else:
            return data
    
    def validate_data(self, data, expected_structure=None):
        """
        验证数据的方法
        :param data: 需要校验的数据
        :param expected_structure: 预期的数据结构，默认为None，在递归调用时使用
        :return: 如果数据通过校验，返回解析后的数据，否则抛出异常
        """
        if expected_structure is None:
            expected_structure = self.expected_structure
        
        data = self.parse_data(data)
        
        for key, expected_type in expected_structure.items():
            if key not in data:
                raise KeyError(f"Missing key: {key}")
            if isinstance(expected_type, dict):
                # If the expected type is a dict, recursively validate the nested data
                self.validate_data(data[key], expected_type)
            elif not isinstance(data[key], expected_type):
                raise TypeError(f"Unexpected type for key {key}: expected {expected_type}, got {type(data[key])}")
        
        # If all keys' values are the expected type, then the data passes validation
        return data
