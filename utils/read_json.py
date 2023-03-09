
import json
class read_json_class:
    def read_json_function(self):
        json_file = open("configs\config.json")
        json_data = json.load(json_file)
        return json_data
        # print(json_data["DB"]["POSTGRES_PASSWORD"])