import json
import datetime

employee = {
    "id": 456,
    "name": { "start": datetime.datetime.now(), "end": datetime.datetime.now() },
    "salary": 8000,
    "joindate": datetime.datetime.now()
}
print("JSON Data")
print(json.dumps(employee, default=str, indent=4))

with open('convert.json', 'w') as convert_file:
     convert_file.write(json.dumps(employee, default=str, indent=4))

with open('convert.json','r') as data:
      emp = json.load(data)
      print(emp)
