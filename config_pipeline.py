#data.json snippet
# {
#   "employee":{
#     "table": "dbo.employee",
#     "type": "sql"
#   },
    # "adls":{
    #   "path": "<adls_path>",
    #   "type": "csv"
    # }
# }

#etl.json snippet
# {
#   "employee":{
      "input": 
#       {"employee": ["employee_id", "employee_name"],
        },
      "output": "adls"
#   }
# }}



def config_pipeline(etl_name: str) -> Tuple[Dict[str, Dict], Dict[str, str]]:
  with open("<data.json>", "r") as ff:
    conf_data = json.load(ff)

  with open("<etl.json>", "r") as ff:
    conf_etl = json.load(ff)

  conf_in = {kk:{**conf_data[kk], columns:vv} for kk, vv in conf_etl["input"].items()}
  conf_out = conf_data[conf_etl["output"]]

  return conf_in, conf_out

conf_in, conf_out = config_pipeline("employee")
