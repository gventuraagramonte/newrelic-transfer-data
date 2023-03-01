import requests
import json
from insertDB import *
import mysql.connector


def nerdgraph_nrql(key):
    # GraphQL query to NerdGraph
    query = """
  {
  actor {
    account(id: 2662887) {
      id
      nrql(query: "SELECT count(*) FROM Transaction WHERE appName='Plataforma Canales V3' FACET actionCode, actionDescription SINCE 1 DAY AGO") {
        results
      }
    }
  }
}"""

    # NerdGraph endpoint
    endpoint = "https://api.newrelic.com/graphql"
    headers = {'API-Key': f'{key}'}
    response = requests.post(endpoint, headers=headers, json={"query": query})

    if response.status_code == 200:
        # convert a JSON into an equivalent python dictionary
        dict_response = json.loads(response.content)
        prueba = dict_response['data']['actor']['account']['nrql']['results']
        print(dict_response['data']['actor']['account']['nrql']['results'])
        miConexion = mysql.connector.connect(
            host='localhost', user='root', passwd='G1V3NTUR4', db='test_db')
        cur = miConexion.cursor()
        sql = "insert into data(actionCode, NroTrxs, actionDescription) values(%s,%s,%s)"

        for clave in prueba:
            d1 = clave['facet'][0]
            d2 = clave['count']
            d3 = clave['facet'][1]
            val = (d1, d2, d3)
            cur.execute(sql, val)
            miConexion.commit()
            print("Successfully")
        # optional - serialize object as a JSON formatted stream
        # json_response = json.dumps(response.json(), indent=2)
        # print(json_response)

    else:
        # raise an exepction with a HTTP response code, if there is an error
        raise Exception(
            f'Nerdgraph query failed with a {response.status_code}.')


nerdgraph_nrql("NRAK-670269I0S393I8F1EHF2WPLNBMM")
