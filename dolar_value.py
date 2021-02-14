import requests
from lxml.html import fromstring
from lxml.cssselect import CSSSelector
import pandas as pd


def request_dolar_value():

    dolar_price_urls = [
        'https://www.dolarhoy.com/cotizaciondolarblue',
        'https://www.dolarhoy.com/cotizaciondolaroficial',
        'https://www.dolarhoy.com/cotizaciondolarbolsa',
        'https://www.dolarhoy.com/cotizaciondolarcontadoconliqui'
    ]

    titleSelector = CSSSelector("div[class='topic']")
    valueSelector = CSSSelector("div[class='value']")

    dolar_value_column = []  # precio del dolar
    dolar_type_column = []  # blue, oficial, bolsa o contado con liqui
    dolar_topic_column = []  # compra o venta

    for url in dolar_price_urls:
        try:
            r = requests.get(url)
            html_tree = fromstring(r.text)
            dolar_topic_column += [e.text for e in titleSelector(html_tree)] # compra o venta
            dolar_value_column += [e.text.replace("$","") for e in valueSelector(html_tree)]
            dolar_name = url.split("cotizaciondolar")[-1]  # blue, bolsa.. etc
            dolar_type_column += [dolar_name]*2
        except Exception as e:
            print(e)
            continue

    dolar_df = pd.DataFrame({
        'dolar_type': dolar_type_column,
        'topic': dolar_topic_column,
        'dolar_value': dolar_value_column
    })

    dolar_df['dolar_value'] = dolar_df['dolar_value'].astype(float)
    dolar_df['load_timestamp'] = pd.to_datetime('now')  # UTC

    return dolar_df
