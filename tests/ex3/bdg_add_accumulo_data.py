import json
import os
from pyaccumulo import Accumulo, Mutation, Range


file_dir = os.path.dirname(os.path.realpath(__file__))
sales_model = json.load(open(file_dir + "/catalog.json",
                        "r", encoding="utf-8"))['entities'][1]
sales = json.load(open(file_dir + "/sales_data.json", "r", encoding="utf-8"))

con = Accumulo(host="0.0.0.0", port=42424, user="bigdawg", password="bigdawg")

table = "sales"

if not con.table_exists(table):
    con.create_table(table)

wr = con.create_batch_writer(table)
for sale in sales:
    m = Mutation("%d" % sale["s_id"])
    m.put(cf="sale", cq="s_invoice_id", val=sale['s_invoice_id'])
    m.put(cf="sale", cq="s_branch", val=sale['s_branch'])
    m.put(cf="sale", cq="s_city", val=sale['s_city'])
    m.put(cf="sale", cq="s_product_line", val=sale['s_product_line'])
    m.put(cf="sale", cq="s_unit_price", val="%f" % sale['s_unit_price'])
    m.put(cf="sale", cq="s_quantity", val="%d" % sale['s_quantity'])
    m.put(cf="sale", cq="s_tax_5_pct", val="%f" % sale['s_tax_5_pct'])
    m.put(cf="sale", cq="s_total", val="%f" % sale['s_total'])
    m.put(cf="sale", cq="s_date", val=sale['s_date'])
    m.put(cf="sale", cq="s_time", val=sale['s_time'])
    m.put(cf="sale", cq="s_payment", val=sale['s_payment'])
    m.put(cf="sale", cq="s_cogs", val="%f" % sale['s_cogs'])
    m.put(cf="sale", cq="s_gross_margin_percentage", val="%f" %
          sale['s_gross_margin_percentage'])
    m.put(cf="sale", cq="s_gross_income", val="%f" % sale['s_gross_income'])
    m.put(cf="sale", cq="s_rating", val="%f" % sale['s_rating'])
    m.put(cf="sale", cq="c_id", val="%d" % sale['c_id'])
    wr.add_mutation(m)
wr.close()
