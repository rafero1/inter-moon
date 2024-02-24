import os
import typing
import uuid
import random
import string
from base64 import b64encode, b64decode
from mimesis import Person, Datetime
from mimesis.locales import PT_BR
from helpers.strings import add_single_quotes as quote

# BigchainDB
# lab_results
# uid	            string
# patient_id        bigint
# content_base64    string
# date	            datetime
# lab_name	        string
# lab_site	        integer
# expired	        bool

class LabResult:
    def __init__(self, uid, patient_id, content_base64, datetime, lab_name, lab_site, expired) -> None:
        self.uid = uid
        self.patient_id = patient_id
        self.content_base64 = content_base64
        self.datetime = datetime
        self.lab_name = lab_name
        self.lab_site = lab_site
        self.expired = expired

    def __str__(self) -> str:
        return ", ".join(
            [
                quote(self.uid),
                str(self.patient_id),
                quote(self.content_base64),
                quote(self.datetime),
                quote(self.lab_name),
                str(self.lab_site),
                str(self.expired),
            ]
        )

class Query:
    def __init__(self, name: str, sql: str) -> None:
        self.name = name
        self.sql = sql

    def __str__(self) -> str:
        return self.name + ": " + self.sql

dir_path = os.path.dirname(os.path.realpath(__file__))
lab_results: typing.List[LabResult] = []

def generate_random_lab_result() -> LabResult:
    person = Person(PT_BR)
    date = Datetime()
    random_string = "".join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=24))
    content_base64 = b64encode(random_string.encode("utf-8"))
    return LabResult(
        uuid.uuid4(),
        random.randint(1, 100),
        str(content_base64.decode("utf-8")),
        date.datetime(2001).date(),
        random.choice(["Lab A", "Lab B", "Lab C"]),
        random.randint(1, 2),
        0,
    )

for n in range(100):
    lab_results.append(generate_random_lab_result())

i = 0


# Query Set 1 (moon v1 & v2)


# INSERT
with open(os.path.join(dir_path, "queries", f"{i}_lab_results_insert.sql"), "w+", encoding="utf-8") as file:
    for lab_result in lab_results:
        file.write(
            ("INSERT INTO lab_results (uid, patient_id, content_base64, datetime, lab_name, lab_site, expired) "
            f"VALUES ({lab_result});\n")
        )
i += 1

# SELECT
with open(os.path.join(dir_path, "queries", f"{i}_lab_results_select.sql"), "w+", encoding="utf-8") as file:
    for lab_result in lab_results:
        file.write("SELECT * FROM lab_results;\n")
i += 1

# JOIN
with open(os.path.join(dir_path, "queries", f"{i}_lab_results_join.sql"), "w+", encoding="utf-8") as file:
    for lab_result in lab_results:
        file.write("SELECT * FROM lab_results JOIN patients ON lab_results.patient_id = patients.id;\n")
i += 1

# UPDATE
with open(os.path.join(dir_path, "queries", f"{i}_lab_results_update.sql"), "w+", encoding="utf-8") as file:
    for lab_result in lab_results:
        file.write(f"UPDATE lab_results SET expired = 1 WHERE uid = '{lab_result.uid}';\n")
i += 1


# Query Set 2 (moon v2 only)


# AGGREGATION
with open(os.path.join(dir_path, "queries", f"{i}_v2_lab_results_groupby.sql"), "w+", encoding="utf-8") as file:
    for lab_result in lab_results:
        file.write("SELECT lab_name, count(lab_name) AS result_count FROM lab_results GROUP BY lab_name;\n")
i += 1

# ADDITIONAL DATA TYPES
with open(os.path.join(dir_path, "queries", f"{i}_v2_lab_results_date.sql"), "w+", encoding="utf-8") as file:
    for lab_result in lab_results:
        file.write("SELECT EXTRACT(YEAR FROM datetime) AS year, count(*) AS count "
                   "FROM lab_results GROUP BY year ORDER BY count DESC;\n")
i += 1

# DELETE
with open(os.path.join(dir_path, "queries", f"{i}_v2_lab_results_delete.sql"), "w+", encoding="utf-8") as file:
    for lab_result in lab_results:
        file.write(f"DELETE FROM lab_results WHERE uid = '{lab_result.uid}';\n")
i += 1


# Misc Queries


# INSERT 1
with open(os.path.join(dir_path, "queries", f"{i}_lab_results_insert_single.sql"), "w+", encoding="utf-8") as file:
    file.write(
        ("INSERT INTO lab_results (uid, patient_id, content_base64, datetime, lab_name, lab_site, expired) "
         f"VALUES ({lab_results[0]});")
        )
i += 1

# SELECT 1
with open(os.path.join(dir_path, "queries", f"{i}_lab_results_select_single.sql"), "w+", encoding="utf-8") as file:
    file.write("SELECT * FROM lab_results;")
i += 1
