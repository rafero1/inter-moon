import os
from mimesis import Person, Datetime
from mimesis.locales import PT_BR
from helpers.strings import add_single_quotes as quote

# SQL
# patients
# id	        bigint
# name	        string
# email	        string
# phone	        string
# birth_date	datetime


class Patient:
    def __init__(self, id, name, email, phone, birth_date) -> None:
        self.id = id
        self.name = name
        self.email = email
        self.phone = phone
        self.birth_date = birth_date

    def __str__(self) -> str:
        return ", ".join([str(self.id), quote(self.name), quote(self.email), quote(self.phone), quote(self.birth_date)])


patients = []

for n in range(100):
    person = Person(PT_BR)
    date = Datetime()
    patients.append(Patient(n+1, person.name(), person.email(),
                            person.telephone(mask="(##) 9####-####"), date.datetime(1950, 2000).date()))

dir_path = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(dir_path, "ddls", "patients.sql"), 'w+', encoding='utf-8') as file:
    for patient in patients:
        file.write(
            f"INSERT INTO patients (id, name, email, phone, birth_date) VALUES ({patient});\n")
