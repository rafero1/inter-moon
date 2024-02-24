
eval "$(pyenv init -)"

pyenv shell moon-venv
python bdg_update_catalog.py
python bdg_add_sql_data.py
pyenv shell accumulo-test
python bdg_add_accumulo_data.py
