### Overview
Example pyiceberg project showcasing a potential bug with valid iceberg column 
name characters and throwing a `ArrowInvalid: No match for FieldRef.Name` when retrieving data via
`table.scan().to_arrow()` or `table.scan().to_pandas()`

### Project setup:
Create virtual env in project folder

`python3 -m virtualenv venv` or `virtualenv venv`

Activate virtualenv & install libraries
```
source venv/bin/activate
pip install -r requirements.txt
```

Run `python column_name_test.py` to see error
Uncomment sanitization block to & delete `warehouse` folder and re-run 
to see it working without issue when sanitizing the column names
