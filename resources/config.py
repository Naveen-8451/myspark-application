app_config = {
    "_comment": "This is for spark application",
    "local":{
        "source_file": {
            "incoming_file":"test_data"
        },
        "target": {
            "target_file":"test_data/master_911",
            "target_stg":"test_data/stg_911"
        },
        "db_type_url": {
            "mysql":"jdbc:mysql://localhost:3306/sample",
            "db2":"",
            "sql-server":"",
            "hive":""
        },
        "db_credential": {
            "user_name":"scott",
            "password":"tiger",

        },
        "db_table": {
            "table_names": [
                "genre_sample",
                "movie",
                "moviegenre",
                "movierating",
                "user",
                "occupation"
            ]
        }

    }
}