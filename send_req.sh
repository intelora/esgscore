curl --location --request POST 'https://adb-5289053630995145.5.azuredatabricks.net/api/2.1/jobs/run-now' --header 'Authorization: Bearer dapid840e1bca6b1fe773e191cafff242f9e' --header 'Content-Type: application/json' --data-raw '{ "job_id": 326741684155277, "notebook_params": {"myinput": "microsoft;google", "startdate": "2022-01-04", "enddate": "2022-01-07"}}'

curl --location --request GET 'https://adb-5289053630995145.5.azuredatabricks.net/api/2.1/jobs/runs/get?run_id=10846' \
--header 'Authorization: Bearer dapid840e1bca6b1fe773e191cafff242f9e'




curl --location --request GET 'https://adb-5289053630995145.5.azuredatabricks.net/api/2.1/jobs/runs/get-output?run_id=11140' \
--header 'Authorization: Bearer dapid840e1bca6b1fe773e191cafff242f9e'



