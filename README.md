
# Mekari Test - Rachmad Rinaldie

This repository are being used to add all of my works related to tasks from Mekari.
And first download my credentials in json for access to bigquery https://drive.google.com/file/d/1tnMbt3niamKqQRevAHYBDLkfO_8bJjZP/view?usp=sharing and after download it, put it inside of the project folder.




## Tech Stack
I'm working from MacOs BigSur and here is what technology I was using :

**Local:** Python 3.8, BigQuery
## Prepare the virtual environment

```bash
cd ~
```

```bash
python3.8 -m pip install --user virtualenv
```

```bash
python3.8 -m venv venv-mekari
```


## Run Locally

Clone the project

```bash
  cd ~
```

```bash
  source ./venv-mekari/bin/activate
```

```bash
  git clone git@github.com:rachmad011/mekari_test_rachmad_rinaldie.git
```

Go to the project directory

```bash
  cd mekari_test_rachmad_rinaldie
```

Install requirements first

```bash
  pip install -r requirements.txt
```

Run the script manually

```bash
  python3.8 data_insert_daily.py
```


## BigQuery
- Go to this link console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sspheric-gearing-318714!2srachmadrinaldie_dataset

- Please request to rachmad011@gmail.com for accessing my dataset, so i can give you some.

