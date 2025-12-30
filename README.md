# Oracle setup scripts

## setup_oracle.py

This script uses oracle-toolkit to set up Oracle 21c on GCP and configure it to use ASMFD.

**Notes**:

* I had to pin to a specific commit of oracle-toolkit. More recent versions refuse to install on this config.
* You need an existing GCP bucket with the swlib artifacts.

## setup_dataguard.py

This script sets up a data guard cluster.

## How to run this:

This is just a suggestion. You need to have uv installed, or execute it with your own python interpreter.

```
./setup_oracle.py --instance ora-1 </dev/null >ora-1.log 2>&1 &
./setup_oracle.py --instance ora-2 </dev/null >ora-2.log 2>&1 &
```

You may need to pass `--project`, `--zone` and `--swlib-bucket` for this to work for you.

Once the initial setup is done, run the data guard setup:

```
./setup_dataguard.py
```

Important to note that **this doesn't work**. While the script finishes successfully, the status of LOG_ARCHIVE_DEST_2
is ERROR after the setup is done.