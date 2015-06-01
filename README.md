# GcsTarget

[Google Cloud Storage](https://cloud.google.com/storage/) target for [luigi](https://github.com/spotify/luigi).

Mostly copy and paste of the [S3Target](https://github.com/spotify/luigi/blob/master/luigi/s3.py) with AWS api calls 
replaced with GCS calls. It has only two major differences: 

* AFAIK we can't make resumable downloads in GCS
* for uploads file extention is required to make it possible to guess file mime type

# How to use

```python
target = GcsTarget(path)
``` 
will create target with GCS account details loaded from default config file (`/etc/gaw/gcs.yaml`)

```python
fs = GcsFileSystem(conf=dict_conf)
target = GcsTarget(path, gcs_filesystem=conf)
```
will create target with gcs conf provided by dictionary `dict_conf`

```python
fs = GcsFileSystem(secret_key=gcs_secret_key_file_path, email=gcs_email, private_key_password=optional_password)
target = GcsTarget(path, gcs_filesystem=conf)
```
will create target that uses GCS account `gcs_email` identified by secret key stored in a file located 
at `gcs_secret_key_file_path` with password `optional_password` (which is "notasecret" by default)

## Sample config

Here is sample YAML file with all required and optional configuration options

```yaml
auth:
  root:
    cert: './conf/privatekey.pem'
    password: 'notasecret'
    email: 'app-id@developer.gserviceaccount.com'
```
