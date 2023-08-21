# Metadatum
Work in progress. Python implementation of the Self Generative Systems concept

### Installation


### Get started

```
export DYNACONF_DOT_META=/<abs path to project>/metadatum_sgs/.meta

docker run -p 6379:6379 --name redis-7.0 -it --rm redis/redis-stack:7.0.0-RC4

pip install -r requirements.txt
uvicorn server:app --reload
```