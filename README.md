# Indexer
Coding video applications with Azure Video Indexer Python wrapper.
To authenticate with Azure please install Azure CLI and apply ```az login```.
To use live streamming from URL to MP4 files please install Streamlink and get its EXE location.
The project assumes the following JSON file with the following structure:

```
{
  "main": {
    "workingDir": "<WorkingDir>"
  },
  "vi":{
    "location": "<region>",
    "account_id": "<vi account id>",
    "subscription_id": "<azure subscription_id>",
    "api_version": "2022-07-20-preview",
    "account_name": "<vi account name>",
    "resource_group_name": "<azure vi resource_group_name>",
    "azure_tenant_id": "<azure_tenant_id>"
  }
}
```
